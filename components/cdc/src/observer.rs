// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::{Arc, RwLock};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::util::get_causal_ts;
use engine_traits::KvEngine;
use fail::fail_point;
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::Request;
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use tikv_util::worker::Scheduler;
use tikv_util::{error, warn};
use txn_types::TimeStamp;

use crate::endpoint::{Deregister, Task};
use crate::old_value::{self, OldValueCache, OldValueReader};
use crate::Error as CdcError;

/// An Observer for CDC.
///
/// It observes raftstore internal events, such as:
///   1. Raft role change events,
///   2. Apply command events.
#[derive(Clone)]
pub struct CdcObserver {
    sched: Scheduler<Task>,
    // A shared registry for managing observed regions.
    // TODO: it may become a bottleneck, find a better way to manage the registry.
    observe_regions: Arc<RwLock<HashMap<u64, ObserveID>>>,
    cmd_batches: RefCell<Vec<CmdBatch>>,
    causal_ts: Option<Arc<dyn causal_ts::CausalTsProvider>>,
}

impl CdcObserver {
    /// Create a new `CdcObserver`.
    ///
    /// Events are strong ordered, so `sched` must be implemented as
    /// a FIFO queue.
    pub fn new(sched: Scheduler<Task>) -> CdcObserver {
        Self::new_opt(sched, None)
    }

    pub fn new_opt(
        sched: Scheduler<Task>,
        causal_ts: Option<Arc<dyn causal_ts::CausalTsProvider>>,
    ) -> CdcObserver {
        CdcObserver {
            sched,
            observe_regions: Arc::default(),
            cmd_batches: RefCell::default(),
            causal_ts,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<RocksEngine>) {
        // 100 is the priority of the observer. CDC should have a high priority.
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_query_observer(100, BoxQueryObserver::new(self.clone()));
    }

    /// Subscribe an region, the observer will sink events of the region into
    /// its scheduler.
    ///
    /// Return pervious ObserveID if there is one.
    pub fn subscribe_region(&self, region_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        self.observe_regions
            .write()
            .unwrap()
            .insert(region_id, observe_id)
    }

    /// Stops observe the region.
    ///
    /// Return ObserverID if unsubscribe successfully.
    pub fn unsubscribe_region(&self, region_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        let mut regions = self.observe_regions.write().unwrap();
        // To avoid ABA problem, we must check the unique ObserveID.
        if let Some(oid) = regions.get(&region_id) {
            if *oid == observe_id {
                return regions.remove(&region_id);
            }
        }
        None
    }

    /// Check whether the region is subscribed or not.
    pub fn is_subscribed(&self, region_id: u64) -> Option<ObserveID> {
        self.observe_regions
            .read()
            .unwrap()
            .get(&region_id)
            .cloned()
    }

    fn send_track_ts(&self, region_id: u64, key: Vec<u8>, track_ts: TimeStamp) -> Result<()> {
        debug!("(rawkv)cdc::CdcObserver::send_track_ts schedule Task::TrackTS"; "region_id" => region_id, "key" => &log_wrappers::Value::key(&key), "track_ts" => track_ts);
        if let Err(e) = self.sched.schedule(Task::TrackTS {
            region_id,
            key,
            ts: track_ts,
        }) {
            error!("(rawkv)cdc schedule cdc task failed"; "error" => ?e);
            Err(Error::Other(box_err!("Schedule error: {:?}", e)))
        } else {
            Ok(())
        }
    }
}

impl Coprocessor for CdcObserver {}

impl<E: KvEngine> CmdObserver<E> for CdcObserver {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, region_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        debug!("(rawkv)CdcObserver::on_apply_cmd"; "region_id" => region_id, "cmd" => ?cmd);
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        debug!("(rawkv)CdcObserver::on_flush_apply"; "cmd_batches" => ?self.cmd_batches.borrow());
        fail_point!("before_cdc_flush_apply");
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
            let reader = OldValueReader::new(snapshot);
            let get_old_value = move |key, query_ts, old_value_cache: &mut OldValueCache| {
                old_value::get_old_value(&reader, key, query_ts, old_value_cache)
            };
            if let Err(e) = self.sched.schedule(Task::MultiBatch {
                multi: batches,
                old_value_cb: Box::new(get_old_value),
            }) {
                warn!("cdc schedule task failed"; "error" => ?e);
            }
        }
    }
}

impl RoleObserver for CdcObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role != StateRole::Leader {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::NotLeader(region_id, None);
                let deregister = Deregister::Delegate {
                    region_id,
                    observe_id,
                    err: CdcError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("cdc schedule cdc task failed"; "error" => ?e);
                }
            }
        }
    }
}

impl RegionChangeObserver for CdcObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        if let RegionChangeEvent::Destroy = event {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::RegionNotFound(region_id);
                let deregister = Deregister::Delegate {
                    region_id,
                    observe_id,
                    err: CdcError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("cdc schedule cdc task failed"; "error" => ?e);
                }
            }
        }
    }
}

impl QueryObserver for CdcObserver {
    fn pre_propose_barrier(&self, ctx: &mut ObserverContext<'_>) -> Result<()> {
        let region_id = ctx.region().get_id();
        if let (Some(_), Some(causal_ts)) = (self.is_subscribed(region_id), self.causal_ts.as_ref())
        {
            let key = region_id.to_be_bytes();
            let lock_ts = causal_ts.get_ts().unwrap(); // error handle
            debug!("(rawkv)cdc::CdcObserver::pre_propose_barrier"; "region_id" => region_id, "lock_ts" => ?lock_ts, "key" => &log_wrappers::Value::key(&key));
            self.send_track_ts(region_id, key.to_vec(), lock_ts)
        } else {
            Ok(())
        }
    }

    fn pre_propose_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        requests: &mut Vec<Request>,
    ) -> Result<()> {
        // TODO(rawkv): check raw cdc
        // TODO(rawkv): timestamp in the region is increasing in a region.
        //   So we can use a more simple structure in ts resolver (now is B-tree).
        let region_id = ctx.region().get_id();
        if self.is_subscribed(region_id).is_some() {
            debug!("(rawkv)cdc::CdcObserver::pre_propose_query"; "region_id" => region_id, "req" => ?requests);
            for req in requests {
                if req.has_put() {
                    if let Some(ts) = get_causal_ts(req.get_put().get_value()) {
                        // resolved_ts should be smaller than commit_ts, so use ts.prev() here.
                        let track_ts = ts.prev();
                        // track the first request ONLY. TS in batch is increasing.
                        return self.send_track_ts(
                            region_id,
                            req.get_put().get_key().to_vec(),
                            track_ts,
                        );
                    }
                }
                if req.has_delete() {
                    // TODO(rawkv): delete request no value field
                    // if let Some(ts) = get_causal_ts(req.get_delete().get_value()) {
                    //     return send_track_ts(req.get_put().get_key().clone(), ts);
                    // }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::RocksEngine;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::*;
    use std::time::Duration;
    use tikv::storage::kv::TestEngineBuilder;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);
        let observe_id = ObserveID::new();
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        <CdcObserver as CmdObserver<RocksEngine>>::on_prepare_for_apply(&observer, observe_id, 0);
        <CdcObserver as CmdObserver<RocksEngine>>::on_apply_cmd(
            &observer,
            observe_id,
            0,
            Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default()),
        );
        observer.on_flush_apply(engine);

        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { multi, .. } => {
                assert_eq!(multi.len(), 1);
                assert_eq!(multi[0].len(), 1);
            }
            _ => panic!("unexpected task"),
        };

        // Does not send unsubscribed region events.
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        let oid = ObserveID::new();
        observer.subscribe_region(1, oid);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister(Deregister::Delegate {
                region_id,
                observe_id,
                ..
            }) => {
                assert_eq!(region_id, 1);
                assert_eq!(observe_id, oid);
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        observer.on_role_change(&mut ctx, StateRole::Leader);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // unsubscribed fail if observer id is different.
        assert_eq!(observer.unsubscribe_region(1, ObserveID::new()), None);

        // No event if it is unsubscribed.
        let oid_ = observer.unsubscribe_region(1, oid).unwrap();
        assert_eq!(oid_, oid);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        region.set_id(999);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
