// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use api_version::{ApiV2, KeyMode, KvFormat};
use collections::HashMap;
use engine_traits::KvEngine;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{CmdType, Request as RaftRequest},
};
use parking_lot::RwLock;
use raft::StateRole;
use raftstore::{
    coprocessor,
    coprocessor::{
        BoxCmdObserver, BoxQueryObserver, BoxRegionChangeObserver, BoxRoleObserver, CmdBatch,
        CmdObserver, Coprocessor, CoprocessorHost, ObserveLevel, ObserverContext, QueryObserver,
        RegionChangeEvent, RegionChangeObserver, RoleChange, RoleObserver,
    },
};

use crate::CausalTsProvider;

#[derive(Default, Debug)]
struct RegionInfo {
    pub ts: AtomicU64,
    pub is_leader: AtomicBool,
}

type RegionMap = HashMap<u64, RegionInfo>;

/// CausalObserver appends timestamp for RawKV V2 data,
/// and invoke causal_ts_provider.flush() on specified event, e.g. leader transfer, snapshot apply.
/// Should be used ONLY when API v2 is enabled.
pub struct CausalObserver<Ts: CausalTsProvider, E: KvEngine> {
    causal_ts_provider: Arc<Ts>,
    region_map: Arc<RwLock<RegionMap>>,
    _phantom: PhantomData<E>,
}

impl<Ts: CausalTsProvider, E: KvEngine> Clone for CausalObserver<Ts, E> {
    fn clone(&self) -> Self {
        Self {
            causal_ts_provider: self.causal_ts_provider.clone(),
            region_map: self.region_map.clone(),
            _phantom: PhantomData,
        }
    }
}

// Causal observer's priority should be higher than all other observers, to avoid being bypassed.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;

impl<Ts: CausalTsProvider + 'static, E: KvEngine> CausalObserver<Ts, E> {
    pub fn new(causal_ts_provider: Arc<Ts>) -> Self {
        Self {
            causal_ts_provider,
            region_map: Arc::new(RwLock::new(RegionMap::default())),
            _phantom: PhantomData,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host.registry.register_query_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host
            .registry
            .register_role_observer(CAUSAL_OBSERVER_PRIORITY, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_cmd_observer(CAUSAL_OBSERVER_PRIORITY, BoxCmdObserver::new(self.clone()));
        coprocessor_host.registry.register_region_change_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxRegionChangeObserver::new(self.clone()),
        );
    }
}

impl<Ts: CausalTsProvider, E: KvEngine> Coprocessor for CausalObserver<Ts, E> {}

impl<Ts: CausalTsProvider, E: KvEngine> QueryObserver for CausalObserver<Ts, E> {
    fn pre_propose_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        requests: &mut Vec<RaftRequest>,
    ) -> coprocessor::Result<()> {
        let mut ts = None;
        let region_id = ctx.region().get_id();

        for req in requests.iter_mut().filter(|r| {
            r.get_cmd_type() == CmdType::Put
                && ApiV2::parse_key_mode(r.get_put().get_key()) == KeyMode::Raw
        }) {
            if ts.is_none() {
                let is_leader = self
                    .region_map
                    .read()
                    .get(&region_id)
                    .unwrap()
                    .is_leader
                    .load(Ordering::Relaxed);
                if !is_leader {
                    if let Err(err) = self.causal_ts_provider.flush() {
                        warn!("CausalObserver::pre_propose_query, flush timestamp error"; "region" => region_id, "error" => ?err);
                        warn!("causal_ts tracing: flush timestamp in pre_propose error"; "region" => region_id, "error" => ?err);
                    } else {
                        info!("causal_ts tracing: flush timestamp in pre_propose"; "region" => region_id);
                    }
                }

                ts = Some(self.causal_ts_provider.get_ts().map_err(|err| {
                    coprocessor::Error::Other(box_err!("Get causal timestamp error: {:?}", err))
                })?);
                info!("causal_ts tracing: pre_propose"; "region" => region_id, "ts" => ts.unwrap(), "is_leader" => is_leader);
            }

            ApiV2::append_ts_on_encoded_bytes(req.mut_put().mut_key(), ts.unwrap());
        }
        Ok(())
    }
}

impl<Ts: CausalTsProvider, E: KvEngine> RoleObserver for CausalObserver<Ts, E> {
    /// Observe becoming leader, to flush CausalTsProvider.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        let region_id = ctx.region().get_id();
        if role_change.state == StateRole::Leader {
            if let Err(err) = self.causal_ts_provider.flush() {
                warn!("CausalObserver::on_role_change, flush timestamp error"; "region" => region_id, "error" => ?err);
                warn!("causal_ts tracing: flush timestamp error"; "region" => region_id, "error" => ?err);
            } else {
                debug!("CausalObserver::on_role_change, flush timestamp succeed"; "region" => region_id);
                let _ = self
                    .causal_ts_provider
                    .get_ts()
                    .map(|ts| {
                        info!("causal_ts tracing: flush"; "region" => region_id, "ts" => ts);
                        ts
                    })
                    .map_err(|err| {
                        error!("causal_ts tracing: flush, get_ts fail"; "region" => region_id);
                        err
                    });
            }
        }

        let is_leader = role_change.state == StateRole::Leader;
        let region_map = self.region_map.read();
        if let Some(info) = region_map.get(&region_id) {
            info.is_leader.store(is_leader, Ordering::Relaxed);
        }
    }
}

impl<Ts: CausalTsProvider, E: KvEngine> CmdObserver<E> for CausalObserver<Ts, E> {
    fn on_flush_applied_cmd_batch(
        &self,
        _max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        _: &E,
    ) {
        for batch in cmd_batches {
            let region_id = batch.region_id;

            // get first
            for cmd in &batch.cmds {
                for req in cmd.request.get_requests() {
                    if req.get_cmd_type() == CmdType::Put
                        && ApiV2::parse_key_mode(req.get_put().get_key()) == KeyMode::Raw
                    {
                        let (_, observed_ts) = ApiV2::split_ts(req.get_put().get_key()).unwrap();
                        let region_ts = self
                            .region_map
                            .read()
                            .get(&region_id)
                            .unwrap()
                            .ts
                            .load(Ordering::Relaxed);
                        if observed_ts.into_inner() < region_ts {
                            error!("causal_ts tracing: fall back"; "region" => region_id, "region_ts" => region_ts, "observed_ts" => observed_ts);
                        }
                        break;
                    }
                }
            }

            // get last
            for cmd in batch.cmds.iter().rev() {
                for req in cmd.request.get_requests().iter().rev() {
                    if req.get_cmd_type() == CmdType::Put
                        && ApiV2::parse_key_mode(req.get_put().get_key()) == KeyMode::Raw
                    {
                        let (_, observed_ts) = ApiV2::split_ts(req.get_put().get_key()).unwrap();
                        let region_map = self.region_map.read();
                        let region_info = region_map.get(&region_id).unwrap();
                        region_info
                            .ts
                            .store(observed_ts.into_inner(), Ordering::Relaxed);

                        break;
                    }
                }
            }
        }
    }

    fn on_applied_current_term(&self, role: StateRole, region: &Region) {
        let region_id = region.get_id();
        if role == StateRole::Leader {
            info!("causal_ts tracing: on_applied_current_term"; "region" => region_id);
        }
    }
}

impl<Ts: CausalTsProvider, E: KvEngine> RegionChangeObserver for CausalObserver<Ts, E> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        let region_id = ctx.region().get_id();
        let is_leader = role == StateRole::Leader;
        match event {
            RegionChangeEvent::Create => {
                self.region_map.write().insert(
                    region_id,
                    RegionInfo {
                        ts: AtomicU64::new(0),
                        is_leader: AtomicBool::new(is_leader),
                    },
                );
            }
            RegionChangeEvent::Destroy => {
                self.region_map.write().remove(&region_id);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{mem, sync::Arc, time::Duration};

    use api_version::{ApiV2, KvFormat};
    use engine_rocks::RocksEngine;
    use futures::executor::block_on;
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{RaftCmdRequest, Request as RaftRequest},
    };
    use test_raftstore::TestPdClient;
    use txn_types::{Key, TimeStamp};

    use super::*;
    use crate::BatchTsoProvider;

    fn init() -> CausalObserver<BatchTsoProvider<TestPdClient>, RocksEngine> {
        let pd_cli = Arc::new(TestPdClient::new(0, true));
        pd_cli.set_tso(100.into());
        let causal_ts_provider =
            Arc::new(block_on(BatchTsoProvider::new_opt(pd_cli, Duration::ZERO, 100)).unwrap());
        CausalObserver::new(causal_ts_provider)
    }

    #[test]
    fn test_causal_observer() {
        let testcases: Vec<&[&[u8]]> = vec![
            &[b"r\0a", b"r\0b"],
            &[b"r\0c"],
            &[b"r\0d", b"r\0e", b"r\0f"],
        ];

        let ob = init();
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);

        for (i, keys) in testcases.into_iter().enumerate() {
            let mut cmd_req = RaftCmdRequest::default();

            for key in keys {
                let key = ApiV2::encode_raw_key(key, None);
                let value = b"value".to_vec();
                let mut req = RaftRequest::default();
                req.set_cmd_type(CmdType::Put);
                req.mut_put().set_key(key.into_encoded());
                req.mut_put().set_value(value);

                cmd_req.mut_requests().push(req);
            }

            let query = cmd_req.mut_requests();
            let mut vec_query: Vec<RaftRequest> = mem::take(query).into();
            ob.pre_propose_query(&mut ctx, &mut vec_query).unwrap();
            *query = vec_query.into();

            for req in cmd_req.get_requests() {
                let key = Key::from_encoded_slice(req.get_put().get_key());
                let (_, ts) = ApiV2::decode_raw_key_owned(key, true).unwrap();
                assert_eq!(ts, Some(TimeStamp::from(i as u64 + 101)));
            }
        }
    }
}
