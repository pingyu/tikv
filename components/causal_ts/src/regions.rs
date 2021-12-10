// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::CfName;
use engine_traits::{util::get_causal_ts, KvEngine};
use raft::StateRole;
use raftstore::coprocessor::{
    ApplySnapshotObserver, BoxApplySnapshotObserver, BoxCmdObserver, BoxRoleObserver, Cmd,
    CmdObserver, Coprocessor, CoprocessorHost, ObserverContext, RoleObserver,
};
use raftstore::store::fsm::ObserveID;
use txn_types::TimeStamp;

use std::cell::RefCell;
use std::sync::{Arc, RwLock};

use crate::CausalTsProvider;

#[derive(Debug, Clone)]
pub(crate) struct RegionCausalInfo {
    pub(crate) max_ts: TimeStamp,
}

impl RegionCausalInfo {
    pub(crate) fn new(latest_ts: TimeStamp) -> RegionCausalInfo {
        RegionCausalInfo { max_ts: latest_ts }
    }
}

type RegionsMap = HashMap<u64, RegionCausalInfo>;

#[derive(Debug, Default)]
pub struct RegionsCausalManager {
    regions_map: Arc<RwLock<RegionsMap>>,
}

impl RegionsCausalManager {
    pub fn new() -> RegionsCausalManager {
        RegionsCausalManager {
            regions_map: Arc::new(RwLock::new(RegionsMap::default())),
        }
    }

    pub fn update_max_ts(&self, region_id: u64, new_ts: TimeStamp) {
        let mut m = self.regions_map.write().unwrap();
        m.entry(region_id)
            .and_modify(|r| {
                if new_ts > r.max_ts {
                    r.max_ts = new_ts;
                }
            })
            .or_insert_with(|| RegionCausalInfo::new(new_ts));
    }

    pub fn max_ts(&self, region_id: u64) -> TimeStamp {
        let m = self.regions_map.read().unwrap();
        m.get(&region_id).map_or_else(TimeStamp::zero, |r| r.max_ts)
    }
}

#[derive(Clone)]
pub struct CausalObserver {
    causal_manager: Arc<RegionsCausalManager>,
    regions_map: RefCell<RegionsMap>,
    causal_ts: Arc<dyn CausalTsProvider>,
}

const CAUSAL_OBSERVER_PRIORITY: u32 = 1000;

impl CausalObserver {
    pub fn new(
        causal_manager: Arc<RegionsCausalManager>,
        causal_ts: Arc<dyn CausalTsProvider>,
    ) -> CausalObserver {
        CausalObserver {
            causal_manager,
            regions_map: RefCell::new(RegionsMap::default()),
            causal_ts,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<RocksEngine>) {
        coprocessor_host.registry.register_global_cmd_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxCmdObserver::new(self.clone()),
        );
        coprocessor_host
            .registry
            .register_role_observer(CAUSAL_OBSERVER_PRIORITY, BoxRoleObserver::new(self.clone()));
        coprocessor_host.registry.register_apply_snapshot_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxApplySnapshotObserver::new(self.clone()),
        );
    }
}

impl Coprocessor for CausalObserver {}

impl<E: KvEngine> CmdObserver<E> for CausalObserver {
    fn on_prepare_for_apply(&self, _observe_id: ObserveID, _region_id: u64) {}

    fn on_apply_cmd(&self, _observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        warn!("CausalObserver on_apply_cmd"; "region" => region_id, "cmd" => ?cmd);
        for req in cmd.request.get_requests() {
            if req.has_put() {
                if let Some(ts) = get_causal_ts(req.get_put().get_value()) {
                    warn!("CausalObserver on_apply_cmd"; "region" => region_id, "ts" => ts);
                    self.regions_map
                        .borrow_mut()
                        .entry(region_id)
                        .and_modify(|r| r.max_ts = ts)
                        .or_insert_with(|| RegionCausalInfo::new(ts));
                }
            }
        }
    }

    fn on_flush_apply(&self, _engine: E) {
        if !self.regions_map.borrow().is_empty() {
            let m = self.regions_map.replace(RegionsMap::default());
            for (region_id, info) in m {
                self.causal_manager.update_max_ts(region_id, info.max_ts);
            }
        }
        warn!("CausalObserver on_flush_apply"; "causal_manager" => ?self.causal_manager);
    }
}

impl RoleObserver for CausalObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role == StateRole::Leader {
            let region_id = ctx.region().get_id();
            let max_ts = self.causal_manager.max_ts(region_id);
            self.causal_ts.advance(max_ts).unwrap();
            warn!("CausalObserver on_role_change to leader"; "region" => region_id, "max_ts" => max_ts);
        }
    }
}

impl ApplySnapshotObserver for CausalObserver {
    fn apply_plain_kvs(
        &self,
        ctx: &mut ObserverContext<'_>,
        _cf: CfName,
        _kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        self.handle_snapshot(ctx.region().get_id()).unwrap(); // TODO: error handle
    }

    fn apply_sst(&self, ctx: &mut ObserverContext<'_>, _cf: CfName, _path: &str) {
        self.handle_snapshot(ctx.region().get_id()).unwrap(); // TODO: error handle
    }
}

impl CausalObserver {
    fn handle_snapshot(&self, region_id: u64) -> Result<()> {
        // update to latest ts
        let ts = self.causal_ts.get_ts()?;
        self.causal_manager.update_max_ts(region_id, ts);
        warn!("CausalObserver::handle_snapshot"; "region" => region_id, "latest-ts" => ts, "causal_manager" => ?self.causal_manager);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TsoSimpleProvider;
    use engine_rocks::RocksEngine;
    use engine_traits::util::append_extended_fields;
    use kvproto::raft_cmdpb::*;
    use std::convert::TryInto;
    use test_pd::TestPdClient;
    use tikv::storage::kv::TestEngineBuilder;

    #[test]
    fn test_causal_observer() {
        let pd_client = TestPdClient::new(0, true);
        let causal_ts = Arc::new(TsoSimpleProvider::new(Arc::new(pd_client)));
        let manager = Arc::new(RegionsCausalManager::default());

        let ob = CausalObserver::new(manager.clone(), causal_ts);
        let ob_id = ObserveID::new();

        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        let testcases: Vec<(u64, &[u64])> = vec![(10, &[100, 200]), (20, &[101]), (20, &[102])];
        let mut m = RegionsMap::default();

        for (i, (region_id, ts_list)) in testcases.into_iter().enumerate() {
            let mut cmd_req = RaftCmdRequest::default();

            for ts in ts_list {
                m.entry(region_id)
                    .and_modify(|v| v.max_ts = ts.into())
                    .or_insert_with(|| RegionCausalInfo::new(ts.into()));

                let mut value = b"value".to_vec();
                append_extended_fields(&mut value, 0, Some(ts.into()));
                let mut req = Request::default();
                req.mut_put().set_value(value);

                cmd_req.mut_requests().push(req);
            }

            <CausalObserver as CmdObserver<RocksEngine>>::on_prepare_for_apply(
                &ob, ob_id, region_id,
            );
            <CausalObserver as CmdObserver<RocksEngine>>::on_apply_cmd(
                &ob,
                ob_id,
                region_id,
                Cmd::new(i.try_into().unwrap(), cmd_req, RaftCmdResponse::default()),
            );
            ob.on_flush_apply(engine.clone());
        }

        for (k, v) in m {
            assert_eq!(v.max_ts, manager.max_ts(k));
        }
    }
}
