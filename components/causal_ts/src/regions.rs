// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::{util::get_causal_ts, KvEngine};
use raftstore::coprocessor::{BoxCmdObserver, Cmd, CmdObserver, Coprocessor, CoprocessorHost};
use raftstore::store::fsm::ObserveID;
use txn_types::TimeStamp;

use std::cell::RefCell;
use std::sync::{Arc, RwLock};

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
}

impl CausalObserver {
    pub fn new(causal_manager: Arc<RegionsCausalManager>) -> CausalObserver {
        CausalObserver {
            causal_manager,
            regions_map: RefCell::new(RegionsMap::default()),
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<RocksEngine>) {
        coprocessor_host
            .registry
            .register_global_cmd_observer(100, BoxCmdObserver::new(self.clone()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::RocksEngine;
    use engine_traits::util::append_extended_fields;
    use kvproto::raft_cmdpb::*;
    use std::convert::TryInto;
    use tikv::storage::kv::TestEngineBuilder;

    #[test]
    fn test_causal_observer() {
        let manager = Arc::new(RegionsCausalManager::default());
        let ob = CausalObserver::new(manager.clone());
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
