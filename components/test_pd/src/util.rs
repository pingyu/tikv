// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::{Config, RpcClient};
pub use raftstore::store::util::{find_peer, new_learner_peer, new_peer};
use security::{SecurityConfig, SecurityManager};
use tikv_util::config::ReadableDuration;

use kvproto::metapb;
use kvproto::pdpb::{
    ChangePeer, ChangePeerV2, CheckPolicy, Merge, RegionHeartbeatResponse, SplitRegion,
    TransferLeader,
};
use raft::eraftpb::ConfChangeType;

use std::sync::Arc;
use std::time::Duration;
use std::{thread, u64};

pub fn new_config(eps: Vec<(String, u16)>) -> Config {
    Config {
        endpoints: eps
            .into_iter()
            .map(|addr| format!("{}:{}", addr.0, addr.1))
            .collect(),
        ..Default::default()
    }
}

pub fn new_client(eps: Vec<(String, u16)>, mgr: Option<Arc<SecurityManager>>) -> RpcClient {
    let cfg = new_config(eps);
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&cfg, None, mgr).unwrap()
}

pub fn new_client_with_update_interval(
    eps: Vec<(String, u16)>,
    mgr: Option<Arc<SecurityManager>>,
    interval: ReadableDuration,
) -> RpcClient {
    let mut cfg = new_config(eps);
    cfg.update_interval = interval;
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&cfg, None, mgr).unwrap()
}

pub fn new_pd_change_peer(
    change_type: ConfChangeType,
    peer: metapb::Peer,
) -> RegionHeartbeatResponse {
    let mut change_peer = ChangePeer::default();
    change_peer.set_change_type(change_type);
    change_peer.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_change_peer(change_peer);
    resp
}

pub fn new_pd_change_peer_v2(changes: Vec<ChangePeer>) -> RegionHeartbeatResponse {
    let mut change_peer = ChangePeerV2::default();
    change_peer.set_changes(changes.into());

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_change_peer_v2(change_peer);
    resp
}

pub fn new_pd_transfer_leader(peer: metapb::Peer) -> RegionHeartbeatResponse {
    let mut transfer_leader = TransferLeader::default();
    transfer_leader.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_transfer_leader(transfer_leader);
    resp
}

pub fn new_pd_merge_region(target_region: metapb::Region) -> RegionHeartbeatResponse {
    let mut merge = Merge::default();
    merge.set_target(target_region);

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_merge(merge);
    resp
}

pub fn new_split_region(policy: CheckPolicy, keys: Vec<Vec<u8>>) -> RegionHeartbeatResponse {
    let mut split_region = SplitRegion::default();
    split_region.set_policy(policy);
    split_region.set_keys(keys.into());
    let mut resp = RegionHeartbeatResponse::default();
    resp.set_split_region(split_region);
    resp
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}
