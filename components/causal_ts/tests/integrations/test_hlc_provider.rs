// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use crate::TestSuite;
use causal_ts::{CausalTsProvider, HlcProviderWithTsoAsPhyClk};
use futures::executor::block_on;
use test_pd::util::sleep_ms;

#[test]
fn test_hlc_tso_provider() {
    let suite = TestSuite::new(1);

    suite.pd_cli.set_tso(100.into());
    let provider = HlcProviderWithTsoAsPhyClk::new(suite.pd_cli.clone());
    block_on(provider.init()).unwrap();

    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 100.into(), "ts: {:?}", ts);

    let ts1 = ts.into_inner() + 10;
    provider.advance(ts1.into()).unwrap();
    let ts2 = provider.get_ts().unwrap();
    assert_eq!(ts2, ts1.into());

    suite.stop();
}

#[test]
fn test_hlc_tso_provider_on_failure() {
    let suite = TestSuite::new(1);

    let tso_refresh_interval = 200;

    suite.pd_cli.set_tso(200.into());
    let provider = HlcProviderWithTsoAsPhyClk::new_opt(
        suite.pd_cli.clone(),
        Duration::from_millis(tso_refresh_interval),
        Duration::from_millis(tso_refresh_interval + tso_refresh_interval / 2), // into error state on the third refresh.
    );
    block_on(provider.init()).unwrap();

    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 200.into(), "ts: {:?}", ts);

    suite.pd_cli.set_tso(250.into());

    sleep_ms(tso_refresh_interval + tso_refresh_interval / 2);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 251.into(), "ts: {:?}", ts);

    // tso fail
    suite.pd_cli.set_tso(300.into());
    fail::cfg("test_raftstore_get_tso", "return(50)").unwrap();

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 252.into(), "ts: {:?}", ts);

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 253.into(), "ts: {:?}", ts);

    sleep_ms(tso_refresh_interval);
    let res = provider.get_ts();
    assert!(res.is_err(), "res: {:?}", res);

    // tso restore
    fail::remove("test_raftstore_get_tso");

    sleep_ms(tso_refresh_interval);
    let ts = provider.get_ts().unwrap();
    assert_eq!(ts, 300.into(), "ts: {:?}", ts);

    suite.stop();
}
