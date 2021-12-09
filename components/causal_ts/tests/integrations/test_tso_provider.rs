// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::TestSuite;
use causal_ts::{CausalTsProvider, TsoSimpleProvider};

#[test]
fn test_tso_simple_provider() {
    let suite = TestSuite::new(1);

    let provider = TsoSimpleProvider::new(suite.pd_cli.clone());

    let _ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 0.into(), "ts: {:?}", ts);

    suite.stop();
}
