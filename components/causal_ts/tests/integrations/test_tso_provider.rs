// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use causal_ts::{CausalTsProvider, TsoSimpleProvider};

#[test]
fn test_tso_simple_provider() {
    let pd_cli = Arc::new(test_pd::TestPdClient::new(1, false));

    let provider = TsoSimpleProvider::new(pd_cli);

    let _ts = provider.get_ts().unwrap();
    // assert_eq!(ts, 0.into(), "ts: {:?}", ts);
}
