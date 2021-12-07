// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use pd_client::PdClient;
use std::sync::Arc;

use crate::errors::Result;
use crate::CausalTsProvider;
use txn_types::TimeStamp;

pub struct TsoSimpleProvider {
    pd_client: Arc<dyn PdClient>,
}

impl TsoSimpleProvider {
    pub fn new(pd_client: Arc<dyn PdClient>) -> TsoSimpleProvider {
        TsoSimpleProvider { pd_client }
    }
}

impl CausalTsProvider for TsoSimpleProvider {
    fn get_ts(&mut self) -> Result<TimeStamp> {
        let ts = block_on(self.pd_client.get_tso())?;
        warn!("TsoSimpleProvider::get_ts"; "ts" => ts,);
        Ok(ts)
    }

    fn advance(&mut self, _ts: TimeStamp) -> Result<()> {
        // TODO
        Ok(())
    }
}
