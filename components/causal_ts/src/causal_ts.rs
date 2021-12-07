// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use txn_types::TimeStamp;

pub trait CausalTsProvider {
    /// Get a new ts
    fn get_ts(&mut self) -> Result<TimeStamp>;

    /// Advance to not less than ts
    fn advance(&mut self, ts: TimeStamp) -> Result<()>;
}
