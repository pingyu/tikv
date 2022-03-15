// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::{CausalTsProvider, Error};
use futures::executor::block_on;
use parking_lot::RwLock;
use pd_client::PdClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tikv_util::time::Duration;
use tikv_util::worker::{Builder as WorkerBuilder, Worker};
use txn_types::TimeStamp;

/// A simple implementation acquiring TSO on every request.
/// For test purpose only. Do not use in production.
pub struct TsoSimpleProvider {
    pd_client: Arc<dyn PdClient>,
}

impl TsoSimpleProvider {
    pub fn new(pd_client: Arc<dyn PdClient>) -> TsoSimpleProvider {
        TsoSimpleProvider { pd_client }
    }
}

impl CausalTsProvider for TsoSimpleProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        let ts = block_on(self.pd_client.get_tso())?;
        debug!("TsoSimpleProvider::get_ts"; "ts" => ?ts);
        Ok(ts)
    }

    fn advance(&self, _ts: TimeStamp) -> Result<()> {
        Ok(())
    }
}

// One TSO for every batch of Raft put message, so 1.6w should be enough.
const TSO_BATCH_INIT_SIZE: u32 = 16384;
const TSO_BATCH_RENEW_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Default)]
struct TsoBatch {
    size: u32,
    physical: u64,
    logical_end: u64,
    logical_start: AtomicU64,
}

pub struct CachedTsoProvider {
    pd_client: Arc<dyn PdClient>,
    tso_batch: Arc<RwLock<TsoBatch>>,
    renew_worker: Worker,
    renew_interval: Duration,
}

impl CachedTsoProvider {
    pub async fn new(pd_client: Arc<dyn PdClient>) -> Result<Self> {
        Self::new_opt(pd_client, TSO_BATCH_RENEW_INTERVAL).await
    }

    pub async fn new_opt(pd_client: Arc<dyn PdClient>, renew_interval: Duration) -> Result<Self> {
        let s = Self {
            pd_client: pd_client.clone(),
            tso_batch: Arc::new(RwLock::new(TsoBatch::default())),
            renew_worker: WorkerBuilder::new("causal_ts_cached_tso_worker").create(),
            renew_interval,
        };
        s.init().await?;
        Ok(s)
    }

    async fn renew_tso_batch(&self, need_flush: bool) -> Result<()> {
        Self::renew_tso_batch_internal(self.pd_client.clone(), self.tso_batch.clone(), need_flush)
            .await
    }

    async fn renew_tso_batch_internal(
        pd_client: Arc<dyn PdClient>,
        tso_batch: Arc<RwLock<TsoBatch>>,
        need_flush: bool,
    ) -> Result<()> {
        let new_batch_size = {
            let batch = tso_batch.read();
            let left_size = batch
                .logical_end
                .saturating_sub(batch.logical_start.load(Ordering::Relaxed));
            Self::calc_new_batch_size(batch.size, left_size as u32)
        };

        match pd_client.batch_get_tso(new_batch_size).await {
            Err(err) => {
                if need_flush {
                    let batch = tso_batch.write();
                    batch
                        .logical_start
                        .store(batch.logical_end + 1, Ordering::Relaxed);
                }
                Err(err.into())
            }
            Ok(ts) => {
                let (physical, logical) = (ts.physical(), ts.logical());
                let mut batch = tso_batch.write();
                batch.size = new_batch_size;
                batch.physical = physical;
                batch.logical_end = logical;
                batch.logical_start.store(
                    (logical + 1).checked_sub(new_batch_size as u64).unwrap(),
                    Ordering::Relaxed,
                );
                Ok(())
            }
        }
    }

    fn calc_new_batch_size(batch_size: u32, left_size: u32) -> u32 {
        if left_size > batch_size * 3 / 4 {
            batch_size << 1
        } else if left_size < batch_size / 4 {
            std::cmp::max(batch_size >> 1, TSO_BATCH_INIT_SIZE)
        } else {
            batch_size
        }
    }

    async fn init(&self) -> Result<()> {
        self.renew_tso_batch(true).await?;

        let pd_client = self.pd_client.clone();
        let tso_batch = self.tso_batch.clone();
        let task = move || {
            let pd_client = pd_client.clone();
            let tso_batch = tso_batch.clone();
            async move {
                let _ = Self::renew_tso_batch_internal(pd_client, tso_batch, false).await;
            }
        };

        // Duration::ZERO means never renew. For test purpose ONLY.
        if self.renew_interval > Duration::ZERO {
            self.renew_worker
                .spawn_interval_async_task(self.renew_interval, task);
        }
        Ok(())
    }
}

impl CausalTsProvider for CachedTsoProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        let batch = self.tso_batch.read();
        let logical = batch.logical_start.fetch_add(1, Ordering::Relaxed);
        if logical <= batch.logical_end {
            Ok(TimeStamp::compose(batch.physical, logical))
        } else {
            Err(Error::TsoBatchUsedUp)
        }
    }

    fn advance(&self, _ts: TimeStamp) -> Result<()> {
        block_on(self.renew_tso_batch(true))
    }
}
