// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tikv_util::worker::{Builder as WorkerBuilder, Worker};
use txn_types::TimeStamp;

use crate::{
    errors::{Error, Result},
    CausalTsProvider,
};

#[derive(Debug)]
struct Hlc(AtomicU64);

impl Default for Hlc {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.load(Ordering::Relaxed))
    }
}

impl Hlc {
    #[inline]
    fn next(&self) -> TimeStamp {
        let ts = self.0.fetch_add(1, Ordering::AcqRel).into(); // TODO(raw): better ordering
        debug!("(raw)HlcWithTsoAsPhyClk::InnerTs::next: {:?}", ts);
        ts
    }

    // self.next() == `to`
    #[inline]
    fn advance(&self, to: TimeStamp) {
        let before = self.0.fetch_max(to.into_inner(), Ordering::Release).into(); // TODO(raw): better ordering
        debug!("(raw)HlcWithTsoAsPhyClk::InnerTs::advance"; "before" => ?before, "after" => ?std::cmp::max(before, to));
    }
}

const TSO_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const TSO_MAX_ERROR_DURATION: Duration = Duration::from_secs(60);

// TODO(raw): SafeHLC
pub struct HlcProviderWithTsoAsPhyClk {
    hlc: Arc<Hlc>,

    pd_client: Arc<dyn PdClient>,
    tso_worker: Worker,
    tso_error: Arc<AtomicBool>,
    tso_first_err: Arc<Mutex<Option<Instant>>>, // TODO(raw): use atomic Instant

    tso_refresh_interval: Duration,
    tso_max_error_duration: Duration,
}

impl HlcProviderWithTsoAsPhyClk {
    pub fn new(pd_client: Arc<dyn PdClient>) -> Self {
        Self::new_opt(pd_client, TSO_REFRESH_INTERVAL, TSO_MAX_ERROR_DURATION)
    }

    pub fn new_opt(
        pd_client: Arc<dyn PdClient>,
        tso_refresh_interval: Duration,
        tso_max_error_duration: Duration,
    ) -> Self {
        Self {
            hlc: Arc::new(Hlc::default()),
            pd_client,
            tso_worker: WorkerBuilder::new("hlc_tso_worker").create(),
            tso_error: Arc::new(AtomicBool::new(true)), // must be initialized by an available TSO.
            tso_first_err: Arc::new(Mutex::new(None)),
            tso_refresh_interval,
            tso_max_error_duration,
        }
    }

    pub async fn init(&self) -> Result<()> {
        let tso = self.pd_client.get_tso().await?;
        self.hlc.advance(tso);
        debug!("(rawkv)HlcProviderWithTsoAsPhyClk::init"; "tso" => ?tso);

        self.tso_error.store(false, Ordering::Release);

        let hlc = self.hlc.clone();
        let pd_client = self.pd_client.clone();
        let tso_error = self.tso_error.clone();
        let tso_first_err = self.tso_first_err.clone();
        let tso_max_error_duration = self.tso_max_error_duration;

        let create_task = move || {
            let hlc = hlc.clone();
            let pd_client = pd_client.clone();
            let tso_error = tso_error.clone();
            let tso_first_err = tso_first_err.clone();
            let tso_max_error_duration = tso_max_error_duration;

            async move {
                match pd_client.get_tso().await {
                    Ok(tso) => {
                        debug!("(raw)HlcProviderWithTsoAsPhyClk::pd_client::get_tso"; "tso" => ?tso);
                        hlc.advance(tso);

                        *tso_first_err.lock().unwrap() = None;
                        tso_error.store(false, Ordering::Release);
                    }
                    Err(err) => {
                        warn!("(raw)HlcProviderWithTsoAsPhyClk::pd_client::get_tso error"; "error" => ?err);
                        // TODO(raw) more error handle.
                        let now = Instant::now();
                        let mut first_err = tso_first_err.lock().unwrap();
                        let first_err = first_err.get_or_insert(now);

                        let error_duration = now.saturating_duration_since(*first_err);
                        if error_duration >= tso_max_error_duration {
                            error!("(raw)HlcProviderWithTsoAsPhyClk go into error state"; "error_duration" => ?error_duration, "tso_max_error_duration" => ?tso_max_error_duration);
                            tso_error.store(true, Ordering::Release);
                        }
                    }
                };
            }
        };

        self.tso_worker
            .spawn_interval_async_task(self.tso_refresh_interval, create_task);
        Ok(())
    }
}

impl CausalTsProvider for HlcProviderWithTsoAsPhyClk {
    fn get_ts(&self) -> Result<TimeStamp> {
        if self.tso_error.load(Ordering::Acquire) {
            Err(Error::Tso("tso_error".to_string()))
        } else {
            let ts = self.hlc.next();
            Ok(ts)
        }
    }

    fn advance(&self, to: TimeStamp) -> Result<()> {
        self.hlc.advance(to);
        Ok(())
    }
}
