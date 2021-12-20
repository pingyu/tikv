// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, IterOptions, util::get_causal_ts};
use txn_types::TimeStamp;

use crate::storage::{Cursor, ScanMode, Snapshot, Statistics, txn::{Result as TxnResult, TxnEntry, TxnEntryScanner}};

use super::{TTLSnapshot, ttl::TTLIterator};

pub struct RawScannerConfig {
    checkpoint_ts: TimeStamp,
    // TODO(rawkv):
    // start_key: Option<Key>,
    // end_key: Option<Key>,
    // enable_ttl: bool,
}

impl RawScannerConfig {
    pub fn new(checkpoint_ts: TimeStamp) -> Self {
        Self {
            checkpoint_ts,
        }
    }
}

pub struct RawTTLForwardScanner<S: Snapshot> {
    cfg: RawScannerConfig,
    snapshot: TTLSnapshot<S>,
    cursor: Option<Cursor<TTLIterator<<S as Snapshot>::Iter>>>,
    statistics: Statistics,
}

impl<S: Snapshot> RawTTLForwardScanner<S> {
    pub fn new(
        cfg: RawScannerConfig,
        snapshot: S,
    ) -> Self {
        Self {
            cfg,
            snapshot: TTLSnapshot::from(snapshot),
            cursor: None,
            statistics: Statistics::default(),
        }
    }

    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }
    
    pub fn read_next(&mut self) -> TxnResult<Option<TxnEntry>> {
        if self.cursor.is_none() {
            let option = IterOptions::new(None, None, false);
            self.cursor = Some(Cursor::new(self.snapshot.iter_cf(CF_DEFAULT, option)?, ScanMode::Forward, false));
        }

        let cursor = self.cursor.as_mut().unwrap();
        let statistics = self.statistics.mut_cf_statistics(CF_DEFAULT);

        while cursor.valid()? {
            let key = cursor.key(statistics);
            let value = cursor.key(statistics);

            let ts = get_causal_ts(value);
            if let Some(ts) = ts {
                if ts > self.cfg.checkpoint_ts {
                    let entry = TxnEntry::Raw {
                        default: (key.to_owned(), value.to_owned()),
                    };
                    return Ok(Some(entry));
                }
            }

            cursor.next(statistics);
        }
        Ok(None)
    }
}

impl<S> TxnEntryScanner for RawTTLForwardScanner<S>
where
    S: Snapshot,
{
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }
}
