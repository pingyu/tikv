// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{util::ExtendedFields, IterOptions, CF_DEFAULT};
use txn_types::TimeStamp;

use crate::storage::{
    txn::{ErrorInner as TxnErrorInner, Result as TxnResult, TxnEntry, TxnEntryScanner},
    Cursor, ScanMode, Snapshot, Statistics,
};

use super::ttl::current_ts;

pub struct RawScannerConfig {
    checkpoint_ts: TimeStamp,
    current_ts: u64,
    // TODO(rawkv):
    // start_key: Option<Key>,
    // end_key: Option<Key>,
    // enable_ttl: bool,
}

impl RawScannerConfig {
    pub fn new(checkpoint_ts: TimeStamp) -> Self {
        Self {
            checkpoint_ts,
            current_ts: current_ts(),
        }
    }
}

pub struct RawForwardScanner<S: Snapshot> {
    cfg: RawScannerConfig,
    snapshot: S,
    cursor: Option<Cursor<S::Iter>>,
    statistics: Statistics,
}

impl<S: Snapshot> RawForwardScanner<S> {
    pub fn new(cfg: RawScannerConfig, snapshot: S) -> Self {
        Self {
            cfg,
            snapshot,
            cursor: None,
            statistics: Statistics::default(),
        }
    }

    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    pub fn read_next(&mut self) -> TxnResult<Option<TxnEntry>> {
        let statistics = self.statistics.mut_cf_statistics(CF_DEFAULT);

        let cursor = if let Some(ref mut cursor) = self.cursor {
            cursor
        } else {
            let option = IterOptions::new(None, None, false);
            let cursor = Cursor::new(
                self.snapshot.iter_cf(CF_DEFAULT, option)?,
                ScanMode::Forward,
                false,
            );

            self.cursor = Some(cursor);
            let cursor = self.cursor.as_mut().unwrap();
            if !cursor.seek_to_first(statistics) {
                return Ok(None);
            }
            cursor
        };

        while cursor.valid()? {
            let key = cursor.key(statistics);
            let value = cursor.value(statistics);

            let (fields, _, _) = ExtendedFields::extract(value).map_err(|err| {
                TxnErrorInner::Other(box_err!("ExtendedFields::extract error: {:?}", err))
            })?;

            if fields.expire_ts() == 0 || fields.expire_ts() > self.cfg.current_ts {
                if let Some(causal_ts) = fields.causal_ts() {
                    if causal_ts > self.cfg.checkpoint_ts {
                        let entry = TxnEntry::Raw {
                            default: (key.to_owned(), value.to_owned()),
                            causal_ts,
                        };
                        cursor.next(statistics);
                        return Ok(Some(entry));
                    }
                }
            }
            cursor.next(statistics);
        }
        Ok(None)
    }
}

impl<S> TxnEntryScanner for RawForwardScanner<S>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::Engine;
    use crate::storage::{SnapContext, TTLSnapshot, TestEngineBuilder};

    use engine_traits::util::append_extended_fields;
    use engine_traits::{SyncMutable, CF_DEFAULT};
    use txn_types::Key;

    #[test]
    fn test_raw_forward_scanner() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(dir.path())
            .ttl(true)
            .build()
            .unwrap();
        let kvdb = engine.get_rocksdb();

        let key1 = b"key1".to_vec();
        let mut value1 = b"value1".to_vec();
        append_extended_fields(&mut value1, 0, Some(1.into()));
        kvdb.put_cf(CF_DEFAULT, &key1, &value1).unwrap();

        let key2 = b"key2".to_vec();
        let mut value2 = b"value2".to_vec();
        append_extended_fields(&mut value2, 0, Some(2.into()));
        kvdb.put_cf(CF_DEFAULT, &key2, &value2).unwrap();

        let key3 = b"key3".to_vec();
        let mut value3 = b"value3".to_vec();
        append_extended_fields(&mut value3, 0, Some(3.into()));
        kvdb.put_cf(CF_DEFAULT, &key3, &value3).unwrap();

        let key4 = b"key4".to_vec();
        let mut value4 = b"value4".to_vec();
        append_extended_fields(&mut value4, 10, Some(4.into()));
        kvdb.put_cf(CF_DEFAULT, &key4, &value4).unwrap();

        let snapshot = engine.snapshot(SnapContext::default()).unwrap();
        let ttl_snapshot = TTLSnapshot::from(snapshot.clone());

        assert_eq!(
            ttl_snapshot.get(&Key::from_encoded_slice(b"key1")).unwrap(),
            Some(b"value1".to_vec())
        );

        assert_eq!(
            ttl_snapshot.get(&Key::from_encoded_slice(b"key4")).unwrap(),
            None
        );

        let checkpoint_ts = 1.into();
        let cfg = RawScannerConfig::new(checkpoint_ts);
        let mut scanner = RawForwardScanner::new(cfg, snapshot);

        {
            let entry = scanner.next_entry().unwrap().unwrap();
            let expected = TxnEntry::Raw {
                default: (key2, value2),
                causal_ts: 2.into(),
            };
            assert_eq!(entry, expected);
        }

        {
            let entry = scanner.next_entry().unwrap().unwrap();
            let expected = TxnEntry::Raw {
                default: (key3, value3),
                causal_ts: 3.into(),
            };
            assert_eq!(entry, expected);
        }

        let entry = scanner.next_entry().unwrap();
        assert!(entry.is_none());
    }
}
