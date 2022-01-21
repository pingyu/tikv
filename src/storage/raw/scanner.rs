// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{util::ExtendedFields, IterOptions, CF_DEFAULT, DATA_KEY_PREFIX_LEN};
use tikv_util::keybuilder::KeyBuilder;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    txn::{ErrorInner as TxnErrorInner, Result as TxnResult, TxnEntry, TxnEntryScanner},
    Cursor, ScanMode, Snapshot, Statistics,
};

use super::ttl::current_ts;

pub struct RawForwardScanner<S: Snapshot> {
    snapshot: S,
    cursor: Option<Cursor<S::Iter>>,
    statistics: Statistics,

    checkpoint_ts: TimeStamp,
    current_ts: u64,
    // TODO(rawkv): enable_ttl: bool,
    start_key: Option<Key>,
    end_key: Option<Key>,
}

impl<S: Snapshot> RawForwardScanner<S> {
    pub fn new(
        snapshot: S,
        checkpoint_ts: TimeStamp,
        start_key: Option<Key>,
        end_key: Option<Key>,
    ) -> Self {
        Self {
            snapshot,
            cursor: None,
            statistics: Statistics::default(),
            checkpoint_ts,
            current_ts: current_ts(),
            start_key,
            end_key,
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
            let (start_key, end_key) = (self.start_key.take(), self.end_key.take());
            let l_bound = if let Some(b) = start_key {
                let builder = KeyBuilder::from_vec(b.into_encoded(), DATA_KEY_PREFIX_LEN, 0);
                Some(builder)
            } else {
                None
            };
            let u_bound = if let Some(b) = end_key {
                let builder = KeyBuilder::from_vec(b.into_encoded(), DATA_KEY_PREFIX_LEN, 0);
                Some(builder)
            } else {
                None
            };

            let option = IterOptions::new(l_bound, u_bound, false);
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
            let value_with_ext = cursor.value(statistics);

            let (fields, _, _) = ExtendedFields::extract(value_with_ext).map_err(|err| {
                TxnErrorInner::Other(box_err!("ExtendedFields::extract error: {:?}", err))
            })?;

            if fields.expire_ts() == 0 || fields.expire_ts() > self.current_ts {
                if let Some(causal_ts) = fields.causal_ts() {
                    if causal_ts > self.checkpoint_ts {
                        let entry = TxnEntry::Raw {
                            key: key.to_owned(),
                            value_with_ext: value_with_ext.to_owned(),
                        };

                        debug!("RawForwardScanner::read_next"; "entry" => ?entry, "causal_ts" => causal_ts);
                        cursor.next(statistics);
                        return Ok(Some(entry));
                    }
                }
            }
            debug!("RawForwardScanner::read_next(skip)";
                "key" => &log_wrappers::Value::key(key),
                // "value" => &log_wrappers::Value::value(value_with_ext),
            );
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
        append_extended_fields(&mut value1, 0, Some(1.into()), false);
        kvdb.put_cf(CF_DEFAULT, &key1, &value1).unwrap();

        let key2 = b"key2".to_vec();
        let mut value2 = b"value2".to_vec();
        append_extended_fields(&mut value2, 0, Some(2.into()), false);
        kvdb.put_cf(CF_DEFAULT, &key2, &value2).unwrap();

        let key3 = b"key3".to_vec();
        let mut value3 = b"value3".to_vec();
        append_extended_fields(&mut value3, 0, Some(3.into()), false);
        kvdb.put_cf(CF_DEFAULT, &key3, &value3).unwrap();

        let key4 = b"key4".to_vec();
        let mut value4 = b"value4".to_vec();
        append_extended_fields(&mut value4, 10, Some(4.into()), false);
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
        {
            let mut scanner = RawForwardScanner::new(snapshot.clone(), checkpoint_ts, None, None);

            {
                let entry = scanner.next_entry().unwrap().unwrap();
                let expected = TxnEntry::Raw {
                    key: key2.clone(),
                    value_with_ext: value2.clone(),
                };
                assert_eq!(entry, expected);
            }

            {
                let entry = scanner.next_entry().unwrap().unwrap();
                let expected = TxnEntry::Raw {
                    key: key3.clone(),
                    value_with_ext: value3,
                };
                assert_eq!(entry, expected);
            }

            let entry = scanner.next_entry().unwrap();
            assert!(entry.is_none());
        }

        {
            let start_key = Key::from_encoded_maybe_unbounded(key2.clone());
            let end_key = Key::from_encoded_maybe_unbounded(key3);
            let mut scanner = RawForwardScanner::new(snapshot, checkpoint_ts, start_key, end_key);

            {
                let entry = scanner.next_entry().unwrap().unwrap();
                let expected = TxnEntry::Raw {
                    key: key2,
                    value_with_ext: value2,
                };
                assert_eq!(entry, expected);
            }

            let entry = scanner.next_entry().unwrap();
            assert!(entry.is_none());
        }
    }
}
