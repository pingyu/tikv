// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ReadBytesExt, WriteBytesExt};

use super::{Error, Result};
use tikv_util::codec;
use tikv_util::codec::number::{self, NumberEncoder};
use txn_types::TimeStamp;

/// Check if key in range [`start_key`, `end_key`).
#[allow(dead_code)]
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange(
            key.to_vec(),
            region_id,
            start_key.to_vec(),
            end_key.to_vec(),
        ))
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct ExtendedFields {
    expire_ts: u64,
    /// flag bits:
    ///   0: causal_ts
    ///   1: 1=deleted, 0=not deleted
    flag: u8,
    causal_ts: Option<TimeStamp>,
}

impl ExtendedFields {
    pub fn new(expire_ts: u64, is_deleted: bool, causal_ts: Option<TimeStamp>) -> ExtendedFields {
        let mut flag: u8 = 0;
        if causal_ts.is_some() {
            flag |= 0x1;
        }
        if is_deleted {
            flag |= 0x1<<1;
        }
        assert!(expire_ts & (0x1<<63) == 0);

        ExtendedFields {
            expire_ts,
            flag,
            causal_ts,
        }
    }

    pub fn expire_ts(&self) -> u64 {
        self.expire_ts
    }

    pub fn is_deleted(&self) -> bool {
        (self.flag & 0x1<<1) != 0
    }

    pub fn causal_ts(&self) -> Option<TimeStamp> {
        self.causal_ts
    }

    pub fn append_to(&self, value: &mut Vec<u8>) {
        if let Some(causal_ts) = self.causal_ts {
            value.encode_u64(causal_ts.into_inner()).unwrap();
        }
        value.write_u8(self.flag).unwrap();
        value.encode_u64(self.expire_ts | 0x1<<63).unwrap(); // must be the last to keep compatible.
    }

    pub fn extract(value_with_extended: &[u8]) -> Result<(ExtendedFields, usize, Option<usize>)> {
        let mut extended = ExtendedFields::default();
        let mut len = value_with_extended.len();

        // expire_ts
        if len < number::U64_SIZE {
            return Err(Error::Codec(codec::Error::ValueLength));
        }
        let mut data = &value_with_extended[len - number::U64_SIZE..];
        extended.expire_ts = number::decode_u64(&mut data)?;
        len -= number::U64_SIZE;

        // flag
        if extended.expire_ts & 0x1<<63 != 0 {
            extended.expire_ts &= !(0x1<<63);

            if len < number::U8_SIZE {
                return Err(Error::Codec(codec::Error::ValueLength));
            }
            let mut data = &value_with_extended[len - number::U8_SIZE..];
            extended.flag = data.read_u8()?;
            len -= number::U8_SIZE;
        }

        // causal_ts
        let mut causal_ts_idx = None;
        if extended.flag & 0x1 != 0 {
            if len < number::U64_SIZE {
                return Err(Error::Codec(codec::Error::ValueLength));
            }
            let mut data = &value_with_extended[len - number::U64_SIZE..];
            extended.causal_ts = Some(TimeStamp::from(number::decode_u64(&mut data)?));
            len -= number::U64_SIZE;
            causal_ts_idx = Some(len);
        }
            
        Ok((extended, len, causal_ts_idx))
    }
}

pub fn append_extended_fields(value: &mut Vec<u8>, expire_ts: u64, causal_ts: Option<TimeStamp>) {
    let ext_fields = ExtendedFields::new(expire_ts, false, causal_ts);
    ext_fields.append_to(value);
}


pub fn append_expire_ts(value: &mut Vec<u8>, expire_ts: u64) {
    // value.encode_u64(expire_ts).unwrap();
    append_extended_fields(value, expire_ts, None);
}

pub fn get_expire_ts(value_with_ttl: &[u8]) -> Result<u64> {
    // let len = value_with_ttl.len();
    // if len < number::U64_SIZE {
    //     return Err(Error::Codec(codec::Error::ValueLength));
    // }
    // let mut ts = &value_with_ttl[len - number::U64_SIZE..];
    // Ok(number::decode_u64(&mut ts)?)
    let (extended, _, _) = ExtendedFields::extract(value_with_ttl)?;
    Ok(extended.expire_ts)
}

// pub fn strip_expire_ts(value_with_ttl: &[u8]) -> &[u8] {
//     let len = value_with_ttl.len();
//     &value_with_ttl[..len - number::U64_SIZE]
// }

pub fn strip_extended_fields(value_with_extended: &[u8]) -> &[u8] {
    let (_, len, _) = ExtendedFields::extract(value_with_extended).unwrap();
    &value_with_extended[..len]
}

// pub fn truncate_expire_ts(value_with_ttl: &mut Vec<u8>) -> Result<()> {
//     let len = value_with_ttl.len();
//     if len < number::U64_SIZE {
//         return Err(Error::Codec(codec::Error::ValueLength));
//     }
//     value_with_ttl.truncate(len - number::U64_SIZE);
//     Ok(())
// }

pub fn truncate_extended_fields(value_with_extended: &mut Vec<u8>) -> Result<()> {
    let (_, len, _) = ExtendedFields::extract(value_with_extended)?;
    value_with_extended.truncate(len);
    Ok(())
}

pub fn position_of_causal_ts(value_with_extended: &[u8]) -> Option<usize> {
    let (_, _, pos) = ExtendedFields::extract(value_with_extended).unwrap();
    pos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extended_fields() {
        let testcases = vec![
            (0, false, None),
            (100, true, Some(TimeStamp::from(200))),
        ];

        for (i, (expire_ts, is_deleted, causal_ts)) in testcases.into_iter().enumerate() {
            let e = ExtendedFields::new(expire_ts, is_deleted, causal_ts);
            assert_eq!(e.expire_ts(), expire_ts);
            assert_eq!(e.is_deleted(), is_deleted);
            assert_eq!(e.causal_ts(), causal_ts);

            let mut value = b"value".to_vec();
            e.append_to(&mut value);

            let (e1, len, causal_ts_idx) = ExtendedFields::extract(&value).unwrap();
            assert_eq!(e, e1, "case {}", i);
            assert_eq!(len, 5, "case {}", i);
            assert_eq!(causal_ts_idx, causal_ts.and(Some(5)), "case {}", i);
        }
    }

    #[test]
    fn test_extended_fields_compatible() {
        let mut value = b"value".to_vec();
        value.encode_u64(100).unwrap();

        let (e, len, causal_ts_idx) = ExtendedFields::extract(&value).unwrap();
        assert_eq!(e, ExtendedFields::new(100, false, None));
        assert_eq!(len, 5);
        assert_eq!(causal_ts_idx, None);
    }

    #[test]
    fn test_extended_fields_utils() {
        {
            let mut value = b"value".to_vec();
            append_expire_ts(&mut value, 100);
            let (e, _, _) = ExtendedFields::extract(&value).unwrap();
            assert_eq!(e, ExtendedFields::new(100, false, None));
            assert_eq!(position_of_causal_ts(&value), None);
        }

        {
            let mut value = b"value".to_vec();
            let e = ExtendedFields::new(100, true, Some(TimeStamp::from(200)));
            e.append_to(&mut value);

            assert_eq!(get_expire_ts(&value).unwrap(), 100);
            assert_eq!(strip_extended_fields(&value), b"value");
            assert_eq!(position_of_causal_ts(&value), Some(5));

            truncate_extended_fields(&mut value).unwrap();
            assert_eq!(value, b"value".to_vec());
        }
    }
}
