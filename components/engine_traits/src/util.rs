// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};

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
            flag |= 0x1 << 1;
        }
        assert!((expire_ts & (0x1_u64 << 63)) == 0);

        ExtendedFields {
            expire_ts,
            flag,
            causal_ts,
        }
    }

    #[inline]
    fn flag_has_causal_ts(flag: u8) -> bool {
        flag & 0x1 != 0
    }

    #[inline]
    fn flag_is_deleted(flag: u8) -> bool {
        (flag & 0x1 << 1) != 0
    }

    #[inline]
    fn parse_expire_ts(expire_ts_raw: u64) -> (u64, bool) {
        (
            expire_ts_raw & !(0x1_u64 << 63),
            expire_ts_raw & 0x1_u64 << 63 != 0,
        )
    }

    pub fn expire_ts(&self) -> u64 {
        self.expire_ts
    }

    pub fn is_deleted(&self) -> bool {
        Self::flag_is_deleted(self.flag)
    }

    pub fn causal_ts(&self) -> Option<TimeStamp> {
        self.causal_ts
    }

    pub fn append_to(&self, value: &mut Vec<u8>) {
        if let Some(causal_ts) = self.causal_ts {
            value.encode_u64(causal_ts.into_inner()).unwrap();
        }
        value.write_u8(self.flag).unwrap();
        value.encode_u64(self.expire_ts | 0x1_u64 << 63).unwrap(); // `expire_ts` must be the last to keep compatibility.
    }

    pub fn extract(value_with_extended: &[u8]) -> Result<(ExtendedFields, usize, Option<usize>)> {
        let mut extended = ExtendedFields::default();
        let mut len = value_with_extended.len();

        // expire_ts
        if len < number::U64_SIZE {
            return Err(Error::Codec(codec::Error::ValueLength));
        }
        let mut data = &value_with_extended[len - number::U64_SIZE..];
        let (expire_ts, extended_bit) = Self::parse_expire_ts(number::decode_u64(&mut data)?);
        len -= number::U64_SIZE;
        extended.expire_ts = expire_ts;

        // flag
        if extended_bit {
            if len < number::U8_SIZE {
                return Err(Error::Codec(codec::Error::ValueLength));
            }
            let mut data = &value_with_extended[len - number::U8_SIZE..];
            extended.flag = data.read_u8()?;
            len -= number::U8_SIZE;
        }

        // causal_ts
        let mut causal_ts_idx = None;
        if Self::flag_has_causal_ts(extended.flag) {
            if len < number::U64_SIZE {
                return Err(Error::Codec(codec::Error::ValueLength));
            }
            let mut data = &value_with_extended[len - number::U64_SIZE..];
            extended.causal_ts = Some((number::decode_u64(&mut data)?).into());
            len -= number::U64_SIZE;
            causal_ts_idx = Some(len);
        }

        Ok((extended, len, causal_ts_idx))
    }

    pub fn extract_value(
        mut value_with_extended: Vec<u8>,
        current_ts: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut len = value_with_extended.len();

        // expire_ts
        if len < number::U64_SIZE {
            return Err(Error::Codec(codec::Error::ValueLength));
        }
        let mut data = &value_with_extended[len - number::U64_SIZE..];
        let (expire_ts, extended_bit) = Self::parse_expire_ts(number::decode_u64(&mut data)?);
        len -= number::U64_SIZE;

        if expire_ts != 0 && expire_ts <= current_ts {
            return Ok(None);
        }

        if extended_bit {
            if len < number::U8_SIZE {
                return Err(Error::Codec(codec::Error::ValueLength));
            }
            let mut data = &value_with_extended[len - number::U8_SIZE..];
            let flag = data.read_u8()?;
            len -= number::U8_SIZE;

            // delete flag
            if Self::flag_is_deleted(flag) {
                return Ok(None);
            }

            // causal_ts
            if Self::flag_has_causal_ts(flag) {
                if len < number::U64_SIZE {
                    return Err(Error::Codec(codec::Error::ValueLength));
                }
                len -= number::U64_SIZE;
            }
        }

        value_with_extended.truncate(len);
        Ok(Some(value_with_extended))
    }
}

#[inline]
pub fn append_extended_fields(value: &mut Vec<u8>, expire_ts: u64, causal_ts: Option<TimeStamp>) {
    let ext_fields = ExtendedFields::new(expire_ts, false, causal_ts);
    ext_fields.append_to(value);
}

#[inline]
pub fn append_expire_ts(value: &mut Vec<u8>, expire_ts: u64) {
    // value.encode_u64(expire_ts).unwrap();
    append_extended_fields(value, expire_ts, None);
}

#[inline]
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

#[inline]
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

#[inline]
pub fn truncate_extended_fields(value_with_extended: &mut Vec<u8>) -> Result<()> {
    let (_, len, _) = ExtendedFields::extract(value_with_extended)?;
    value_with_extended.truncate(len);
    Ok(())
}

#[inline]
pub fn position_of_causal_ts(value_with_extended: &[u8]) -> Option<usize> {
    let (_, _, pos) = ExtendedFields::extract(value_with_extended).unwrap();
    pos
}

pub fn emplace_causal_ts(value_with_extended: &mut Vec<u8>, causal_ts: TimeStamp) -> Result<()> {
    let (_, _, causal_ts_idx) = ExtendedFields::extract(value_with_extended)?;
    if let Some(causal_ts_idx) = causal_ts_idx {
        let mut buf = &mut value_with_extended[causal_ts_idx..causal_ts_idx + number::U64_SIZE];
        byteorder::BigEndian::write_u64(&mut buf, causal_ts.into_inner());
        Ok(())
    } else {
        Err(Error::Codec(codec::Error::ValueLength))
    }
}

#[inline]
pub fn get_causal_ts(value_with_extended: &[u8]) -> Option<TimeStamp> {
    let (e, _, _) = ExtendedFields::extract(value_with_extended).unwrap();
    e.causal_ts()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extended_fields() {
        let testcases = vec![(0, false, None), (100, true, Some(200.into()))];

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

            let res = ExtendedFields::extract_value(value.clone(), 0).unwrap();
            assert_eq!(res, Some(b"value".to_vec()));

            let res = ExtendedFields::extract_value(value, 100).unwrap();
            assert_eq!(res, None);
        }

        {
            let mut value = b"value".to_vec();
            let e = ExtendedFields::new(100, true, Some(200.into()));
            e.append_to(&mut value);

            assert_eq!(get_expire_ts(&value).unwrap(), 100);
            assert_eq!(strip_extended_fields(&value), b"value");
            assert_eq!(position_of_causal_ts(&value), Some(5));

            let res = ExtendedFields::extract_value(value.clone(), 0).unwrap();
            assert_eq!(res, None);

            truncate_extended_fields(&mut value).unwrap();
            assert_eq!(value, b"value".to_vec());
        }
    }

    #[test]
    fn test_extended_fields_emplace_causal_ts() {
        let mut value = b"value".to_vec();
        append_extended_fields(&mut value, 100, Some(0.into()));
        emplace_causal_ts(&mut value, 200.into()).unwrap();

        assert_eq!(get_causal_ts(&value), Some(200.into()));
        assert_eq!(strip_extended_fields(&value), b"value");
        let (e, _, _) = ExtendedFields::extract(&value).unwrap();
        assert_eq!(e, ExtendedFields::new(100, false, Some(200.into())));
    }
}
