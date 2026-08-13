use super::*;

impl Db {
    pub(crate) fn string_get_bit_from_live_raw(
        raw: Option<&[u8]>,
        offset: usize,
    ) -> Result<u8, Error> {
        let bytes = match raw {
            Some(raw) => {
                decode_string_bytes_slice(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?
            }
            None => &[],
        };
        let byte = bytes.get(offset / 8).copied().unwrap_or(0);
        Ok((byte >> (7 - (offset % 8))) & 1)
    }

    pub(crate) fn string_bitcount_from_live_raw(
        raw: Option<&[u8]>,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<u64, Error> {
        let bytes = match raw {
            Some(raw) => {
                decode_string_bytes_slice(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?
            }
            None => &[],
        };
        Ok(if bit_unit {
            bitcount_range(bytes, start, end)
        } else {
            byte_bitcount_range(bytes, start, end)
        })
    }

    pub(crate) fn string_bitpos_from_live_raw(
        raw: Option<&[u8]>,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<i64, Error> {
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let Some(raw) = raw else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        let bytes = decode_string_bytes_slice(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
        Ok(if bit_unit {
            bitpos_range(bytes, bit, start, end)
        } else {
            byte_bitpos_range(bytes, bit, start, end)
        })
    }

    pub(crate) fn string_range_from_live_raw(
        raw: Option<&[u8]>,
        start: i64,
        end: i64,
    ) -> Result<&[u8], Error> {
        let bytes = match raw {
            Some(raw) => {
                decode_string_bytes_slice(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?
            }
            None => &[],
        };
        let Some((start, end)) = redis_range(bytes.len(), Some(start), Some(end)) else {
            return Ok(&[]);
        };
        Ok(&bytes[start..=end])
    }

    pub async fn string_get_bit_async(&self, key: &str, offset: usize) -> Result<u8, Error> {
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        let byte = bytes.get(offset / 8).copied().unwrap_or(0);
        Ok((byte >> (7 - (offset % 8))) & 1)
    }

    pub fn string_get_bit(&self, key: &str, offset: usize) -> Result<u8, Error> {
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
        let byte = bytes.get(offset / 8).copied().unwrap_or(0);
        Ok((byte >> (7 - (offset % 8))) & 1)
    }

    pub fn string_set_bit(&self, key: &str, offset: usize, bit: u8) -> Result<u8, Error> {
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let mut bytes = self.get_string_bytes(key)?.unwrap_or_default();
        let byte_idx = offset / 8;
        if bytes.len() <= byte_idx {
            resize_bitmap(&mut bytes, byte_idx.saturating_add(1))?;
        }
        let mask = 1u8 << (7 - (offset % 8));
        let old = if bytes[byte_idx] & mask == 0 { 0 } else { 1 };
        if bit == 1 {
            bytes[byte_idx] |= mask;
        } else {
            bytes[byte_idx] &= !mask;
        }
        self.set_string_bytes(
            key.to_string(),
            bytes,
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )?;
        Ok(old)
    }

    pub async fn string_set_bit_async(
        &self,
        key: &str,
        offset: usize,
        bit: u8,
    ) -> Result<u8, Error> {
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        self.mutate_string_bytes_async(key, |bytes, _| {
            let byte_idx = offset / 8;
            if bytes.len() <= byte_idx {
                resize_bitmap(bytes, byte_idx.saturating_add(1))?;
            }
            let mask = 1u8 << (7 - (offset % 8));
            let old = u8::from(bytes[byte_idx] & mask != 0);
            if bit == 1 {
                bytes[byte_idx] |= mask;
            } else {
                bytes[byte_idx] &= !mask;
            }
            Ok(old)
        })
        .await
    }

    pub fn string_bitcount(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<u64, Error> {
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
        Ok(byte_bitcount_range(&bytes, start, end))
    }

    pub async fn string_bitcount_async(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<u64, Error> {
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        Ok(byte_bitcount_range(&bytes, start, end))
    }

    pub fn string_bitcount_with_unit(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<u64, Error> {
        if !bit_unit {
            return self.string_bitcount(key, start, end);
        }
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
        Ok(bitcount_range(&bytes, start, end))
    }

    pub async fn string_bitcount_with_unit_async(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<u64, Error> {
        if !bit_unit {
            return self.string_bitcount_async(key, start, end).await;
        }
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        Ok(bitcount_range(&bytes, start, end))
    }

    pub fn string_bitpos(
        &self,
        key: &str,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64, Error> {
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let Some(bytes) = self.get_string_bytes(key)? else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        Ok(byte_bitpos_range(&bytes, bit, start, end))
    }

    pub async fn string_bitpos_async(
        &self,
        key: &str,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64, Error> {
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let Some(bytes) = self.get_string_bytes_async(key).await? else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        Ok(byte_bitpos_range(&bytes, bit, start, end))
    }

    pub fn string_bitpos_with_unit(
        &self,
        key: &str,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<i64, Error> {
        if !bit_unit {
            return self.string_bitpos(key, bit, start, end);
        }
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let Some(bytes) = self.get_string_bytes(key)? else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        Ok(bitpos_range(&bytes, bit, start, end))
    }

    pub async fn string_bitpos_with_unit_async(
        &self,
        key: &str,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_unit: bool,
    ) -> Result<i64, Error> {
        if !bit_unit {
            return self.string_bitpos_async(key, bit, start, end).await;
        }
        if bit > 1 {
            return Err(Error::msg("ERR bit is not an integer or out of range"));
        }
        let Some(bytes) = self.get_string_bytes_async(key).await? else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        Ok(bitpos_range(&bytes, bit, start, end))
    }

    pub fn string_bitop(&self, op: &str, dest: &str, keys: &[String]) -> Result<usize, Error> {
        let op = validate_bitop(op, keys.len())?;
        let mut out = Vec::new();
        for (source_index, key) in keys.iter().enumerate() {
            let source = self.get_string_bytes(key)?.unwrap_or_default();
            combine_bitop(&mut out, &source, op, source_index)?;
        }
        if op == BitOperation::Not {
            out.iter_mut().for_each(|byte| *byte = !*byte);
        }
        let len = out.len();
        if len == 0 {
            self.delete_key(dest);
        } else {
            self.insert_string_bytes(dest.to_string(), out, None);
        }
        Ok(len)
    }

    pub async fn string_bitop_async(
        &self,
        op: &str,
        dest: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let op = validate_bitop(op, keys.len())?;
        let mut logical_keys = Vec::with_capacity(keys.len().saturating_add(1));
        logical_keys.push(dest);
        for key in keys {
            if !logical_keys.contains(&key.as_str()) {
                logical_keys.push(key);
            }
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            logical_keys.iter().map(|key| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        let raw_keys = logical_keys
            .iter()
            .map(|key| self.mk(key))
            .collect::<Vec<_>>();

        for _ in 0..64 {
            let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
            let now = now_ms();
            let mut out = Vec::new();
            for (source_index, key) in keys.iter().enumerate() {
                let position = logical_keys
                    .iter()
                    .position(|candidate| *candidate == key)
                    .expect("BITOP source key is observed");
                let source = match observations[position].value() {
                    Some(raw) if decode_expire_ms(raw) == 0 || now < decode_expire_ms(raw) => {
                        decode_string_bytes_slice(raw)
                            .ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?
                    }
                    _ => &[],
                };
                combine_bitop(&mut out, source, op, source_index)?;
            }
            if op == BitOperation::Not {
                out.iter_mut().for_each(|byte| *byte = !*byte);
            }
            let len = out.len();
            let old_dest_raw = observations[0].value().map(AsRef::as_ref);
            let old_expire_ms = old_dest_raw
                .and_then(decode_meta_header)
                .map_or(0, |header| header.expire_ms);
            let mut batch = WriteBatch::new();
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, dest, old_expire_ms);
            } else {
                self.prepare_string_overwrite_to_batch(&mut batch, dest, old_dest_raw);
                self.write_string_to_batch_with_deferred_old_raw(
                    &mut batch,
                    dest,
                    &out,
                    0,
                    old_dest_raw,
                );
            }
            let conditions = observations
                .iter()
                .map(CompareCondition::from_observed)
                .collect::<Vec<_>>();
            match self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await
            {
                Ok(true) => {
                    self.changes.fetch_add(1, Ordering::Relaxed);
                    return Ok(len);
                }
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR BITOP write conflict"))
    }

    pub fn string_read_bits(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        signed: bool,
    ) -> Result<i64, Error> {
        if width == 0 || width > 64 || (!signed && width == 64) {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
        read_bits_from(&bytes, offset, width, signed)
    }

    pub async fn string_read_bits_async(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        signed: bool,
    ) -> Result<i64, Error> {
        if width == 0 || width > 64 || (!signed && width == 64) {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        read_bits_from(&bytes, offset, width, signed)
    }

    pub fn string_write_bits(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        value: i64,
    ) -> Result<(), Error> {
        if width == 0 || width > 64 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let mut bytes = self.get_string_bytes(key)?.unwrap_or_default();
        write_bits_into(&mut bytes, offset, width, value)?;
        self.set_string_bytes(
            key.to_string(),
            bytes,
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )?;
        Ok(())
    }

    pub async fn string_write_bits_async(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        value: i64,
    ) -> Result<(), Error> {
        if width == 0 || width > 64 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        self.mutate_string_bytes_async(key, |bytes, _| write_bits_into(bytes, offset, width, value))
            .await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BitOperation {
    And,
    Or,
    Xor,
    Not,
}

fn validate_bitop(op: &str, source_count: usize) -> Result<BitOperation, Error> {
    let op = match op.to_ascii_uppercase().as_str() {
        "AND" => BitOperation::And,
        "OR" => BitOperation::Or,
        "XOR" => BitOperation::Xor,
        "NOT" if source_count == 1 => BitOperation::Not,
        "NOT" => {
            return Err(Error::msg(
                "ERR BITOP NOT must be called with a single source key",
            ));
        }
        _ => return Err(Error::msg("ERR syntax error")),
    };
    if source_count == 0 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'bitop' command",
        ));
    }
    Ok(op)
}

fn combine_bitop(
    out: &mut Vec<u8>,
    source: &[u8],
    op: BitOperation,
    source_index: usize,
) -> Result<(), Error> {
    if source_index == 0 {
        resize_bitmap(out, source.len())?;
        out.copy_from_slice(source);
        return Ok(());
    }
    resize_bitmap(out, out.len().max(source.len()))?;
    for (index, output) in out.iter_mut().enumerate() {
        let byte = source.get(index).copied().unwrap_or(0);
        match op {
            BitOperation::And => *output &= byte,
            BitOperation::Or => *output |= byte,
            BitOperation::Xor => *output ^= byte,
            BitOperation::Not => unreachable!("NOT has exactly one source"),
        }
    }
    Ok(())
}

fn byte_bitcount_range(bytes: &[u8], start: Option<i64>, end: Option<i64>) -> u64 {
    redis_range(bytes.len(), start, end).map_or(0, |(start, end)| {
        bytes[start..=end]
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum()
    })
}

fn byte_bitpos_range(bytes: &[u8], bit: u8, start: Option<i64>, end: Option<i64>) -> i64 {
    let Some((start, end_index)) = redis_range(bytes.len(), start, end) else {
        return -1;
    };
    for (relative_index, byte) in bytes[start..=end_index].iter().copied().enumerate() {
        let candidate = if bit == 1 { byte } else { !byte };
        if candidate != 0 {
            return ((start + relative_index) * 8) as i64 + i64::from(candidate.leading_zeros());
        }
    }
    if bit == 0 && end.is_none() {
        (bytes.len() * 8) as i64
    } else {
        -1
    }
}

fn bitcount_range(bytes: &[u8], start: Option<i64>, end: Option<i64>) -> u64 {
    bit_range(bytes.len().saturating_mul(8), start, end).map_or(0, |(start, end)| {
        let first_byte = start / 8;
        let last_byte = end / 8;
        (first_byte..=last_byte)
            .map(|byte_index| {
                u64::from((bytes[byte_index] & bit_range_mask(byte_index, start, end)).count_ones())
            })
            .sum()
    })
}

fn bitpos_range(bytes: &[u8], bit: u8, start: Option<i64>, end: Option<i64>) -> i64 {
    let Some((start, end_index)) = bit_range(bytes.len().saturating_mul(8), start, end) else {
        return -1;
    };
    for (byte_index, byte) in bytes
        .iter()
        .enumerate()
        .take(end_index / 8 + 1)
        .skip(start / 8)
    {
        let range_mask = bit_range_mask(byte_index, start, end_index);
        let candidate = if bit == 1 {
            *byte & range_mask
        } else {
            !*byte & range_mask
        };
        if candidate != 0 {
            return (byte_index * 8) as i64 + i64::from(candidate.leading_zeros());
        }
    }
    if bit == 0 && end.is_none() {
        (bytes.len() * 8) as i64
    } else {
        -1
    }
}

fn bit_range(len: usize, start: Option<i64>, end: Option<i64>) -> Option<(usize, usize)> {
    redis_range(len, start, end)
}

fn redis_range(len: usize, start: Option<i64>, end: Option<i64>) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let raw_start = start.unwrap_or(0);
    let raw_end = end.map_or(len as i128 - 1, i128::from);
    if raw_start < 0 && raw_end < 0 && i128::from(raw_start) > raw_end {
        return None;
    }
    let normalize = |index: i128| {
        if index < 0 {
            len as i128 + index
        } else {
            index
        }
    };
    let start = normalize(i128::from(raw_start)).max(0);
    let end = normalize(raw_end).max(0).min(len as i128 - 1);
    if start > end || start >= len as i128 {
        None
    } else {
        Some((start as usize, end as usize))
    }
}

fn bit_range_mask(byte_index: usize, start: usize, end: usize) -> u8 {
    let byte_start = byte_index * 8;
    let first_bit = start.saturating_sub(byte_start).min(7);
    let last_bit = end.saturating_sub(byte_start).min(7);
    (u8::MAX >> first_bit) & (u8::MAX << (7 - last_bit))
}

pub(crate) fn read_bits_from(
    bytes: &[u8],
    offset: usize,
    width: usize,
    signed: bool,
) -> Result<i64, Error> {
    offset
        .checked_add(width)
        .ok_or_else(|| Error::msg("ERR bit offset is not an integer or out of range"))?;
    let mut value = 0u64;
    for bit_idx in 0..width {
        let absolute_bit = offset + bit_idx;
        let byte = bytes.get(absolute_bit / 8).copied().unwrap_or(0);
        value = (value << 1) | ((byte >> (7 - (absolute_bit % 8))) & 1) as u64;
    }
    if signed && width == 64 {
        Ok(value as i64)
    } else if signed && (value & (1u64 << (width - 1))) != 0 {
        Ok((value as i64) - (1i64 << width))
    } else {
        Ok(value as i64)
    }
}

pub(crate) fn write_bits_into(
    bytes: &mut Vec<u8>,
    offset: usize,
    width: usize,
    value: i64,
) -> Result<(), Error> {
    let required_bits = offset
        .checked_add(width)
        .ok_or_else(|| Error::msg("ERR bit offset is not an integer or out of range"))?;
    let required_bytes = required_bits.saturating_add(7) / 8;
    resize_bitmap(bytes, required_bytes)?;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    let value = (value as u64) & mask;
    for bit_idx in 0..width {
        let absolute_bit = offset + bit_idx;
        let byte_idx = absolute_bit / 8;
        let bit_mask = 1u8 << (7 - (absolute_bit % 8));
        let shift = width - bit_idx - 1;
        if (value >> shift) & 1 == 1 {
            bytes[byte_idx] |= bit_mask;
        } else {
            bytes[byte_idx] &= !bit_mask;
        }
    }
    Ok(())
}

pub(in crate::store::db) fn resize_bitmap(
    bytes: &mut Vec<u8>,
    required_bytes: usize,
) -> Result<(), Error> {
    if required_bytes <= bytes.len() {
        return Ok(());
    }
    if required_bytes > crate::frame::MAX_BULK_STRING_BYTES {
        return Err(Error::msg("ERR string exceeds maximum allowed size"));
    }
    bytes
        .try_reserve_exact(required_bytes - bytes.len())
        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
    bytes.resize(required_bytes, 0);
    Ok(())
}
