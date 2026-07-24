use super::*;

impl Db {
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
        let slice = byte_range_slice(&bytes, start, end);
        Ok(slice.iter().map(|byte| byte.count_ones() as u64).sum())
    }

    pub async fn string_bitcount_async(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<u64, Error> {
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        let slice = byte_range_slice(&bytes, start, end);
        Ok(slice.iter().map(|byte| byte.count_ones() as u64).sum())
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
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
        let start_byte = normalize_byte_index(bytes.len(), start.unwrap_or(0)).unwrap_or(0);
        let end_byte = end
            .and_then(|idx| normalize_byte_index(bytes.len(), idx))
            .unwrap_or(bytes.len().saturating_sub(1));
        if start_byte > end_byte || start_byte >= bytes.len() {
            return Ok(if bit == 0 && bytes.is_empty() { 0 } else { -1 });
        }
        for (offset, &byte) in bytes[start_byte..=end_byte].iter().enumerate() {
            let byte_idx = start_byte + offset;
            for bit_idx in 0..8 {
                let current = (byte >> (7 - bit_idx)) & 1;
                if current == bit {
                    return Ok((byte_idx * 8 + bit_idx) as i64);
                }
            }
        }
        Ok(if bit == 0 && end.is_none() {
            (bytes.len() * 8) as i64
        } else {
            -1
        })
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
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        let start_byte = normalize_byte_index(bytes.len(), start.unwrap_or(0)).unwrap_or(0);
        let end_byte = end
            .and_then(|idx| normalize_byte_index(bytes.len(), idx))
            .unwrap_or(bytes.len().saturating_sub(1));
        if start_byte > end_byte || start_byte >= bytes.len() {
            return Ok(if bit == 0 && bytes.is_empty() { 0 } else { -1 });
        }
        for (offset, &byte) in bytes[start_byte..=end_byte].iter().enumerate() {
            let byte_idx = start_byte + offset;
            for bit_idx in 0..8 {
                let current = (byte >> (7 - bit_idx)) & 1;
                if current == bit {
                    return Ok((byte_idx * 8 + bit_idx) as i64);
                }
            }
        }
        Ok(if bit == 0 && end.is_none() {
            (bytes.len() * 8) as i64
        } else {
            -1
        })
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
        let bytes = self.get_string_bytes(key)?.unwrap_or_default();
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
        let bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        Ok(bitpos_range(&bytes, bit, start, end))
    }

    pub fn string_bitop(&self, op: &str, dest: &str, keys: &[String]) -> Result<usize, Error> {
        let values = keys
            .iter()
            .map(|key| self.get_string_bytes(key))
            .collect::<Result<Vec<_>, _>>()?;
        let max_len = values
            .iter()
            .filter_map(|value| value.as_ref().map(Vec::len))
            .max()
            .unwrap_or(0);
        let mut out = vec![0u8; max_len];
        match op.to_ascii_uppercase().as_str() {
            "NOT" => {
                if values.len() != 1 {
                    return Err(Error::msg(
                        "ERR BITOP NOT must be called with a single source key",
                    ));
                }
                let source = values[0].clone().unwrap_or_default();
                out = source.into_iter().map(|byte| !byte).collect();
            }
            "AND" | "OR" | "XOR" => {
                for (idx, output) in out.iter_mut().enumerate() {
                    let mut acc = match op.to_ascii_uppercase().as_str() {
                        "AND" => 0xFF,
                        _ => 0,
                    };
                    for value in &values {
                        let byte = value
                            .as_ref()
                            .and_then(|v| v.get(idx))
                            .copied()
                            .unwrap_or(0);
                        match op.to_ascii_uppercase().as_str() {
                            "AND" => acc &= byte,
                            "OR" => acc |= byte,
                            "XOR" => acc ^= byte,
                            _ => unreachable!(),
                        }
                    }
                    *output = acc;
                }
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
        let len = out.len();
        self.insert_string_bytes(dest.to_string(), out, None);
        Ok(len)
    }

    pub async fn string_bitop_async(
        &self,
        op: &str,
        dest: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_string_bytes_async(key).await?);
        }
        let max_len = values
            .iter()
            .filter_map(|value| value.as_ref().map(Vec::len))
            .max()
            .unwrap_or(0);
        let mut out = vec![0u8; max_len];
        match op.to_ascii_uppercase().as_str() {
            "NOT" => {
                if values.len() != 1 {
                    return Err(Error::msg(
                        "ERR BITOP NOT must be called with a single source key",
                    ));
                }
                let source = values[0].clone().unwrap_or_default();
                out = source.into_iter().map(|byte| !byte).collect();
            }
            "AND" | "OR" | "XOR" => {
                for (idx, output) in out.iter_mut().enumerate() {
                    let mut acc = match op.to_ascii_uppercase().as_str() {
                        "AND" => 0xFF,
                        _ => 0,
                    };
                    for value in &values {
                        let byte = value
                            .as_ref()
                            .and_then(|v| v.get(idx))
                            .copied()
                            .unwrap_or(0);
                        match op.to_ascii_uppercase().as_str() {
                            "AND" => acc &= byte,
                            "OR" => acc |= byte,
                            "XOR" => acc ^= byte,
                            _ => unreachable!(),
                        }
                    }
                    *output = acc;
                }
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
        let len = out.len();
        self.insert_string_bytes_async(dest.to_string(), out, None)
            .await?;
        Ok(len)
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

fn bitcount_range(bytes: &[u8], start: Option<i64>, end: Option<i64>) -> u64 {
    bit_range(bytes.len().saturating_mul(8), start, end).map_or(0, |(start, end)| {
        (start..=end)
            .map(|offset| {
                let byte = bytes[offset / 8];
                u64::from((byte >> (7 - (offset % 8))) & 1)
            })
            .sum()
    })
}

fn bitpos_range(bytes: &[u8], bit: u8, start: Option<i64>, end: Option<i64>) -> i64 {
    let Some((start, end)) = bit_range(bytes.len().saturating_mul(8), start, end) else {
        return -1;
    };
    for offset in start..=end {
        let byte = bytes[offset / 8];
        if ((byte >> (7 - (offset % 8))) & 1) == bit {
            return offset as i64;
        }
    }
    -1
}

fn bit_range(len: usize, start: Option<i64>, end: Option<i64>) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let normalize = |index: i64| -> i128 {
        if index < 0 {
            len as i128 + index as i128
        } else {
            index as i128
        }
    };
    let start = normalize(start.unwrap_or(0)).max(0);
    let end = normalize(end.unwrap_or(-1)).min(len as i128 - 1);
    if start > end || start >= len as i128 || end < 0 {
        None
    } else {
        Some((start as usize, end as usize))
    }
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

fn resize_bitmap(bytes: &mut Vec<u8>, required_bytes: usize) -> Result<(), Error> {
    if required_bytes <= bytes.len() {
        return Ok(());
    }
    bytes
        .try_reserve_exact(required_bytes - bytes.len())
        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
    bytes.resize(required_bytes, 0);
    Ok(())
}
