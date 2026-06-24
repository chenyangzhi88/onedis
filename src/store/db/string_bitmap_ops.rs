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
            bytes.resize(byte_idx + 1, 0);
        }
        let mask = 1u8 << (7 - (offset % 8));
        let old = if bytes[byte_idx] & mask == 0 { 0 } else { 1 };
        if bit == 1 {
            bytes[byte_idx] |= mask;
        } else {
            bytes[byte_idx] &= !mask;
        }
        self.insert_string_bytes(key.to_string(), bytes, None);
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
        let mut bytes = self.get_string_bytes_async(key).await?.unwrap_or_default();
        let byte_idx = offset / 8;
        if bytes.len() <= byte_idx {
            bytes.resize(byte_idx + 1, 0);
        }
        let mask = 1u8 << (7 - (offset % 8));
        let old = if bytes[byte_idx] & mask == 0 { 0 } else { 1 };
        if bit == 1 {
            bytes[byte_idx] |= mask;
        } else {
            bytes[byte_idx] &= !mask;
        }
        self.set_string_bytes_async(
            key.to_string(),
            bytes,
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )
        .await?;
        Ok(old)
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
            return Ok(if bit == 0 {
                (bytes.len() * 8) as i64
            } else {
                -1
            });
        }
        for byte_idx in start_byte..=end_byte {
            let byte = bytes[byte_idx];
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
            return Ok(if bit == 0 {
                (bytes.len() * 8) as i64
            } else {
                -1
            });
        }
        for byte_idx in start_byte..=end_byte {
            let byte = bytes[byte_idx];
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
                for idx in 0..max_len {
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
                    out[idx] = acc;
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
                for idx in 0..max_len {
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
                    out[idx] = acc;
                }
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
        let len = out.len();
        self.insert_string_bytes_async(dest.to_string(), out, None)
            .await;
        Ok(len)
    }

    pub fn string_read_bits(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        signed: bool,
    ) -> Result<i64, Error> {
        if width == 0 || width > 63 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let mut value = 0u64;
        for bit_idx in 0..width {
            value = (value << 1) | self.string_get_bit(key, offset + bit_idx)? as u64;
        }
        if signed && width < 64 && (value & (1u64 << (width - 1))) != 0 {
            Ok((value as i64) - (1i64 << width))
        } else {
            Ok(value as i64)
        }
    }

    pub async fn string_read_bits_async(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        signed: bool,
    ) -> Result<i64, Error> {
        if width == 0 || width > 63 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let mut value = 0u64;
        for bit_idx in 0..width {
            value = (value << 1) | self.string_get_bit_async(key, offset + bit_idx).await? as u64;
        }
        if signed && width < 64 && (value & (1u64 << (width - 1))) != 0 {
            Ok((value as i64) - (1i64 << width))
        } else {
            Ok(value as i64)
        }
    }

    pub fn string_write_bits(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        value: i64,
    ) -> Result<(), Error> {
        if width == 0 || width > 63 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let mask = if width == 63 {
            u64::MAX >> 1
        } else {
            (1u64 << width) - 1
        };
        let value = (value as u64) & mask;
        for bit_idx in 0..width {
            let shift = width - bit_idx - 1;
            let bit = ((value >> shift) & 1) as u8;
            self.string_set_bit(key, offset + bit_idx, bit)?;
        }
        Ok(())
    }

    pub async fn string_write_bits_async(
        &self,
        key: &str,
        offset: usize,
        width: usize,
        value: i64,
    ) -> Result<(), Error> {
        if width == 0 || width > 63 {
            return Err(Error::msg("ERR unsupported bitfield type"));
        }
        let mask = if width == 63 {
            u64::MAX >> 1
        } else {
            (1u64 << width) - 1
        };
        let value = (value as u64) & mask;
        for bit_idx in 0..width {
            let shift = width - bit_idx - 1;
            let bit = ((value >> shift) & 1) as u8;
            self.string_set_bit_async(key, offset + bit_idx, bit)
                .await?;
        }
        Ok(())
    }
}
