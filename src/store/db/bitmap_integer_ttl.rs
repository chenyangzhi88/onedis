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

    pub fn update_integer_string<F>(&self, key: &str, update: F) -> Result<i64, Error>
    where
        F: FnOnce(i64) -> Option<i64>,
    {
        self.update_integer_string_read_modify_write(key, update)
    }

    pub async fn update_integer_string_async<F>(&self, key: &str, update: F) -> Result<i64, Error>
    where
        F: FnOnce(i64) -> Option<i64>,
    {
        self.expire_if_needed_async(key).await;

        let key_bytes = self.mk(key);
        let (expire_ms, current) = self.read_integer_string_for_update(&key_bytes)?;

        let next = update(current)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        self.changes.fetch_add(1, Ordering::Relaxed);

        let encoded = encode_raw_string(next.to_string().as_bytes(), expire_ms);
        let mut batch = WriteBatch::new();
        batch.put(&key_bytes, &encoded);
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        self.write_batch_if_not_empty_async(&batch).await;

        Ok(next)
    }

    fn update_integer_string_read_modify_write<F>(&self, key: &str, update: F) -> Result<i64, Error>
    where
        F: FnOnce(i64) -> Option<i64>,
    {
        self.expire_if_needed(key);

        let key_bytes = self.mk(key);
        let (expire_ms, current) = self.read_integer_string_for_update(&key_bytes)?;

        let next = update(current)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        self.changes.fetch_add(1, Ordering::Relaxed);

        let encoded = encode_raw_string(next.to_string().as_bytes(), expire_ms);
        let mut batch = WriteBatch::new();
        batch.put(&key_bytes, &encoded);
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        self.write_batch_if_not_empty(&batch);

        Ok(next)
    }

    pub fn increment_integer_string(&self, key: &str, delta: i64) -> Result<i64, Error> {
        if self.store.is_transactional() {
            return self.update_integer_string_read_modify_write(key, |current| {
                current.checked_add(delta)
            });
        }

        let key_bytes = self.mk(key);
        let now = now_ms();

        let cached_next = match self.counter_cache.entry(key_bytes.clone()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                if entry.expire_ms == 0 || entry.expire_ms > now {
                    let next = entry
                        .value
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
                    entry.value = next;
                    Some(next)
                } else {
                    occupied.remove();
                    None
                }
            }
            Entry::Vacant(_) => None,
        };
        if let Some(next) = cached_next {
            self.store.merge_raw(&key_bytes, &delta.to_be_bytes());
            self.changes.fetch_add(1, Ordering::Relaxed);
            return Ok(next);
        }

        self.expire_if_needed(key);
        let now = now_ms();
        let next = loop {
            match self.counter_cache.entry(key_bytes.clone()) {
                Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    if entry.expire_ms > 0 && entry.expire_ms <= now {
                        occupied.remove();
                        continue;
                    }
                    let next = entry
                        .value
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
                    entry.value = next;
                    break next;
                }
                Entry::Vacant(vacant) => {
                    let cache_epoch = self.counter_cache_epoch.load(Ordering::Acquire);
                    let (expire_ms, current) = self.read_integer_string_for_update(&key_bytes)?;
                    let next = current
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;

                    if self.counter_cache_epoch.load(Ordering::Acquire) == cache_epoch {
                        vacant.insert(CounterCacheEntry {
                            value: next,
                            expire_ms,
                        });
                    }
                    break next;
                }
            }
        };

        self.store.merge_raw(&key_bytes, &delta.to_be_bytes());
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(next)
    }

    pub async fn increment_integer_string_async(
        &self,
        key: &str,
        delta: i64,
    ) -> Result<i64, Error> {
        if self.store.is_transactional() {
            return self.update_integer_string_read_modify_write(key, |current| {
                current.checked_add(delta)
            });
        }

        let key_bytes = self.mk(key);
        let now = now_ms();

        let cached_next = match self.counter_cache.entry(key_bytes.clone()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                if entry.expire_ms == 0 || entry.expire_ms > now {
                    let next = entry
                        .value
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
                    entry.value = next;
                    Some(next)
                } else {
                    occupied.remove();
                    None
                }
            }
            Entry::Vacant(_) => None,
        };
        if let Some(next) = cached_next {
            self.store
                .merge_raw_async(&key_bytes, &delta.to_be_bytes())
                .await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            return Ok(next);
        }

        self.expire_if_needed_async(key).await;
        let now = now_ms();
        let next = loop {
            match self.counter_cache.entry(key_bytes.clone()) {
                Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    if entry.expire_ms > 0 && entry.expire_ms <= now {
                        occupied.remove();
                        continue;
                    }
                    let next = entry
                        .value
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
                    entry.value = next;
                    break next;
                }
                Entry::Vacant(vacant) => {
                    let cache_epoch = self.counter_cache_epoch.load(Ordering::Acquire);
                    let (expire_ms, current) = self.read_integer_string_for_update(&key_bytes)?;
                    let next = current
                        .checked_add(delta)
                        .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;

                    if self.counter_cache_epoch.load(Ordering::Acquire) == cache_epoch {
                        vacant.insert(CounterCacheEntry {
                            value: next,
                            expire_ms,
                        });
                    }
                    break next;
                }
            }
        };

        self.store
            .merge_raw_async(&key_bytes, &delta.to_be_bytes())
            .await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(next)
    }

    fn read_integer_string_for_update(&self, key_bytes: &[u8]) -> Result<(u64, i64), Error> {
        let Some(raw) = self.store.get_raw(key_bytes) else {
            return Ok((0, 0));
        };
        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        };
        if header.type_tag != TYPE_STRING {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let value = decode_string_bytes_slice(&raw)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|text| text.parse::<i64>().ok())
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        Ok((header.expire_ms, value))
    }

    pub fn get_string_entry_raw_bytes(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        let Some(raw) = self.read_live_raw_key_bytes(key) else {
            return Ok(None);
        };
        if decode_string_bytes_slice(&raw).is_some() {
            Ok(Some(raw))
        } else {
            Err(Error::msg("Type parsing error"))
        }
    }

    pub fn getex_string_bytes(
        &self,
        key: &str,
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw(key) else {
            return Ok(None);
        };
        let value = decode_string_bytes(&raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
        let Some(expiration) = expiration else {
            return Ok(Some(value));
        };

        match expiration {
            StringExpireUpdate::Persist => {
                self.persist(key);
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                self.expire(key.to_string(), ttl_ms);
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                if expire_ms <= now_ms() {
                    self.delete_key_internal(key, false);
                } else {
                    self.expire(key.to_string(), expire_ms - now_ms());
                }
            }
        }
        Ok(Some(value))
    }

    pub async fn getex_string_bytes_async(
        &self,
        key: &str,
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return Ok(None);
        };
        let value = decode_string_bytes(&raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
        let Some(expiration) = expiration else {
            return Ok(Some(value));
        };

        match expiration {
            StringExpireUpdate::Persist => {
                self.persist_async(key).await;
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                self.expire_async(key.to_string(), ttl_ms).await;
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                if expire_ms <= now_ms() {
                    self.delete_key_internal_async(key, false).await;
                } else {
                    self.expire_async(key.to_string(), expire_ms - now_ms())
                        .await;
                }
            }
        }
        Ok(Some(value))
    }

    pub fn type_name_readonly(&self, key: &str) -> &'static str {
        let Some(raw) = self.read_live_raw(key) else {
            return "none";
        };
        let Some(header) = decode_meta_header(&raw) else {
            return "none";
        };
        match header.type_tag {
            TYPE_STRING => "string",
            TYPE_HASH => "hash",
            TYPE_SET => "set",
            TYPE_SORTED_SET => "zset",
            TYPE_LIST => "list",
            TYPE_STREAM => "stream",
            TYPE_VECTOR => "vector",
            TYPE_JSON => "json",
            _ => "none",
        }
    }

    pub async fn type_name_readonly_async(&self, key: &str) -> &'static str {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return "none";
        };
        let Some(header) = decode_meta_header(&raw) else {
            return "none";
        };
        match header.type_tag {
            TYPE_STRING => "string",
            TYPE_HASH => "hash",
            TYPE_SET => "set",
            TYPE_SORTED_SET => "zset",
            TYPE_LIST => "list",
            TYPE_STREAM => "stream",
            TYPE_VECTOR => "vector",
            TYPE_JSON => "json",
            _ => "none",
        }
    }

    pub fn exists_readonly(&self, key: &str) -> bool {
        self.read_live_raw(key).is_some()
    }

    pub async fn exists_readonly_async(&self, key: &str) -> bool {
        self.read_live_raw_async(key).await.is_some()
    }

    pub fn ttl_millis_readonly(&self, key: &str) -> i64 {
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return -1;
        }
        let now = now_ms();
        if now >= expire_ms {
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    pub async fn ttl_millis_readonly_async(&self, key: &str) -> i64 {
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return -1;
        }
        let now = now_ms();
        if now >= expire_ms {
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    pub fn expire_time_millis_readonly(&self, key: &str) -> i64 {
        let Some(raw) = self.read_live_raw(key) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 { -1 } else { expire_ms as i64 }
    }

    pub async fn expire_time_millis_readonly_async(&self, key: &str) -> i64 {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 { -1 } else { expire_ms as i64 }
    }

    fn read_live_raw(&self, key: &str) -> Option<Vec<u8>> {
        let raw = self.store.get_raw(&self.mk(key))?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    async fn read_live_raw_async(&self, key: &str) -> Option<Vec<u8>> {
        let raw = self.store.get_raw_async(&self.mk(key)).await?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    fn read_live_raw_key_bytes(&self, key: &[u8]) -> Option<Bytes> {
        let raw = self
            .store
            .get_raw_bytes(&main_key_bytes(self.db_index, key))?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    /**
     * 设置过期
     *
     * @param key 键名
     * @param ttl 距离现在多少【毫秒】后过期
     */
    pub fn expire(&self, key: String, ttl: u64) -> bool {
        self.expire_with_condition(key, ttl, ExpireCondition::Always)
    }

    pub async fn expire_async(&self, key: String, ttl: u64) -> bool {
        self.expire_with_condition_async(key, ttl, ExpireCondition::Always)
            .await
    }

    pub fn expire_with_condition(&self, key: String, ttl: u64, condition: ExpireCondition) -> bool {
        self.expire_if_needed(&key);
        let key_bytes = self.mk(&key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw) {
                if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                    return false;
                }
                if ttl == 0 {
                    return self.remove_internal(&key, false).is_some();
                }
                let expire_ms = now_ms() + ttl;
                if let Some(patched) = patch_meta_expire_ms(&raw, expire_ms) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager
                        .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
                    self.write_batch_if_not_empty(&batch);
                    return true;
                }
            }
        }
        false
    }

    pub async fn expire_with_condition_async(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> bool {
        self.expire_if_needed_async(&key).await;
        let key_bytes = self.mk(&key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw) {
                if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                    return false;
                }
                if ttl == 0 {
                    return self.remove_internal_async(&key, false).await.is_some();
                }
                let expire_ms = now_ms() + ttl;
                if let Some(patched) = patch_meta_expire_ms(&raw, expire_ms) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager
                        .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
                    self.write_batch_if_not_empty_async(&batch).await;
                    return true;
                }
            }
        }
        false
    }

    fn expire_condition_matches(
        current_expire_ms: u64,
        ttl: u64,
        condition: ExpireCondition,
    ) -> bool {
        match condition {
            ExpireCondition::Always => true,
            ExpireCondition::Nx => current_expire_ms == 0,
            ExpireCondition::Xx => current_expire_ms > 0,
            ExpireCondition::Gt => {
                current_expire_ms > 0 && now_ms().saturating_add(ttl) > current_expire_ms
            }
            ExpireCondition::Lt => {
                current_expire_ms == 0 || now_ms().saturating_add(ttl) < current_expire_ms
            }
        }
    }

    /**
     * 移除过期时间（PERSIST 命令）
     */
    pub fn persist(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 {
                if let Some(patched) = patch_meta_expire_ms(&raw, 0) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        expire_ms,
                        self.db_index,
                        key,
                    );
                    self.write_batch_if_not_empty(&batch);
                    return true;
                }
            }
        }
        false
    }

    pub async fn persist_async(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 {
                if let Some(patched) = patch_meta_expire_ms(&raw, 0) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        expire_ms,
                        self.db_index,
                        key,
                    );
                    self.write_batch_if_not_empty_async(&batch).await;
                    return true;
                }
            }
        }
        false
    }

    /**
     * 删除键值
     *
     * @param key 键名
     * @return 如果删除成功，返回被删除的值
     */
    pub fn remove(&self, key: &str) -> Option<Structure> {
        self.remove_internal(key, true)
    }

    pub async fn remove_async(&self, key: &str) -> Option<Structure> {
        self.remove_internal_async(key, true).await
    }

    pub fn delete_key(&self, key: &str) -> bool {
        self.delete_key_internal(key, true)
    }

    pub async fn delete_key_async(&self, key: &str) -> bool {
        self.delete_key_internal_async(key, true).await
    }

    pub fn touch(&self, key: &str) -> bool {
        self.read_live_raw(key).is_some()
    }

    pub async fn touch_async(&self, key: &str) -> bool {
        self.read_live_raw_async(key).await.is_some()
    }

    /**
     * 清理过期键
     */
    /**
     * 过期检测【惰性】
     */
    pub fn expire_if_needed(&self, key: &str) {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw) {
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    let mut batch = WriteBatch::new();
                    batch.delete(&key_bytes);
                    delete_sub_keys_to_batch(
                        &mut batch,
                        self.db_index,
                        key,
                        header.version,
                        header.type_tag,
                    );
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        key,
                    );
                    match header.type_tag {
                        TYPE_HASH => {
                            if let Err(err) =
                                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)
                            {
                                log::error!(
                                    "failed to enqueue fulltext delete for expired {key}: {err}"
                                );
                                return;
                            }
                        }
                        TYPE_JSON => {
                            if let Err(err) =
                                self.fulltext_enqueue_json_delete_to_batch(&mut batch, key)
                            {
                                log::error!(
                                    "failed to enqueue fulltext JSON delete for expired {key}: {err}"
                                );
                                return;
                            }
                        }
                        _ => {}
                    }
                    self.write_batch_if_not_empty(&batch);
                    let refresh = match header.type_tag {
                        TYPE_HASH => self.fulltext_request_refresh(key),
                        TYPE_JSON => self.fulltext_request_json_refresh(key),
                        _ => Ok(()),
                    };
                    if let Err(err) = refresh {
                        log::error!("failed to refresh fulltext expire for {key}: {err}");
                    }
                }
            }
        }
    }

    pub async fn expire_if_needed_async(&self, key: &str) {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw_async(&key_bytes).await {
            if let Some(header) = decode_meta_header(&raw) {
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    let mut batch = WriteBatch::new();
                    batch.delete(&key_bytes);
                    delete_sub_keys_to_batch(
                        &mut batch,
                        self.db_index,
                        key,
                        header.version,
                        header.type_tag,
                    );
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        key,
                    );
                    match header.type_tag {
                        TYPE_HASH => {
                            if let Err(err) =
                                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)
                            {
                                log::error!(
                                    "failed to enqueue fulltext delete for expired {key}: {err}"
                                );
                                return;
                            }
                        }
                        TYPE_JSON => {
                            if let Err(err) =
                                self.fulltext_enqueue_json_delete_to_batch(&mut batch, key)
                            {
                                log::error!(
                                    "failed to enqueue fulltext JSON delete for expired {key}: {err}"
                                );
                                return;
                            }
                        }
                        _ => {}
                    }
                    self.write_batch_if_not_empty_async(&batch).await;
                    let refresh = match header.type_tag {
                        TYPE_HASH => self.fulltext_request_refresh(key),
                        TYPE_JSON => self.fulltext_request_json_refresh(key),
                        _ => Ok(()),
                    };
                    if let Err(err) = refresh {
                        log::error!("failed to refresh fulltext expire for {key}: {err}");
                    }
                }
            }
        }
    }

    /**
     * 获取过期毫秒数
     */
    pub fn ttl_millis(&self, key: &str) -> i64 {
        let key_bytes = self.mk(key);
        if !self.store.contains_key(&key_bytes) {
            return -2;
        }
        let expire_ms = self.get_expire_ms(key);
        if expire_ms == 0 {
            return -1; // 无过期
        }
        let now = now_ms();
        if now >= expire_ms {
            self.remove_internal(key, false);
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    /**
     * 检查键是否存在
     */
    pub fn exists(&self, key: &str) -> bool {
        self.expire_if_needed(key);
        self.store.contains_key(&self.mk(key))
    }

}
