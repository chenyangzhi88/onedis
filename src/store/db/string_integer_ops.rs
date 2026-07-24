use super::*;

impl Db {
    pub fn update_integer_string<F>(&self, key: &str, update: F) -> Result<i64, Error>
    where
        F: FnOnce(i64) -> Option<i64>,
    {
        self.update_integer_string_read_modify_write(key, update)
    }

    pub async fn update_integer_string_async<F>(&self, key: &str, update: F) -> Result<i64, Error>
    where
        F: Fn(i64) -> Option<i64>,
    {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let (expire_ms, current) =
                Self::decode_integer_string_for_update(observed.value().map(|raw| raw.as_ref()))?;
            let next = update(current)
                .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
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
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(next);
            }
        }
        Err(Error::msg("ERR integer write conflict"))
    }

    pub(in crate::store::db) fn update_integer_string_read_modify_write<F>(
        &self,
        key: &str,
        update: F,
    ) -> Result<i64, Error>
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
                        self.counter_cache_maybe_non_empty
                            .store(true, Ordering::Release);
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
        self.update_integer_string_async(key, |current| current.checked_add(delta))
            .await
    }

    pub(in crate::store::db) fn read_integer_string_for_update(
        &self,
        key_bytes: &[u8],
    ) -> Result<(u64, i64), Error> {
        let raw = self.store.get_raw(key_bytes);
        Self::decode_integer_string_for_update(raw.as_deref())
    }

    fn decode_integer_string_for_update(raw: Option<&[u8]>) -> Result<(u64, i64), Error> {
        let Some(raw) = raw else {
            return Ok((0, 0));
        };
        let Some(header) = decode_meta_header(raw) else {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        };
        if header.type_tag != TYPE_STRING {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let value = decode_string_bytes_slice(raw)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|text| text.parse::<i64>().ok())
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        Ok((header.expire_ms, value))
    }
}
