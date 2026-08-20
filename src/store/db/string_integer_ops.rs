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
            self.expire_if_needed_async(key).await?;
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let (expire_ms, current) =
                Self::decode_integer_string_for_update(observed.value().map(|raw| raw.as_ref()))?;
            let next = update(current)
                .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
            let encoded = encode_raw_string(next.to_string().as_bytes(), expire_ms);
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &encoded)).expect("write batch append invariant violated");
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
        self.expire_if_needed(key)?;

        let key_bytes = self.mk(key);
        let (expire_ms, current) = self.read_integer_string_for_update(&key_bytes)?;

        let next = update(current)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        self.changes.fetch_add(1, Ordering::Relaxed);

        let encoded = encode_raw_string(next.to_string().as_bytes(), expire_ms);
        let mut batch = WriteBatch::new();
        (batch.put(&key_bytes, &encoded)).expect("write batch append invariant violated");
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        self.write_batch_if_not_empty(&batch)?;

        Ok(next)
    }

    pub fn increment_integer_string(&self, key: &str, delta: i64) -> Result<i64, Error> {
        // The server hot path is async. Keep the synchronous API on the strict read-modify-write
        // implementation so it remains usable from both ordinary and current-thread runtimes.
        self.update_integer_string_read_modify_write(key, |current| current.checked_add(delta))
    }

    pub async fn increment_integer_string_async(
        &self,
        key: &str,
        delta: i64,
    ) -> Result<i64, Error> {
        // Arbitrary INCRBY operands can be individually valid relative to a negative base while
        // overflowing if kv_engine partial-merges the operands without that base. Keep those on
        // the strict path; the hot INCR/DECR cases cannot reach that condition in practice.
        if self.store.is_transactional() || !matches!(delta, -1 | 1) {
            return self
                .update_integer_string_async(key, |current| current.checked_add(delta))
                .await;
        }

        let logical_key = key.as_bytes().to_vec();
        let cache_key = (self.db_index, logical_key.clone());
        let raw_key = self.mk(key);

        let read_guard = self.set_write_lock(key).read_owned().await;
        if let Some((next, sequence, commit_state)) =
            self.assign_cached_counter(&cache_key, delta)?
        {
            self.spawn_counter_merge(
                logical_key,
                raw_key,
                delta,
                sequence,
                commit_state.clone(),
                read_guard,
            );
            commit_state.wait_for(sequence).await?;
            return Ok(next);
        }
        drop(read_guard);

        // Cache initialization is a structural operation: wait for any evicted in-flight merge,
        // then read one authoritative base value while SET/DEL/expiry are excluded.
        let write_guard = self.set_write_lock(key).lock_owned().await;
        if let Some((next, sequence, commit_state)) =
            self.assign_cached_counter(&cache_key, delta)?
        {
            self.spawn_counter_merge(
                logical_key,
                raw_key,
                delta,
                sequence,
                commit_state.clone(),
                write_guard,
            );
            commit_state.wait_for(sequence).await?;
            return Ok(next);
        }

        self.expire_if_needed_async(key).await?;
        let (expire_ms, current) = self.read_integer_string_for_update_async(&raw_key).await?;
        if expire_ms > 0 {
            // TTL-bearing counters stay on the compare-and-write path. An expiry delete and a
            // blind merge must never race, because the merge could recreate an expired key.
            let result = self
                .update_integer_string_async(key, |value| value.checked_add(delta))
                .await;
            drop(write_guard);
            return result;
        }
        let next = current
            .checked_add(delta)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        let commit_state = Arc::new(CounterCommitState::default());
        self.counter_cache.evict_if_full();
        self.counter_cache
            .ever_populated
            .store(true, Ordering::Release);
        self.counter_cache.entries.insert(
            cache_key,
            CounterCacheEntry {
                value: next,
                next_sequence: 1,
                commit_state: commit_state.clone(),
            },
        );
        self.spawn_counter_merge(
            logical_key,
            raw_key,
            delta,
            1,
            commit_state.clone(),
            write_guard,
        );
        commit_state.wait_for(1).await?;
        Ok(next)
    }

    fn assign_cached_counter(
        &self,
        cache_key: &(u16, Vec<u8>),
        delta: i64,
    ) -> Result<Option<(i64, u64, Arc<CounterCommitState>)>, Error> {
        let Some(mut entry) = self.counter_cache.entries.get_mut(cache_key) else {
            return Ok(None);
        };
        if let Some(error) = entry.commit_state.failure() {
            return Err(Error::msg(error));
        }
        let next = entry
            .value
            .checked_add(delta)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        let sequence = entry
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::msg("ERR counter merge sequence exhausted"))?;
        entry.value = next;
        entry.next_sequence = sequence;
        Ok(Some((next, sequence, entry.commit_state.clone())))
    }

    fn spawn_counter_merge<G>(
        &self,
        logical_key: Vec<u8>,
        raw_key: Vec<u8>,
        delta: i64,
        sequence: u64,
        commit_state: Arc<CounterCommitState>,
        guard: G,
    ) where
        G: Send + 'static,
    {
        let db = self.shared_task_view();
        tokio::spawn(async move {
            // The owned key guard moves into this task so disconnecting/cancelling the request
            // cannot let a structural overwrite pass an already assigned counter delta.
            let _guard = guard;
            match db
                .store
                .merge_raw_async(&raw_key, &delta.to_be_bytes())
                .await
            {
                Ok(()) => {
                    db.changes.fetch_add(1, Ordering::Relaxed);
                    if db.mutation_tracker.has_observers() {
                        db.record_external_key_mutation(db.db_index, raw_key);
                    }
                    commit_state.complete(sequence);
                }
                Err(error) => {
                    db.counter_cache.invalidate_key(db.db_index, &logical_key);
                    commit_state.fail(format!("ERR counter merge failed: {error}"));
                }
            }
        });
    }

    async fn read_integer_string_for_update_async(
        &self,
        key_bytes: &[u8],
    ) -> Result<(u64, i64), Error> {
        let raw = self.store.get_raw_async(key_bytes).await?;
        Self::decode_integer_string_for_update(raw.as_deref())
    }

    pub(in crate::store::db) fn read_integer_string_for_update(
        &self,
        key_bytes: &[u8],
    ) -> Result<(u64, i64), Error> {
        let raw = self.store.get_raw(key_bytes)?;
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
