use super::*;

pub(in crate::store::db) fn hash_counter_route_key(key: &str, field: &str) -> Vec<u8> {
    let mut route = Vec::with_capacity(8 + key.len() + field.len());
    route.extend_from_slice(&(key.len() as u64).to_be_bytes());
    route.extend_from_slice(key.as_bytes());
    route.extend_from_slice(field.as_bytes());
    route
}

enum HashFieldCasAttempt<T> {
    Applied(T),
    Conflict,
}

struct HashFieldCasState {
    key_bytes: Vec<u8>,
    raw_meta: Option<Vec<u8>>,
    expired_at: Option<u64>,
    key_expire_ms: u64,
    meta_exists: bool,
    version: u64,
    field_key: Vec<u8>,
    field_raw: Option<Vec<u8>>,
    expire_key: Vec<u8>,
    expire_raw: Option<Vec<u8>>,
    live: bool,
    may_have_field_ttl: bool,
    packed_fields: Option<PackedHashFields>,
}

impl Db {
    pub fn hash_set_nx(&self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        self.hash_set_nx_bytes(key, field, value.as_bytes())
    }

    pub fn hash_set_nx_bytes(&self, key: &str, field: &str, value: &[u8]) -> Result<bool, Error> {
        if self.hash_exists(key, field)? {
            return Ok(false);
        }
        self.hash_set_bytes(key, field, value)
    }

    pub async fn hash_set_nx_async(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<bool, Error> {
        self.hash_set_nx_bytes_async(key, field, value.as_bytes())
            .await
    }

    pub async fn hash_set_nx_bytes_async(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<bool, Error> {
        let _structural_guard = self.set_write_lock(key).read().await;
        let _field_guard = self.hash_field_write_lock(key, field).lock().await;
        loop {
            match self
                .hash_set_nx_bytes_async_attempt(key, field, value)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
    }

    /// 按整数增量更新 hash field，返回更新后的值。
    pub fn hash_increment_by(&self, key: &str, field: &str, increment: i64) -> Result<i64, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, version)) => version,
            None => self.next_version(),
        };
        let current_value = match meta {
            Some(_) => self.hash_live_field_value(key, version, field),
            None => None,
        };
        let current = match current_value.as_deref() {
            Some(value) => std::str::from_utf8(value)
                .map_err(|_| Error::msg("ERR hash value is not an integer"))?
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR hash value is not an integer"))?,
            None => 0,
        };
        let next = current
            .checked_add(increment)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;

        if meta.is_none() || meta.is_some_and(|(_, version)| version == 0) {
            self.hash_set_bytes(key, field, next.to_string().as_bytes())?;
            return Ok(next);
        }

        let mut batch = WriteBatch::new();
        if meta.is_none() {
            (batch.put(&self.mk(key), &encode_hash_meta(0, version)))
                .expect("write batch append invariant violated");
        }
        (batch.put(
            &hash_field_key(self.db_index, key, version, field),
            next.to_string().as_bytes(),
        ))
        .expect("write batch append invariant violated");
        if current_value.is_none() {
            (batch.delete(&hash_field_expire_key(self.db_index, key, version, field)))
                .expect("write batch append invariant violated");
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(next)
    }

    pub async fn hash_increment_by_async(
        &self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<i64, Error> {
        if !self.store.is_transactional()
            && matches!(increment, -1 | 1)
            && !self.fulltext_hash_source_is_indexed(key)?
        {
            if let Some(result) = self
                .hash_increment_by_cached_async(key, field, increment)
                .await?
            {
                return Ok(result);
            }
        }
        let _structural_guard = self.set_write_lock(key).read().await;
        let _field_guard = self.hash_field_write_lock(key, field).lock().await;
        loop {
            match self
                .hash_increment_by_async_attempt(key, field, increment)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                // An HSET may still update this field while holding the shared structural
                // barrier. Keep the field lock and re-observe; competing increments cannot
                // now create a retry storm.
                HashFieldCasAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
    }

    async fn hash_increment_by_cached_async(
        &self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<Option<i64>, Error> {
        let logical_key = key.as_bytes().to_vec();
        let route_key = hash_counter_route_key(key, field);
        let key_epoch = self
            .counter_cache
            .hash_key_epoch(self.db_index, &logical_key);

        let structural_guard = self.set_write_lock(key).read_owned().await;
        let field_guard = self.hash_field_write_lock(key, field).read_owned().await;
        if let Some(raw_key) = self
            .counter_cache
            .hash_routes
            .get(&(self.db_index, route_key.clone()))
            .map(|route| route.clone())
            && let Some((next, sequence, commit_state)) =
                self.assign_cached_hash_counter(&raw_key, key_epoch, increment)?
        {
            self.spawn_hash_counter_merge(
                logical_key,
                raw_key,
                increment,
                sequence,
                commit_state.clone(),
                (structural_guard, field_guard),
            );
            commit_state.wait_for(sequence).await?;
            return Ok(Some(next));
        }
        drop(field_guard);
        drop(structural_guard);

        let structural_guard = self.set_write_lock(key).read_owned().await;
        let field_guard = self.hash_field_write_lock(key, field).lock_owned().await;
        let key_epoch = self
            .counter_cache
            .hash_key_epoch(self.db_index, &logical_key);
        if let Some(raw_key) = self
            .counter_cache
            .hash_routes
            .get(&(self.db_index, route_key.clone()))
            .map(|route| route.clone())
            && let Some((next, sequence, commit_state)) =
                self.assign_cached_hash_counter(&raw_key, key_epoch, increment)?
        {
            self.spawn_hash_counter_merge(
                logical_key,
                raw_key,
                increment,
                sequence,
                commit_state.clone(),
                (structural_guard, field_guard),
            );
            commit_state.wait_for(sequence).await?;
            return Ok(Some(next));
        }

        let state = self.observe_hash_field_cas_state(key, field).await?;
        if state.packed_fields.is_some() {
            return Ok(None);
        }
        if !state.meta_exists
            || state.expired_at.is_some()
            || state.key_expire_ms > 0
            || state.may_have_field_ttl
            || !state.live
        {
            return Ok(None);
        }
        let current = state
            .field_raw
            .as_deref()
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| Error::msg("ERR hash value is not an integer"))?;
        let next = current
            .checked_add(increment)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        let raw_key = state.field_key;
        let commit_state = Arc::new(CounterCommitState::default());
        self.counter_cache.evict_hash_if_full();
        self.counter_cache
            .hash_ever_populated
            .store(true, Ordering::Release);
        self.counter_cache.hash_entries.insert(
            (self.db_index, raw_key.clone()),
            HashCounterCacheEntry {
                value: next,
                next_sequence: 1,
                key_epoch,
                commit_state: commit_state.clone(),
            },
        );
        self.counter_cache
            .hash_routes
            .insert((self.db_index, route_key), raw_key.clone());
        self.spawn_hash_counter_merge(
            logical_key,
            raw_key,
            increment,
            1,
            commit_state.clone(),
            (structural_guard, field_guard),
        );
        commit_state.wait_for(1).await?;
        Ok(Some(next))
    }

    fn assign_cached_hash_counter(
        &self,
        raw_key: &[u8],
        key_epoch: u64,
        increment: i64,
    ) -> Result<Option<(i64, u64, Arc<CounterCommitState>)>, Error> {
        let cache_key = (self.db_index, raw_key.to_vec());
        let Some(mut entry) = self.counter_cache.hash_entries.get_mut(&cache_key) else {
            return Ok(None);
        };
        if entry.key_epoch != key_epoch {
            drop(entry);
            self.counter_cache.hash_entries.remove(&cache_key);
            return Ok(None);
        }
        if let Some(error) = entry.commit_state.failure() {
            return Err(Error::msg(error));
        }
        let next = entry
            .value
            .checked_add(increment)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        let sequence = entry
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::msg("ERR counter merge sequence exhausted"))?;
        entry.value = next;
        entry.next_sequence = sequence;
        Ok(Some((next, sequence, entry.commit_state.clone())))
    }

    fn spawn_hash_counter_merge<G>(
        &self,
        logical_key: Vec<u8>,
        raw_key: Vec<u8>,
        increment: i64,
        sequence: u64,
        commit_state: Arc<CounterCommitState>,
        guards: G,
    ) where
        G: Send + 'static,
    {
        let db = self.shared_task_view();
        tokio::spawn(async move {
            let _guards = guards;
            match db
                .store
                .try_merge_raw_async(&raw_key, &increment.to_be_bytes())
                .await
            {
                Ok(()) => {
                    db.changes.fetch_add(1, Ordering::Relaxed);
                    if db.mutation_tracker.has_watched_keys() {
                        db.record_external_key_mutation(db.db_index, logical_key);
                    }
                    commit_state.complete(sequence);
                }
                Err(error) => {
                    db.counter_cache
                        .invalidate_hash_field(db.db_index, &raw_key);
                    commit_state.fail(format!("ERR hash counter merge failed: {error}"));
                }
            }
        });
    }

    pub fn hash_increment_by_float(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<String, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, version)) => version,
            None => self.next_version(),
        };
        let current_value = match meta {
            Some(_) => self.hash_live_field_value(key, version, field),
            None => None,
        };
        let current = match current_value.as_deref() {
            Some(value) => {
                let parsed = std::str::from_utf8(value)
                    .map_err(|_| Error::msg("ERR hash value is not a float"))?
                    .parse::<f64>()
                    .map_err(|_| Error::msg("ERR hash value is not a float"))?;
                if !parsed.is_finite() {
                    return Err(Error::msg("ERR hash value is not a float"));
                }
                parsed
            }
            None => 0.0,
        };
        let next = current + increment;
        if !next.is_finite() {
            return Err(Error::msg("ERR increment would produce NaN or Infinity"));
        }
        let formatted = crate::cmds::string::incrbyfloat::IncrbyFloat::format_float(next);
        if meta.is_none() || meta.is_some_and(|(_, version)| version == 0) {
            self.hash_set_bytes(key, field, formatted.as_bytes())?;
            return Ok(formatted);
        }
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            (batch.put(&self.mk(key), &encode_hash_meta(0, version)))
                .expect("write batch append invariant violated");
        }
        (batch.put(
            &hash_field_key(self.db_index, key, version, field),
            formatted.as_bytes(),
        ))
        .expect("write batch append invariant violated");
        if current_value.is_none() {
            (batch.delete(&hash_field_expire_key(self.db_index, key, version, field)))
                .expect("write batch append invariant violated");
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(formatted)
    }

    pub async fn hash_increment_by_float_async(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<String, Error> {
        let _structural_guard = self.set_write_lock(key).read().await;
        let _field_guard = self.hash_field_write_lock(key, field).lock().await;
        loop {
            match self
                .hash_increment_by_float_async_attempt(key, field, increment)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
    }

    async fn observe_hash_field_cas_state(
        &self,
        key: &str,
        field: &str,
    ) -> Result<HashFieldCasState, Error> {
        let key_bytes = self.mk(key);
        let raw_meta = self.store.get_raw_async(&key_bytes).await;
        let mut expired_at = None;
        let mut key_expire_ms = 0;
        let mut packed_fields = None;
        let (meta_exists, version, may_have_field_ttl) = match raw_meta.as_deref() {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode hash metadata"))?;
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    expired_at = Some(header.expire_ms);
                    (false, self.next_version_async().await, false)
                } else {
                    key_expire_ms = header.expire_ms;
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let meta = decode_hash_meta_checked(raw)?;
                    if meta.packed {
                        packed_fields = Some(
                            decode_packed_hash(raw)
                                .ok_or_else(|| Error::msg("Failed to decode packed hash"))?,
                        );
                    }
                    (true, meta.version, meta.may_have_field_ttl)
                }
            }
            None => (false, self.next_version_async().await, false),
        };

        let field_key = hash_field_key(self.db_index, key, version, field);
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let (field_raw, expire_raw) = if let Some(fields) = &packed_fields {
            (fields.get(field).cloned(), None)
        } else if meta_exists {
            let keys = if may_have_field_ttl {
                vec![field_key.clone(), expire_key.clone()]
            } else {
                vec![field_key.clone()]
            };
            let mut values = self.store.multi_get_raw_async(&keys).await.into_iter();
            let field_raw = values.next().flatten();
            let expire_raw = values.next().flatten();
            (field_raw, expire_raw)
        } else {
            (None, None)
        };
        let expire_ms = expire_raw.as_deref().and_then(decode_u64_be).unwrap_or(0);
        let live = field_raw.is_some() && (expire_ms == 0 || now_ms() < expire_ms);

        Ok(HashFieldCasState {
            key_bytes,
            raw_meta,
            expired_at,
            key_expire_ms,
            meta_exists,
            version,
            field_key,
            field_raw,
            expire_key,
            expire_raw,
            live,
            may_have_field_ttl,
            packed_fields,
        })
    }

    fn prepare_hash_field_cas_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        state: &HashFieldCasState,
    ) {
        if let Some(expire_ms) = state.expired_at {
            self.ttl_manager
                .remove_known_to_batch(batch, expire_ms, self.db_index, key);
        }
    }

    fn hash_field_cas_conditions(&self, state: &HashFieldCasState) -> Vec<CompareCondition> {
        let mut conditions = Vec::with_capacity(3);
        if !state.meta_exists || state.packed_fields.is_some() {
            conditions.push(CompareCondition::with_expected(
                &state.key_bytes,
                state.raw_meta.clone(),
            ));
            return conditions;
        }
        conditions.push(CompareCondition::with_expected(
            &state.field_key,
            state.field_raw.clone(),
        ));
        if state.may_have_field_ttl {
            conditions.push(CompareCondition::with_expected(
                &state.expire_key,
                state.expire_raw.clone(),
            ));
        }
        conditions
    }

    async fn stage_hash_field_cas_value(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        field: &str,
        value: &[u8],
        state: &HashFieldCasState,
    ) {
        if state.packed_fields.is_none() && state.meta_exists {
            (batch.put(&state.field_key, value)).expect("write batch append invariant violated");
            return;
        }
        let mut fields = state.packed_fields.clone().unwrap_or_default();
        fields.insert(field.to_string(), value.to_vec());
        if let Some(raw) = encode_packed_hash(state.key_expire_ms, &fields) {
            (batch.put(&state.key_bytes, &raw)).expect("write batch append invariant violated");
            return;
        }
        let version = if state.version == 0 {
            self.next_version_async().await
        } else {
            state.version
        };
        (batch.put(
            &state.key_bytes,
            &encode_hash_meta(state.key_expire_ms, version),
        ))
        .expect("write batch append invariant violated");
        for (field, value) in fields {
            (batch.put(&hash_field_key(self.db_index, key, version, &field), &value))
                .expect("write batch append invariant violated");
        }
    }

    async fn hash_set_nx_bytes_async_attempt(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<HashFieldCasAttempt<bool>, Error> {
        let state = self.observe_hash_field_cas_state(key, field).await?;
        if state.live {
            return Ok(HashFieldCasAttempt::Applied(false));
        }

        let mut batch = WriteBatch::new();
        self.prepare_hash_field_cas_batch(&mut batch, key, &state);
        self.stage_hash_field_cas_value(&mut batch, key, field, value, &state)
            .await;
        if state.expire_raw.is_some() {
            (batch.delete(&state.expire_key)).expect("write batch append invariant violated");
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        if !self
            .compare_and_write_batch_if_not_empty_async(
                &self.hash_field_cas_conditions(&state),
                &batch,
            )
            .await?
        {
            return Ok(HashFieldCasAttempt::Conflict);
        }
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(HashFieldCasAttempt::Applied(true))
    }

    async fn hash_increment_by_async_attempt(
        &self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<HashFieldCasAttempt<i64>, Error> {
        let state = self.observe_hash_field_cas_state(key, field).await?;
        let current = match state.live.then_some(state.field_raw.as_deref()).flatten() {
            Some(value) => std::str::from_utf8(value)
                .map_err(|_| Error::msg("ERR hash value is not an integer"))?
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR hash value is not an integer"))?,
            None => 0,
        };
        let next = current
            .checked_add(increment)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;

        let mut batch = WriteBatch::new();
        self.prepare_hash_field_cas_batch(&mut batch, key, &state);
        self.stage_hash_field_cas_value(
            &mut batch,
            key,
            field,
            next.to_string().as_bytes(),
            &state,
        )
        .await;
        if !state.live && state.expire_raw.is_some() {
            (batch.delete(&state.expire_key)).expect("write batch append invariant violated");
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        if !self
            .compare_and_write_batch_if_not_empty_async(
                &self.hash_field_cas_conditions(&state),
                &batch,
            )
            .await?
        {
            return Ok(HashFieldCasAttempt::Conflict);
        }
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(HashFieldCasAttempt::Applied(next))
    }

    async fn hash_increment_by_float_async_attempt(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<HashFieldCasAttempt<String>, Error> {
        let state = self.observe_hash_field_cas_state(key, field).await?;
        let current = match state.live.then_some(state.field_raw.as_deref()).flatten() {
            Some(value) => {
                let parsed = std::str::from_utf8(value)
                    .map_err(|_| Error::msg("ERR hash value is not a float"))?
                    .parse::<f64>()
                    .map_err(|_| Error::msg("ERR hash value is not a float"))?;
                if !parsed.is_finite() {
                    return Err(Error::msg("ERR hash value is not a float"));
                }
                parsed
            }
            None => 0.0,
        };
        let next = current + increment;
        if !next.is_finite() {
            return Err(Error::msg("ERR increment would produce NaN or Infinity"));
        }
        let formatted = crate::cmds::string::incrbyfloat::IncrbyFloat::format_float(next);

        let mut batch = WriteBatch::new();
        self.prepare_hash_field_cas_batch(&mut batch, key, &state);
        self.stage_hash_field_cas_value(&mut batch, key, field, formatted.as_bytes(), &state)
            .await;
        if !state.live && state.expire_raw.is_some() {
            (batch.delete(&state.expire_key)).expect("write batch append invariant violated");
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        if !self
            .compare_and_write_batch_if_not_empty_async(
                &self.hash_field_cas_conditions(&state),
                &batch,
            )
            .await?
        {
            return Ok(HashFieldCasAttempt::Conflict);
        }
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(HashFieldCasAttempt::Applied(formatted))
    }
}
