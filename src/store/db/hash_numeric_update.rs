use super::*;

const HASH_FIELD_CAS_SHARED_ATTEMPTS: usize = 3;

enum HashFieldCasAttempt<T> {
    Applied(T),
    Conflict,
}

struct HashFieldCasState {
    key_bytes: Vec<u8>,
    raw_meta: Option<Vec<u8>>,
    expired_at: Option<u64>,
    meta_exists: bool,
    version: u64,
    field_key: Vec<u8>,
    field_raw: Option<Vec<u8>>,
    expire_key: Vec<u8>,
    expire_raw: Option<Vec<u8>>,
    live: bool,
    may_have_field_ttl: bool,
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
        for _ in 0..HASH_FIELD_CAS_SHARED_ATTEMPTS {
            let structural_guard = self.set_write_lock(key).read().await;
            match self
                .hash_set_nx_bytes_async_attempt(key, field, value)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => {}
            }
            drop(structural_guard);
            tokio::task::yield_now().await;
        }

        let _structural_guard = self.set_write_lock(key).lock().await;
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

        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }
        batch.put(
            &hash_field_key(self.db_index, key, version, field),
            next.to_string().as_bytes(),
        );
        if current_value.is_none() {
            batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
        for _ in 0..HASH_FIELD_CAS_SHARED_ATTEMPTS {
            let structural_guard = self.set_write_lock(key).read().await;
            match self
                .hash_increment_by_async_attempt(key, field, increment)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => {}
            }
            drop(structural_guard);
            tokio::task::yield_now().await;
        }

        let _structural_guard = self.set_write_lock(key).lock().await;
        loop {
            match self
                .hash_increment_by_async_attempt(key, field, increment)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
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
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }
        batch.put(
            &hash_field_key(self.db_index, key, version, field),
            formatted.as_bytes(),
        );
        if current_value.is_none() {
            batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
        for _ in 0..HASH_FIELD_CAS_SHARED_ATTEMPTS {
            let structural_guard = self.set_write_lock(key).read().await;
            match self
                .hash_increment_by_float_async_attempt(key, field, increment)
                .await?
            {
                HashFieldCasAttempt::Applied(result) => return Ok(result),
                HashFieldCasAttempt::Conflict => {}
            }
            drop(structural_guard);
            tokio::task::yield_now().await;
        }

        let _structural_guard = self.set_write_lock(key).lock().await;
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
        let (meta_exists, version, may_have_field_ttl) = match raw_meta.as_deref() {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode hash metadata"))?;
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    expired_at = Some(header.expire_ms);
                    (false, self.next_version_async().await, false)
                } else {
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let meta = decode_hash_meta_checked(raw)?;
                    (true, meta.version, meta.may_have_field_ttl)
                }
            }
            None => (false, self.next_version_async().await, false),
        };

        let field_key = hash_field_key(self.db_index, key, version, field);
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let (field_raw, expire_raw) = if meta_exists {
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
            meta_exists,
            version,
            field_key,
            field_raw,
            expire_key,
            expire_raw,
            live,
            may_have_field_ttl,
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
        if !state.meta_exists {
            batch.put(&state.key_bytes, &encode_hash_meta(0, state.version));
        }
    }

    fn hash_field_cas_conditions(&self, state: &HashFieldCasState) -> Vec<CompareCondition> {
        let mut conditions = Vec::with_capacity(3);
        if !state.meta_exists {
            conditions.push(CompareCondition::with_expected(
                &state.key_bytes,
                state.raw_meta.clone(),
            ));
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
        batch.put(&state.field_key, value);
        if state.expire_raw.is_some() {
            batch.delete(&state.expire_key);
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
        batch.put(&state.field_key, next.to_string().as_bytes());
        if !state.live && state.expire_raw.is_some() {
            batch.delete(&state.expire_key);
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
        batch.put(&state.field_key, formatted.as_bytes());
        if !state.live && state.expire_raw.is_some() {
            batch.delete(&state.expire_key);
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
