use super::*;

const HASH_SET_SHARED_CAS_ATTEMPTS: usize = 3;

enum OrderedHashSetAttempt {
    Applied(Vec<bool>),
    Conflict,
}

impl Db {
    pub fn hash_get(&self, key: &str, field: &str) -> Result<Option<String>, Error> {
        Ok(self
            .hash_get_bytes(key, field)?
            .and_then(|value| String::from_utf8(value).ok()))
    }

    pub fn hash_get_bytes(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        Ok(self.hash_live_field_value(key, version, field))
    }

    pub async fn hash_get_async(&self, key: &str, field: &str) -> Result<Option<String>, Error> {
        Ok(self
            .hash_get_bytes_async(key, field)
            .await?
            .and_then(|value| String::from_utf8(value).ok()))
    }

    pub async fn hash_get_bytes_async(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(None);
        };
        if meta.may_have_field_ttl
            && !self
                .hash_field_is_live_async(key, meta.version, field)
                .await
        {
            return Ok(None);
        }
        Ok(self
            .store
            .get_raw_async(&hash_field_key(self.db_index, key, meta.version, field))
            .await)
    }

    /// 设置 hash field，返回是否为新字段。
    pub fn hash_set(&self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        self.hash_set_bytes(key, field, value.as_bytes())
    }

    pub fn hash_set_bytes(&self, key: &str, field: &str, value: &[u8]) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_version(),
        };
        let field_key = hash_field_key(self.db_index, key, version, field);
        let is_new_field =
            meta.is_none() || self.hash_live_field_value(key, version, field).is_none();

        let mut batch = WriteBatch::new();
        if meta.is_none() {
            (batch.put(&self.mk(key), &encode_hash_meta(0, version)))
                .expect("write batch append invariant violated");
        }
        (batch.put(&field_key, value)).expect("write batch append invariant violated");
        if meta.is_some() {
            (batch.delete(&hash_field_expire_key(self.db_index, key, version, field)))
                .expect("write batch append invariant violated");
        }

        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(is_new_field)
    }

    pub async fn hash_set_async(&self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        self.hash_set_bytes_async(key, field, value.as_bytes())
            .await
    }

    pub async fn hash_set_bytes_async(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<bool, Error> {
        let fields = [(field, value)];
        let mut added = self.hash_set_ordered_bytes_async(key, &fields).await?;
        Ok(added.pop().unwrap_or(false))
    }

    pub fn hash_set_many(&self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
        let fields = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        self.hash_set_many_bytes(key, &fields)
    }

    pub fn hash_set_many_bytes(
        &self,
        key: &str,
        fields: &[(String, Vec<u8>)],
    ) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        if fields.is_empty() {
            return Ok(0);
        }
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_version(),
        };
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            (batch.put(&self.mk(key), &encode_hash_meta(0, version)))
                .expect("write batch append invariant violated");
        }

        let mut added = 0usize;
        let mut seen_in_batch = HashSet::new();
        for (field, value) in fields {
            if !seen_in_batch.insert(field.clone()) {
                (batch.put(&hash_field_key(self.db_index, key, version, field), value))
                    .expect("write batch append invariant violated");
                continue;
            }
            let field_key = hash_field_key(self.db_index, key, version, field);
            if meta.is_none() || self.hash_live_field_value(key, version, field).is_none() {
                added += 1;
            }
            (batch.put(&field_key, value)).expect("write batch append invariant violated");
            if meta.is_some() {
                (batch.delete(&hash_field_expire_key(self.db_index, key, version, field)))
                    .expect("write batch append invariant violated");
            }
        }

        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(added)
    }

    pub async fn hash_set_many_async(
        &self,
        key: &str,
        fields: &[(String, String)],
    ) -> Result<usize, Error> {
        let fields = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        self.hash_set_many_bytes_async(key, &fields).await
    }

    pub async fn hash_set_many_bytes_async(
        &self,
        key: &str,
        fields: &[(String, Vec<u8>)],
    ) -> Result<usize, Error> {
        if fields.is_empty() {
            self.hash_expire_ms_async(key).await?;
            return Ok(0);
        }
        let fields = fields
            .iter()
            .map(|(field, value)| (field.as_str(), value.as_slice()))
            .collect::<Vec<_>>();
        Ok(self
            .hash_set_ordered_bytes_async(key, &fields)
            .await?
            .into_iter()
            .filter(|added| *added)
            .count())
    }

    /// Applies ordered HSET operations to one hash and reports the Redis integer result for each
    /// field/value pair. A shared structural guard allows field-local writes under one hash to run
    /// concurrently while excluding RENAME/COPY/MOVE, type replacement, and TTL sweeping.
    /// Existing live fields are blind last-write-wins updates. Only absent or expired fields use
    /// CAS because their observed state determines the Redis added-field result.
    pub async fn hash_set_ordered_bytes_async(
        &self,
        key: &str,
        fields: &[(&str, &[u8])],
    ) -> Result<Vec<bool>, Error> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }

        let field_shards = unique_hash_field_write_lock_shards(
            self.db_index,
            key,
            fields.iter().map(|(field, _)| *field),
        );
        let _structural_guard = self.set_write_lock(key).read().await;
        // Large batches of mostly-new fields have a wide CAS conflict surface: one collision
        // would retry the entire batch. Lock their field shards up front. Small hot updates keep
        // the shared path so existing fields remain parallel last-write-wins writes.
        if field_shards.len() > 8 {
            let _field_write_guards = self.lock_hash_field_write_shards(&field_shards).await;
            loop {
                match self
                    .hash_set_ordered_bytes_async_attempt(key, fields)
                    .await?
                {
                    OrderedHashSetAttempt::Applied(added) => return Ok(added),
                    OrderedHashSetAttempt::Conflict => tokio::task::yield_now().await,
                }
            }
        }
        let field_read_guards = self.lock_hash_field_read_shards(&field_shards).await;
        let has_cached_counter = fields.iter().any(|(field, _)| {
            self.counter_cache.hash_routes.contains_key(&(
                self.db_index,
                super::hash_numeric_update::hash_counter_route_key(key, field),
            ))
        });
        if !has_cached_counter {
            for _ in 0..HASH_SET_SHARED_CAS_ATTEMPTS {
                match self
                    .hash_set_ordered_bytes_async_attempt(key, fields)
                    .await?
                {
                    OrderedHashSetAttempt::Applied(added) => return Ok(added),
                    OrderedHashSetAttempt::Conflict => tokio::task::yield_now().await,
                }
            }
        }
        drop(field_read_guards);
        let _field_write_guards = self.lock_hash_field_write_shards(&field_shards).await;
        loop {
            match self
                .hash_set_ordered_bytes_async_attempt(key, fields)
                .await?
            {
                OrderedHashSetAttempt::Applied(added) => return Ok(added),
                OrderedHashSetAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
    }

    async fn hash_set_ordered_bytes_async_attempt(
        &self,
        key: &str,
        fields: &[(&str, &[u8])],
    ) -> Result<OrderedHashSetAttempt, Error> {
        let key_bytes = self.mk(key);
        let raw_meta = self.store.get_raw_async(&key_bytes).await;
        let mut expired_at = None;
        let (meta, version) = match raw_meta.as_deref() {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode hash metadata"))?;
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    expired_at = Some(header.expire_ms);
                    (None, self.next_version_async().await)
                } else {
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let hash_meta = decode_hash_meta_checked(raw)?;
                    (Some(hash_meta), hash_meta.version)
                }
            }
            None => (None, self.next_version_async().await),
        };

        let mut unique_fields = Vec::new();
        let mut field_slots = HashMap::with_capacity(fields.len());
        let mut final_values = Vec::new();
        let mut added = vec![false; fields.len()];
        for (field, value) in fields {
            if let Some(slot) = field_slots.get(field).copied() {
                final_values[slot] = *value;
            } else {
                let slot = unique_fields.len();
                field_slots.insert(*field, slot);
                unique_fields.push(*field);
                final_values.push(*value);
            }
        }

        let may_have_field_ttl = meta.is_some_and(|meta| meta.may_have_field_ttl);
        let field_keys = unique_fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let mut read_keys = field_keys.clone();
        if may_have_field_ttl {
            read_keys.extend(
                unique_fields
                    .iter()
                    .map(|field| hash_field_expire_key(self.db_index, key, version, field)),
            );
        }
        let existing = if meta.is_some() {
            self.store.multi_get_raw_async(&read_keys).await
        } else {
            vec![None; read_keys.len()]
        };
        let field_count = unique_fields.len();
        let now = now_ms();
        let mut live = vec![false; field_count];
        let mut has_expire_marker = vec![false; field_count];
        for slot in 0..field_count {
            let expire = if may_have_field_ttl {
                let raw = existing[field_count + slot].as_deref();
                has_expire_marker[slot] = raw.is_some();
                raw.and_then(decode_u64_be).unwrap_or(0)
            } else {
                0
            };
            live[slot] = existing[slot].is_some() && (expire == 0 || now < expire);
        }

        let mut conditions = Vec::new();
        if meta.is_none() {
            conditions.push(match raw_meta.as_deref() {
                Some(raw) => CompareCondition::exists_with(&key_bytes, raw),
                None => CompareCondition::absent(&key_bytes),
            });
        } else {
            for slot in 0..field_count {
                if live[slot] {
                    continue;
                }
                conditions.push(CompareCondition::with_expected(
                    &field_keys[slot],
                    existing[slot].clone(),
                ));
                if may_have_field_ttl {
                    conditions.push(CompareCondition::with_expected(
                        &read_keys[field_count + slot],
                        existing[field_count + slot].clone(),
                    ));
                }
            }
        }

        let mut first_seen = HashSet::with_capacity(field_count);
        for (index, (field, _)) in fields.iter().enumerate() {
            if first_seen.insert(*field) {
                added[index] = !live[field_slots[field]];
            }
        }

        let mut batch = WriteBatch::new();
        if let Some(expire_ms) = expired_at {
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
        }
        if meta.is_none() {
            (batch.put(&key_bytes, &encode_hash_meta(0, version)))
                .expect("write batch append invariant violated");
        }
        for slot in 0..field_count {
            let unchanged = live[slot]
                && !has_expire_marker[slot]
                && existing[slot].as_deref() == Some(final_values[slot]);
            if !unchanged {
                (batch.put(&field_keys[slot], final_values[slot]))
                    .expect("write batch append invariant violated");
            }
            if has_expire_marker[slot] {
                (batch.delete(&read_keys[field_count + slot]))
                    .expect("write batch append invariant violated");
            }
        }

        if batch.count() == 0 {
            return Ok(OrderedHashSetAttempt::Applied(added));
        }
        self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
        if conditions.is_empty() {
            self.write_batch_if_not_empty_async(&batch).await;
        } else if !self
            .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
            .await?
        {
            return Ok(OrderedHashSetAttempt::Conflict);
        }
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_refresh(key)?;
        Ok(OrderedHashSetAttempt::Applied(added))
    }

    /// 删除 hash fields，返回实际删除的字段数量。
    pub fn hash_delete(&self, key: &str, fields: &[String]) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((expire_ms, version)) = meta else {
            return Ok(0);
        };

        let existing_fields = self.hash_live_entries_raw(key, version);
        let existing_field_keys: std::collections::HashSet<Vec<u8>> = existing_fields
            .iter()
            .map(|(field, _)| {
                hash_field_key(self.db_index, key, version, &String::from_utf8_lossy(field))
            })
            .collect();

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut seen_fields = HashSet::new();
        for field in fields {
            if !seen_fields.insert(field) {
                continue;
            }
            let field_key = hash_field_key(self.db_index, key, version, field);
            if existing_field_keys.contains(&field_key) {
                (batch.delete(&field_key)).expect("write batch append invariant violated");
                (batch.delete(&hash_field_expire_key(self.db_index, key, version, field)))
                    .expect("write batch append invariant violated");
                deleted += 1;
            }
        }

        if deleted > 0 && existing_fields.len() == deleted {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
        }

        if batch.count() > 0 {
            if existing_fields.len() == deleted {
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(deleted)
    }

    pub async fn hash_delete_async(&self, key: &str, fields: &[String]) -> Result<usize, Error> {
        let fields = fields.iter().map(String::as_str).collect::<Vec<_>>();
        Ok(self
            .hash_delete_ordered_refs_async(key, &fields)
            .await?
            .into_iter()
            .filter(|deleted| *deleted)
            .count())
    }

    pub(crate) async fn hash_delete_ordered_refs_async(
        &self,
        key: &str,
        fields: &[&str],
    ) -> Result<Vec<bool>, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.hash_delete_ordered_refs_async_unlocked(key, fields)
            .await
    }

    pub(in crate::store::db) async fn hash_delete_async_unlocked(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<usize, Error> {
        let fields = fields.iter().map(String::as_str).collect::<Vec<_>>();
        Ok(self
            .hash_delete_ordered_refs_async_unlocked(key, &fields)
            .await?
            .into_iter()
            .filter(|deleted| *deleted)
            .count())
    }

    async fn hash_delete_ordered_refs_async_unlocked(
        &self,
        key: &str,
        fields: &[&str],
    ) -> Result<Vec<bool>, Error> {
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(vec![false; fields.len()]);
        };
        let logical_key = key.as_bytes().to_vec();
        let key_epoch = self
            .counter_cache
            .hash_key_epoch(self.db_index, &logical_key);
        let cache_key = (self.db_index, logical_key);
        let total_len = if !meta.may_have_field_ttl
            && let Some(cached) = self.counter_cache.hash_lengths.get(&cache_key)
            && cached.version == meta.version
            && cached.key_epoch == key_epoch
        {
            cached.len
        } else {
            let len = self.hash_live_entries_for_meta_async(key, meta).await.len();
            if !meta.may_have_field_ttl {
                self.counter_cache
                    .hash_ever_populated
                    .store(true, Ordering::Release);
                self.counter_cache.hash_lengths.insert(
                    cache_key.clone(),
                    HashLenCacheEntry {
                        len,
                        version: meta.version,
                        key_epoch,
                    },
                );
            }
            len
        };
        let mut unique_fields = Vec::new();
        let mut seen_fields = HashSet::new();
        for (index, &field) in fields.iter().enumerate() {
            if seen_fields.insert(field) {
                unique_fields.push((field, index));
            }
        }
        let field_keys = unique_fields
            .iter()
            .map(|(field, _)| hash_field_key(self.db_index, key, meta.version, field))
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw_async(&field_keys).await;
        let expires = if meta.may_have_field_ttl {
            let expire_keys = unique_fields
                .iter()
                .map(|(field, _)| hash_field_expire_key(self.db_index, key, meta.version, field))
                .collect::<Vec<_>>();
            self.store.multi_get_raw_async(&expire_keys).await
        } else {
            vec![None; unique_fields.len()]
        };
        let mut batch = WriteBatch::new();
        let mut deleted_by_position = vec![false; fields.len()];
        let mut deleted = 0usize;
        let now = now_ms();
        for (((field, index), field_key), (value, expire)) in unique_fields
            .into_iter()
            .zip(field_keys)
            .zip(values.into_iter().zip(expires))
        {
            let expired = expire
                .as_deref()
                .and_then(decode_u64_be)
                .is_some_and(|expire_ms| expire_ms > 0 && now >= expire_ms);
            if value.is_some() && !expired {
                (batch.delete(&field_key)).expect("write batch append invariant violated");
                (batch.delete(&hash_field_expire_key(
                    self.db_index,
                    key,
                    meta.version,
                    field,
                )))
                .expect("write batch append invariant violated");
                deleted_by_position[index] = true;
                deleted += 1;
            }
        }

        if deleted > 0 && total_len == deleted {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
        }

        if batch.count() > 0 {
            if total_len == deleted {
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
            let new_epoch = self
                .counter_cache
                .hash_key_epoch(self.db_index, &cache_key.1);
            if !meta.may_have_field_ttl && total_len > deleted {
                self.counter_cache.hash_lengths.insert(
                    cache_key,
                    HashLenCacheEntry {
                        len: total_len - deleted,
                        version: meta.version,
                        key_epoch: new_epoch,
                    },
                );
            }
        }
        Ok(deleted_by_position)
    }
}
