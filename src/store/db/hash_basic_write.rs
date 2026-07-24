use super::*;

impl Db {
    pub fn hash_get(&self, key: &str, field: &str) -> Result<Option<String>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        Ok(self
            .hash_live_field_value(key, version, field)
            .and_then(|value| String::from_utf8(value).ok()))
    }

    pub async fn hash_get_async(&self, key: &str, field: &str) -> Result<Option<String>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        Ok(self
            .hash_live_field_value_async(key, version, field)
            .await
            .and_then(|value| String::from_utf8(value).ok()))
    }

    /// 设置 hash field，返回是否为新字段。
    pub fn hash_set(&self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        let field_key = hash_field_key(self.db_index, key, version, field);
        let is_new_field = meta.is_none() || !self.store.contains_key(&field_key);

        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }
        batch.put(&field_key, value.as_bytes());
        if meta.is_some() {
            batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.hash_set_async_unlocked(key, field, value).await
    }

    async fn hash_set_async_unlocked(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<bool, Error> {
        for _ in 0..64 {
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value().map(|value| value.to_vec());
            let mut expired_meta = None;
            let (meta, version) = match raw_meta.as_deref() {
                Some(raw) => {
                    let header = decode_hash_meta_checked(raw)?;
                    if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                        expired_meta = Some((header.expire_ms, header.version, TYPE_HASH));
                        (None, self.next_persisted_version_async().await)
                    } else {
                        (Some(header), header.version)
                    }
                }
                None => (None, self.next_persisted_version_async().await),
            };
            let field_key = hash_field_key(self.db_index, key, version, field);
            let mut conditions = Vec::with_capacity(2);
            conditions.push(CompareCondition::from_observed(&observed_meta));

            let mut batch = WriteBatch::new();
            if let Some((expire_ms, old_version, old_type_tag)) = expired_meta {
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, old_version, old_type_tag);
                self.ttl_manager
                    .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            }
            if meta.is_none() {
                batch.put(&key_bytes, &encode_hash_meta(0, version));
            }
            batch.put(&field_key, value.as_bytes());

            let may_have_field_ttl = meta.is_some_and(|meta| meta.may_have_field_ttl);
            let use_value_observed = meta.is_none() || may_have_field_ttl;
            let observed_field = if use_value_observed {
                Some(
                    self.hash_live_field_observed_async(key, version, field)
                        .await,
                )
            } else {
                None
            };
            let observed_field_state = if use_value_observed {
                None
            } else {
                Some(self.store.observe_raw_key_state_async(&field_key).await)
            };
            let is_new_field = observed_field.as_ref().map_or_else(
                || !observed_field_state.as_ref().unwrap().exists(),
                |observed| observed.value().is_none(),
            );
            if may_have_field_ttl {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            if let Some(observed) = observed_field.as_ref() {
                conditions.push(CompareCondition::from_observed(observed));
            } else {
                conditions.push(CompareCondition::from_observed_state(
                    observed_field_state.as_ref().unwrap(),
                ));
            }

            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                self.fulltext_request_refresh(key)?;
                return Ok(is_new_field);
            }
        }
        Err(Error::msg("ERR hash write conflict"))
    }

    pub fn hash_set_many(&self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }

        let mut added = 0usize;
        let mut seen_in_batch = HashSet::new();
        for (field, value) in fields {
            if !seen_in_batch.insert(field.clone()) {
                batch.put(
                    &hash_field_key(self.db_index, key, version, field),
                    value.as_bytes(),
                );
                continue;
            }
            let field_key = hash_field_key(self.db_index, key, version, field);
            if meta.is_none() || !self.store.contains_key(&field_key) {
                added += 1;
            }
            batch.put(&field_key, value.as_bytes());
            if meta.is_some() {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.hash_set_many_async_unlocked(key, fields).await
    }

    async fn hash_set_many_async_unlocked(
        &self,
        key: &str,
        fields: &[(String, String)],
    ) -> Result<usize, Error> {
        for _ in 0..64 {
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value().map(|value| value.to_vec());
            let mut expired_meta = None;
            let (meta, version) = match raw_meta.as_deref() {
                Some(raw) => {
                    let header = decode_hash_meta_checked(raw)?;
                    if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                        expired_meta = Some((header.expire_ms, header.version, TYPE_HASH));
                        (None, self.next_persisted_version_async().await)
                    } else {
                        (Some(header), header.version)
                    }
                }
                None => (None, self.next_persisted_version_async().await),
            };
            let mut batch = WriteBatch::new();
            if let Some((expire_ms, old_version, old_type_tag)) = expired_meta {
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, old_version, old_type_tag);
                self.ttl_manager
                    .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            }
            if meta.is_none() {
                batch.put(&key_bytes, &encode_hash_meta(0, version));
            }

            let mut added = 0usize;
            let mut seen_in_batch = HashSet::new();
            let mut conditions = Vec::with_capacity(fields.len() + 1);
            conditions.push(CompareCondition::from_observed(&observed_meta));
            let may_have_field_ttl = meta.is_some_and(|meta| meta.may_have_field_ttl);
            let use_value_observed = meta.is_none() || may_have_field_ttl;
            for (field, value) in fields {
                if !seen_in_batch.insert(field.clone()) {
                    batch.put(
                        &hash_field_key(self.db_index, key, version, field),
                        value.as_bytes(),
                    );
                    continue;
                }
                let field_key = hash_field_key(self.db_index, key, version, field);
                if use_value_observed {
                    let observed_field = if may_have_field_ttl {
                        self.hash_live_field_observed_async(key, version, field)
                            .await
                    } else {
                        self.store.get_raw_observed_async(&field_key).await
                    };
                    if observed_field.value().is_none() {
                        added += 1;
                    }
                    batch.put(&field_key, value.as_bytes());
                    if may_have_field_ttl {
                        batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
                    }
                    conditions.push(CompareCondition::from_observed(&observed_field));
                } else {
                    let observed_field = self.store.observe_raw_key_state_async(&field_key).await;
                    if !observed_field.exists() {
                        added += 1;
                    }
                    batch.put(&field_key, value.as_bytes());
                    conditions.push(CompareCondition::from_observed_state(&observed_field));
                }
            }

            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                self.fulltext_request_refresh(key)?;
                return Ok(added);
            }
        }
        Err(Error::msg("ERR hash write conflict"))
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
                batch.delete(&field_key);
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.hash_delete_async_unlocked(key, fields).await
    }

    pub(in crate::store::db) async fn hash_delete_async_unlocked(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<usize, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((expire_ms, version)) = meta else {
            return Ok(0);
        };

        let existing_fields = self.hash_live_entries_raw_async(key, version).await;
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
                batch.delete(&field_key);
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
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
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(deleted)
    }
}
