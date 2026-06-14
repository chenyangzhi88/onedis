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
        for _ in 0..64 {
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
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
            conditions.push(CompareCondition::from_observed(
                key_bytes.clone(),
                &observed_meta,
            ));

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
                || !observed_field_state.as_ref().unwrap().exists,
                |observed| observed.value.is_none(),
            );
            if may_have_field_ttl {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            if let Some(observed) = observed_field.as_ref() {
                conditions.push(CompareCondition::from_observed(field_key, observed));
            } else {
                conditions.push(CompareCondition::from_observed_state(
                    field_key,
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
        for _ in 0..64 {
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
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
            conditions.push(CompareCondition::from_observed(
                key_bytes.clone(),
                &observed_meta,
            ));
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
                    if observed_field.value.is_none() {
                        added += 1;
                    }
                    batch.put(&field_key, value.as_bytes());
                    if may_have_field_ttl {
                        batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
                    }
                    conditions.push(CompareCondition::from_observed(field_key, &observed_field));
                } else {
                    let observed_field = self.store.observe_raw_key_state_async(&field_key).await;
                    if !observed_field.exists {
                        added += 1;
                    }
                    batch.put(&field_key, value.as_bytes());
                    conditions.push(CompareCondition::from_observed_state(
                        field_key,
                        &observed_field,
                    ));
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
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        let existing_fields = self.hash_entries_raw(key, version);
        let existing_field_keys: std::collections::HashSet<Vec<u8>> = existing_fields
            .iter()
            .map(|(field, _)| {
                hash_field_key(self.db_index, key, version, &String::from_utf8_lossy(field))
            })
            .collect();

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            if existing_field_keys.contains(&field_key) {
                batch.delete(&field_key);
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
                deleted += 1;
            }
        }

        if deleted > 0 && existing_fields.len() == deleted {
            batch.delete(&self.mk(key));
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
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        let existing_fields = self.hash_entries_raw(key, version);
        let existing_field_keys: std::collections::HashSet<Vec<u8>> = existing_fields
            .iter()
            .map(|(field, _)| {
                hash_field_key(self.db_index, key, version, &String::from_utf8_lossy(field))
            })
            .collect();

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            if existing_field_keys.contains(&field_key) {
                batch.delete(&field_key);
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
                deleted += 1;
            }
        }

        if deleted > 0 && existing_fields.len() == deleted {
            batch.delete(&self.mk(key));
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

    /// 检查 hash field 是否存在。
    pub fn hash_exists(&self, key: &str, field: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(false);
        };

        Ok(self.hash_live_field_value(key, version, field).is_some())
    }

    pub async fn hash_exists_async(&self, key: &str, field: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(false);
        };

        Ok(self
            .hash_live_field_value_async(key, version, field)
            .await
            .is_some())
    }

    /// 返回 hash field 数量。
    pub fn hash_len(&self, key: &str) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.hash_live_entries_raw(key, version).len())
    }

    pub async fn hash_len_async(&self, key: &str) -> Result<usize, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.hash_live_entries_raw_async(key, version).await.len())
    }

    /// 批量读取 hash fields。
    pub fn hash_multi_get(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        Ok(fields
            .iter()
            .map(|field| {
                self.hash_live_field_value(key, version, field)
                    .and_then(|value| String::from_utf8(value).ok())
            })
            .collect())
    }

    pub async fn hash_multi_get_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        let mut values = Vec::with_capacity(fields.len());
        for field in fields {
            values.push(
                self.hash_live_field_value_async(key, version, field)
                    .await
                    .and_then(|value| String::from_utf8(value).ok()),
            );
        }
        Ok(values)
    }

    /// 返回 hash 所有 field/value。
    pub fn hash_get_all(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw(key, version)
            .into_iter()
            .filter_map(|(field, value)| {
                match (String::from_utf8(field), String::from_utf8(value)) {
                    (Ok(field), Ok(value)) => Some((field, value)),
                    _ => None,
                }
            })
            .collect())
    }

    pub async fn hash_get_all_async(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(field, value)| {
                match (String::from_utf8(field), String::from_utf8(value)) {
                    (Ok(field), Ok(value)) => Some((field, value)),
                    _ => None,
                }
            })
            .collect())
    }

    /// 返回 hash 所有 field。
    pub fn hash_keys(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all(key)?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    pub async fn hash_keys_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_async(key)
            .await?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    /// 返回 hash 所有 value。
    pub fn hash_values(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all(key)?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub async fn hash_values_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_async(key)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub fn hash_random_fields(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<Vec<(String, Option<String>)>>, Error> {
        let mut entries = self.hash_get_all(key)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            let (field, value) = entries.remove(0);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            entries
                .into_iter()
                .take((count as usize).min(len))
                .collect::<Vec<_>>()
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|idx| entries[idx % len].clone())
                .collect::<Vec<_>>()
        };
        Ok(Some(
            selected
                .into_iter()
                .map(|(field, value)| (field, with_values.then_some(value)))
                .collect(),
        ))
    }

    pub async fn hash_random_fields_async(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<Vec<(String, Option<String>)>>, Error> {
        let mut entries = self.hash_get_all_async(key).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            let (field, value) = entries.remove(0);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            entries
                .into_iter()
                .take((count as usize).min(len))
                .collect::<Vec<_>>()
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|idx| entries[idx % len].clone())
                .collect::<Vec<_>>()
        };
        Ok(Some(
            selected
                .into_iter()
                .map(|(field, value)| (field, with_values.then_some(value)))
                .collect(),
        ))
    }

    /// 仅在 field 不存在时设置 hash field。
    pub fn hash_set_nx(&self, key: &str, field: &str, value: &str) -> Result<bool, Error> {
        if self.hash_exists(key, field)? {
            return Ok(false);
        }
        self.hash_set(key, field, value)
    }

    pub async fn hash_set_nx_async(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<bool, Error> {
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
            let (meta, version) = match raw_meta.as_deref() {
                Some(raw) => {
                    let Some(header) = decode_meta_header(raw) else {
                        return Err(Error::msg("Failed to decode hash metadata"));
                    };
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    (Some((header.expire_ms, header.version)), header.version)
                }
                None => (None, self.next_persisted_version_async().await),
            };
            let field_key = hash_field_key(self.db_index, key, version, field);
            let observed_field = self
                .hash_live_field_observed_async(key, version, field)
                .await;
            if observed_field.value.is_some() {
                return Ok(false);
            }

            let mut batch = WriteBatch::new();
            if meta.is_none() {
                batch.put(&key_bytes, &encode_hash_meta(0, version));
            }
            batch.put(&field_key, value.as_bytes());
            if meta.is_some() {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            let conditions = [
                CompareCondition::from_observed(key_bytes, &observed_meta),
                CompareCondition::from_observed(field_key, &observed_field),
            ];
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(true);
            }
        }
        Err(Error::msg("ERR hash write conflict"))
    }

    /// 按整数增量更新 hash field，返回更新后的值。
    pub fn hash_increment_by(&self, key: &str, field: &str, increment: i64) -> Result<i64, Error> {
        let current = match self.hash_get(key, field)? {
            Some(value) => value
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR hash value is not an integer"))?,
            None => 0,
        };
        let next = current
            .checked_add(increment)
            .ok_or_else(|| Error::msg("ERR increment or decrement would overflow"))?;
        self.hash_set(key, field, &next.to_string())?;
        Ok(next)
    }

    pub async fn hash_increment_by_async(
        &self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<i64, Error> {
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
            let (meta, version) = match raw_meta.as_deref() {
                Some(raw) => {
                    let Some(header) = decode_meta_header(raw) else {
                        return Err(Error::msg("Failed to decode hash metadata"));
                    };
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    (Some((header.expire_ms, header.version)), header.version)
                }
                None => (None, self.next_persisted_version_async().await),
            };
            let field_key = hash_field_key(self.db_index, key, version, field);
            let observed_field = self
                .hash_live_field_observed_async(key, version, field)
                .await;
            let raw_field = observed_field.value.as_ref().map(|value| value.to_vec());
            let current = match raw_field.as_deref() {
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
                batch.put(&key_bytes, &encode_hash_meta(0, version));
            }
            batch.put(&field_key, next.to_string().as_bytes());
            if meta.is_some() {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            let conditions = [
                CompareCondition::from_observed(key_bytes, &observed_meta),
                CompareCondition::from_observed(field_key, &observed_field),
            ];
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(next);
            }
        }
        Err(Error::msg("ERR hash write conflict"))
    }

    pub fn hash_increment_by_float(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<String, Error> {
        let current = match self.hash_get(key, field)? {
            Some(value) => {
                let parsed = value
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
        self.hash_set(key, field, &formatted)?;
        Ok(formatted)
    }

    pub async fn hash_increment_by_float_async(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<String, Error> {
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
            let (meta, version) = match raw_meta.as_deref() {
                Some(raw) => {
                    let Some(header) = decode_meta_header(raw) else {
                        return Err(Error::msg("Failed to decode hash metadata"));
                    };
                    if header.type_tag != TYPE_HASH {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    (Some((header.expire_ms, header.version)), header.version)
                }
                None => (None, self.next_persisted_version_async().await),
            };
            let field_key = hash_field_key(self.db_index, key, version, field);
            let observed_field = self
                .hash_live_field_observed_async(key, version, field)
                .await;
            let raw_field = observed_field.value.as_ref().map(|value| value.to_vec());
            let current = match raw_field.as_deref() {
                Some(value) => {
                    let text = std::str::from_utf8(value)
                        .map_err(|_| Error::msg("ERR hash value is not a float"))?;
                    let parsed = text
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
                batch.put(&key_bytes, &encode_hash_meta(0, version));
            }
            batch.put(&field_key, formatted.as_bytes());
            if meta.is_some() {
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            let conditions = [
                CompareCondition::from_observed(key_bytes, &observed_meta),
                CompareCondition::from_observed(field_key, &observed_field),
            ];
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(formatted);
            }
        }
        Err(Error::msg("ERR hash write conflict"))
    }

    /// 批量设置 hash fields。
    pub fn hash_multi_set(&self, key: &str, fields: &HashMap<String, String>) -> Result<(), Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }

        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
        }

        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(())
    }

    pub async fn hash_multi_set_async(
        &self,
        key: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Error> {
        let items = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect::<Vec<_>>();
        self.hash_set_many_async(key, &items).await?;
        Ok(())
    }

    pub fn hash_get_del(&self, key: &str, fields: &[String]) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        self.hash_delete(key, fields)?;
        Ok(values)
    }

    pub async fn hash_get_del_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        self.hash_delete_async(key, fields).await?;
        Ok(values)
    }

    pub fn hash_get_ex(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        let Some(expiration) = expiration else {
            return Ok(values);
        };
        match expiration {
            StringExpireUpdate::Persist => {
                self.hash_persist_fields(key, fields)?;
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                let expire_ms = now_ms().saturating_add(ttl_ms);
                self.hash_expire_fields_at_ms(key, expire_ms, fields, ExpireCondition::Always)?;
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                self.hash_expire_fields_at_ms(key, expire_ms, fields, ExpireCondition::Always)?;
            }
        }
        Ok(values)
    }

    pub async fn hash_get_ex_async(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        let Some(expiration) = expiration else {
            return Ok(values);
        };
        match expiration {
            StringExpireUpdate::Persist => {
                self.hash_persist_fields_async(key, fields).await?;
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                let expire_ms = now_ms().saturating_add(ttl_ms);
                self.hash_expire_fields_at_ms_async(
                    key,
                    expire_ms,
                    fields,
                    ExpireCondition::Always,
                )
                .await?;
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                self.hash_expire_fields_at_ms_async(
                    key,
                    expire_ms,
                    fields,
                    ExpireCondition::Always,
                )
                .await?;
            }
        }
        Ok(values)
    }

    pub fn hash_set_ex(
        &self,
        key: &str,
        fields: &[(String, String)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        if fnx
            && fields
                .iter()
                .any(|(field, _)| self.hash_live_field_value(key, version, field).is_some())
        {
            return Ok(false);
        }
        if fxx
            && fields
                .iter()
                .any(|(field, _)| self.hash_live_field_value(key, version, field).is_none())
        {
            return Ok(false);
        }

        let expire_ms = match expiration {
            Some(StringExpireUpdate::RelativeMs(ttl_ms)) => Some(now_ms().saturating_add(ttl_ms)),
            Some(StringExpireUpdate::AbsoluteMs(expire_ms)) => Some(expire_ms),
            Some(StringExpireUpdate::Persist) => Some(0),
            None => None,
        };
        let field_ttl_requested = expire_ms.is_some_and(|expire_ms| expire_ms > 0);
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        } else if field_ttl_requested {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(meta.unwrap().0, version, true),
            );
        }
        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if let Some(expire_ms) = expire_ms {
                if expire_ms > 0 {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                } else {
                    batch.delete(&expire_key);
                }
            } else if !keep_ttl {
                batch.delete(&expire_key);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(true)
    }

    pub async fn hash_set_ex_async(
        &self,
        key: &str,
        fields: &[(String, String)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        if fnx || fxx {
            for (field, _) in fields {
                let exists = self
                    .hash_live_field_value_async(key, version, field)
                    .await
                    .is_some();
                if (fnx && exists) || (fxx && !exists) {
                    return Ok(false);
                }
            }
        }

        let expire_ms = match expiration {
            Some(StringExpireUpdate::RelativeMs(ttl_ms)) => Some(now_ms().saturating_add(ttl_ms)),
            Some(StringExpireUpdate::AbsoluteMs(expire_ms)) => Some(expire_ms),
            Some(StringExpireUpdate::Persist) => Some(0),
            None => None,
        };
        let field_ttl_requested = expire_ms.is_some_and(|expire_ms| expire_ms > 0);
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        } else if field_ttl_requested {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(meta.unwrap().0, version, true),
            );
        }
        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if let Some(expire_ms) = expire_ms {
                if expire_ms > 0 {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                } else {
                    batch.delete(&expire_key);
                }
            } else if !keep_ttl {
                batch.delete(&expire_key);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(true)
    }

    pub fn hash_expire_fields_at_ms(
        &self,
        key: &str,
        expire_ms: u64,
        fields: &[String],
        condition: ExpireCondition,
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((hash_expire_ms, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let now = now_ms();
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            if self.hash_live_field_value(key, version, field).is_none() {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            let current = self
                .store
                .get_raw(&expire_key)
                .and_then(|raw| decode_u64_be(&raw))
                .unwrap_or(0);
            if expire_ms <= now {
                batch.delete(&field_key);
                batch.delete(&expire_key);
                result.push(2);
                continue;
            }
            let matches = match condition {
                ExpireCondition::Always => true,
                ExpireCondition::Nx => current == 0,
                ExpireCondition::Xx => current > 0,
                ExpireCondition::Gt => current > 0 && expire_ms > current,
                ExpireCondition::Lt => current == 0 || expire_ms < current,
            };
            if matches {
                batch.put(
                    &self.mk(key),
                    &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
                );
                batch.put(&expire_key, &expire_ms.to_be_bytes());
                result.push(1);
            } else {
                result.push(0);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(result)
    }

    pub async fn hash_expire_fields_at_ms_async(
        &self,
        key: &str,
        expire_ms: u64,
        fields: &[String],
        condition: ExpireCondition,
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((hash_expire_ms, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let now = now_ms();
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            if self.hash_live_field_value(key, version, field).is_none() {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            let current = self
                .store
                .get_raw(&expire_key)
                .and_then(|raw| decode_u64_be(&raw))
                .unwrap_or(0);
            if expire_ms <= now {
                batch.delete(&field_key);
                batch.delete(&expire_key);
                result.push(2);
                continue;
            }
            let matches = match condition {
                ExpireCondition::Always => true,
                ExpireCondition::Nx => current == 0,
                ExpireCondition::Xx => current > 0,
                ExpireCondition::Gt => current > 0 && expire_ms > current,
                ExpireCondition::Lt => current == 0 || expire_ms < current,
            };
            if matches {
                batch.put(
                    &self.mk(key),
                    &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
                );
                batch.put(&expire_key, &expire_ms.to_be_bytes());
                result.push(1);
            } else {
                result.push(0);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(result)
    }

    pub fn hash_persist_fields(&self, key: &str, fields: &[String]) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            if self.hash_live_field_value(key, version, field).is_none() {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if self.store.contains_key(&expire_key) {
                batch.delete(&expire_key);
                result.push(1);
            } else {
                result.push(-1);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(result)
    }

    pub async fn hash_persist_fields_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            if self.hash_live_field_value(key, version, field).is_none() {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if self.store.contains_key(&expire_key) {
                batch.delete(&expire_key);
                result.push(1);
            } else {
                result.push(-1);
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(result)
    }

    pub fn hash_field_ttls(
        &self,
        key: &str,
        fields: &[String],
        millis: bool,
        absolute: bool,
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let now = now_ms();
        Ok(fields
            .iter()
            .map(|field| {
                if self.hash_live_field_value(key, version, field).is_none() {
                    return -2;
                }
                let expire_key = hash_field_expire_key(self.db_index, key, version, field);
                let Some(expire_ms) = self
                    .store
                    .get_raw(&expire_key)
                    .and_then(|raw| decode_u64_be(&raw))
                else {
                    return -1;
                };
                if absolute {
                    if millis {
                        expire_ms as i64
                    } else {
                        (expire_ms / 1000) as i64
                    }
                } else if expire_ms <= now {
                    -2
                } else {
                    let ttl_ms = expire_ms - now;
                    if millis {
                        ttl_ms as i64
                    } else {
                        ttl_ms.div_ceil(1000) as i64
                    }
                }
            })
            .collect())
    }

    pub async fn hash_field_ttls_async(
        &self,
        key: &str,
        fields: &[String],
        millis: bool,
        absolute: bool,
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let now = now_ms();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            if self
                .hash_live_field_value_async(key, version, field)
                .await
                .is_none()
            {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            let Some(expire_ms) = self
                .store
                .get_raw_async(&expire_key)
                .await
                .and_then(|raw| decode_u64_be(&raw))
            else {
                result.push(-1);
                continue;
            };
            let value = if absolute {
                if millis {
                    expire_ms as i64
                } else {
                    (expire_ms / 1000) as i64
                }
            } else if expire_ms <= now {
                -2
            } else {
                let ttl_ms = expire_ms - now;
                if millis {
                    ttl_ms as i64
                } else {
                    ttl_ms.div_ceil(1000) as i64
                }
            };
            result.push(value);
        }
        Ok(result)
    }

    /// 扫描 hash fields，返回下一个游标和 field/value 对。
    pub fn hash_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, String)>), Error> {
        let mut entries = self.hash_get_all(key)?;
        if pattern_str != "*" {
            entries.retain(|(field, _)| pattern::is_match(field, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
        let items = if start_index < entries.len() {
            entries[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= entries.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }

    pub async fn hash_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, String)>), Error> {
        let mut entries = self.hash_get_all_async(key).await?;
        if pattern_str != "*" {
            entries.retain(|(field, _)| pattern::is_match(field, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
        let items = if start_index < entries.len() {
            entries[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= entries.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }

}
