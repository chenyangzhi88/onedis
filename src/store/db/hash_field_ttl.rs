use super::*;

impl Db {
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
        if version == 0 {
            self.promote_packed_hash(key)?;
            return self.hash_expire_fields_at_ms(key, expire_ms, fields, condition);
        }
        let now = now_ms();
        let delete_immediately = expire_ms <= now;
        let live_field_count = if delete_immediately {
            self.hash_live_entries_raw(key, version).len()
        } else {
            0
        };
        let mut deleted_fields = HashSet::new();
        let mut staged = HashMap::<String, (bool, u64)>::new();
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            let (exists, current) = if let Some(state) = staged.get(field) {
                *state
            } else {
                let exists = self.hash_live_field_value(key, version, field).is_some();
                let current = if exists {
                    self.store
                        .get_raw(&hash_field_expire_key(self.db_index, key, version, field))
                        .and_then(|raw| decode_u64_be(&raw))
                        .unwrap_or(0)
                } else {
                    0
                };
                staged.insert(field.clone(), (exists, current));
                (exists, current)
            };
            if !exists {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            let matches = match condition {
                ExpireCondition::Always => true,
                ExpireCondition::Nx => current == 0,
                ExpireCondition::Xx => current > 0,
                ExpireCondition::Gt => current > 0 && expire_ms > current,
                ExpireCondition::Lt => current == 0 || expire_ms < current,
                ExpireCondition::XxGt => current > 0 && expire_ms > current,
                ExpireCondition::XxLt => current > 0 && expire_ms < current,
            };
            if !matches {
                result.push(0);
                continue;
            }
            if delete_immediately {
                (batch.delete(&field_key)).expect("write batch append invariant violated");
                (batch.delete(&expire_key)).expect("write batch append invariant violated");
                deleted_fields.insert(field.clone());
                staged.insert(field.clone(), (false, 0));
                result.push(2);
                continue;
            }
            (batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
            ))
            .expect("write batch append invariant violated");
            (batch.put(&expire_key, &expire_ms.to_be_bytes()))
                .expect("write batch append invariant violated");
            staged.insert(field.clone(), (true, expire_ms));
            result.push(1);
        }
        if batch.count() > 0 {
            let delete_hash = live_field_count > 0 && deleted_fields.len() == live_field_count;
            if delete_hash {
                (batch.delete(&self.mk(key))).expect("write batch append invariant violated");
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, TYPE_HASH);
                if hash_expire_ms > 0 {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        hash_expire_ms,
                        self.db_index,
                        key,
                    );
                }
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
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
        self.promote_packed_hash_async(key).await?;
        if expire_ms <= now_ms() {
            let _hash_write_guard = self.set_write_lock(key).lock().await;
            return self
                .hash_expire_fields_at_ms_async_unlocked(key, expire_ms, fields, condition)
                .await;
        }
        let _hash_read_guard = self.set_write_lock(key).read().await;
        let field_shards = unique_hash_field_write_lock_shards(
            self.db_index,
            key,
            fields.iter().map(String::as_str),
        );
        let _field_guards = self.lock_hash_field_write_shards(&field_shards).await;
        self.hash_expire_fields_at_ms_async_unlocked(key, expire_ms, fields, condition)
            .await
    }

    pub(in crate::store::db) async fn hash_expire_fields_at_ms_async_unlocked(
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
        let delete_immediately = expire_ms <= now;
        let live_field_count = if delete_immediately {
            self.hash_live_entries_raw_async(key, version).await.len()
        } else {
            0
        };
        let mut deleted_fields = HashSet::new();
        let mut unique_fields = Vec::new();
        let mut seen_fields = HashSet::new();
        for field in fields {
            if seen_fields.insert(field) {
                unique_fields.push(field);
            }
        }
        let field_keys = unique_fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let expire_keys = unique_fields
            .iter()
            .map(|field| hash_field_expire_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw_async(&field_keys).await;
        let expires = self.store.multi_get_raw_async(&expire_keys).await;
        let mut staged = HashMap::<String, (bool, u64)>::with_capacity(unique_fields.len());
        for ((field, value), expire) in unique_fields.into_iter().zip(values).zip(expires) {
            let current = expire.as_deref().and_then(decode_u64_be).unwrap_or(0);
            let exists = value.is_some() && (current == 0 || current > now);
            staged.insert(field.clone(), (exists, current));
        }
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let field_key = hash_field_key(self.db_index, key, version, field);
            let (exists, current) = staged.get(field).copied().unwrap_or((false, 0));
            if !exists {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            let matches = match condition {
                ExpireCondition::Always => true,
                ExpireCondition::Nx => current == 0,
                ExpireCondition::Xx => current > 0,
                ExpireCondition::Gt => current > 0 && expire_ms > current,
                ExpireCondition::Lt => current == 0 || expire_ms < current,
                ExpireCondition::XxGt => current > 0 && expire_ms > current,
                ExpireCondition::XxLt => current > 0 && expire_ms < current,
            };
            if !matches {
                result.push(0);
                continue;
            }
            if delete_immediately {
                (batch.delete(&field_key)).expect("write batch append invariant violated");
                (batch.delete(&expire_key)).expect("write batch append invariant violated");
                deleted_fields.insert(field.clone());
                staged.insert(field.clone(), (false, 0));
                result.push(2);
                continue;
            }
            (batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
            ))
            .expect("write batch append invariant violated");
            (batch.put(&expire_key, &expire_ms.to_be_bytes()))
                .expect("write batch append invariant violated");
            staged.insert(field.clone(), (true, expire_ms));
            result.push(1);
        }
        if batch.count() > 0 {
            let delete_hash = live_field_count > 0 && deleted_fields.len() == live_field_count;
            if delete_hash {
                (batch.delete(&self.mk(key))).expect("write batch append invariant violated");
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, TYPE_HASH);
                if hash_expire_ms > 0 {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        hash_expire_ms,
                        self.db_index,
                        key,
                    );
                }
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
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
        let mut staged = HashMap::<String, (bool, bool)>::new();
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let (exists, has_ttl) = if let Some(state) = staged.get(field) {
                *state
            } else {
                let exists = self.hash_live_field_value(key, version, field).is_some();
                let has_ttl = exists
                    && self.store.contains_key(&hash_field_expire_key(
                        self.db_index,
                        key,
                        version,
                        field,
                    ));
                staged.insert(field.clone(), (exists, has_ttl));
                (exists, has_ttl)
            };
            if !exists {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if has_ttl {
                (batch.delete(&expire_key)).expect("write batch append invariant violated");
                staged.insert(field.clone(), (true, false));
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
        if let Some(meta) = self.hash_meta_async(key).await?
            && meta.packed
        {
            let packed = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .and_then(|raw| decode_packed_hash(&raw))
                .unwrap_or_default();
            return Ok(fields
                .iter()
                .map(|field| if packed.contains_key(field) { -1 } else { -2 })
                .collect());
        }
        let _hash_read_guard = self.set_write_lock(key).read().await;
        let field_shards = unique_hash_field_write_lock_shards(
            self.db_index,
            key,
            fields.iter().map(String::as_str),
        );
        let _field_guards = self.lock_hash_field_write_shards(&field_shards).await;
        self.hash_persist_fields_async_unlocked(key, fields).await
    }

    pub(in crate::store::db) async fn hash_persist_fields_async_unlocked(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<i64>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        let now = now_ms();
        let mut unique_fields = Vec::new();
        let mut seen_fields = HashSet::new();
        for field in fields {
            if seen_fields.insert(field) {
                unique_fields.push(field);
            }
        }
        let field_keys = unique_fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let expire_keys = unique_fields
            .iter()
            .map(|field| hash_field_expire_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw_async(&field_keys).await;
        let expires = self.store.multi_get_raw_async(&expire_keys).await;
        let mut staged = HashMap::<String, (bool, bool)>::with_capacity(unique_fields.len());
        for ((field, value), expire) in unique_fields.into_iter().zip(values).zip(expires) {
            let expire_ms = expire.as_deref().and_then(decode_u64_be).unwrap_or(0);
            let exists = value.is_some() && (expire_ms == 0 || expire_ms > now);
            staged.insert(field.clone(), (exists, exists && expire_ms > 0));
        }
        let mut batch = WriteBatch::new();
        let mut result = Vec::with_capacity(fields.len());
        for field in fields {
            let (exists, has_ttl) = staged.get(field).copied().unwrap_or((false, false));
            if !exists {
                result.push(-2);
                continue;
            }
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if has_ttl {
                (batch.delete(&expire_key)).expect("write batch append invariant violated");
                staged.insert(field.clone(), (true, false));
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
        let meta = self.hash_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(vec![-2; fields.len()]);
        };
        if meta.packed {
            let packed = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .and_then(|raw| decode_packed_hash(&raw))
                .unwrap_or_default();
            return Ok(fields
                .iter()
                .map(|field| if packed.contains_key(field) { -1 } else { -2 })
                .collect());
        }
        let version = meta.version;
        let now = now_ms();
        let field_keys = fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let expire_keys = fields
            .iter()
            .map(|field| hash_field_expire_key(self.db_index, key, version, field))
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw_async(&field_keys).await;
        let expires = self.store.multi_get_raw_async(&expire_keys).await;
        Ok(values
            .into_iter()
            .zip(expires)
            .map(|(field_value, expire)| {
                if field_value.is_none() {
                    return -2;
                }
                let Some(expire_ms) = expire.as_deref().and_then(decode_u64_be) else {
                    return -1;
                };
                if expire_ms > 0 && expire_ms <= now {
                    -2
                } else if absolute {
                    if millis {
                        expire_ms as i64
                    } else {
                        (expire_ms / 1000) as i64
                    }
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
}
