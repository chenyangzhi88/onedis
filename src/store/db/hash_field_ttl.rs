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
}
