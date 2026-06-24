impl Db {
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
}
