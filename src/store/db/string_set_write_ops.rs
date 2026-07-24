use super::*;

impl Db {
    pub fn insert_string(&self, key: String, value: String, ttl_ms: Option<u64>) {
        self.insert_string_bytes(key, value.into_bytes(), ttl_ms);
    }

    pub async fn insert_string_async(
        &self,
        key: String,
        value: String,
        ttl_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.insert_string_bytes_async(key, value.into_bytes(), ttl_ms)
            .await
    }

    pub fn insert_string_bytes(&self, key: String, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let expire_ms = ttl_ms.map_or(0, |ttl| now_ms().saturating_add(ttl));
        self.write_plain_string(&key, &value, expire_ms);
    }

    pub async fn insert_string_bytes_async(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<(), Error> {
        let expiration = ttl_ms.map_or(SetExpiration::Clear, |ttl| {
            SetExpiration::At(now_ms().saturating_add(ttl))
        });
        self.set_string_bytes_async(key, value, expiration, SetCondition::Always, false)
            .await?;
        Ok(())
    }

    pub fn set_string_bytes(
        &self,
        key: String,
        value: Vec<u8>,
        expiration: SetExpiration,
        condition: SetCondition,
        return_old: bool,
    ) -> Result<SetOutcome, Error> {
        if condition == SetCondition::Always
            && !return_old
            && let Some(expire_ms) = plain_set_expire_ms(expiration)
        {
            self.changes.fetch_add(1, Ordering::Relaxed);
            if expire_ms > 0 && now_ms() >= expire_ms {
                self.remove_internal(&key, false);
            } else {
                self.write_plain_string(&key, &value, expire_ms);
            }
            return Ok(SetOutcome::Set { old_value: None });
        }

        self.expire_if_needed(&key);
        let old_raw = self.store.get_raw(&self.mk(&key));
        let old_header = old_raw.as_deref().and_then(decode_meta_header);
        let exists = old_header.is_some();

        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !exists,
            SetCondition::Xx => exists,
        };
        if !condition_matches {
            return Ok(SetOutcome::NotSet);
        }

        let old_value = if return_old {
            match old_raw.as_deref() {
                Some(raw) => {
                    let Some(header) = old_header else {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    };
                    if header.type_tag != TYPE_STRING {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    Some(decode_string_bytes(raw).ok_or_else(|| Error::msg("Type parsing error"))?)
                }
                None => None,
            }
        } else {
            None
        };

        let expire_ms = match expiration {
            SetExpiration::Clear => 0,
            SetExpiration::KeepTtl => old_header.map_or(0, |header| header.expire_ms),
            SetExpiration::At(expire_ms) => expire_ms,
        };

        self.changes.fetch_add(1, Ordering::Relaxed);
        if expire_ms > 0 && now_ms() >= expire_ms {
            self.remove_internal(&key, false);
        } else {
            self.write_string(&key, &value, expire_ms);
        }

        Ok(SetOutcome::Set { old_value })
    }

    pub async fn set_string_bytes_async(
        &self,
        key: String,
        value: Vec<u8>,
        expiration: SetExpiration,
        condition: SetCondition,
        return_old: bool,
    ) -> Result<SetOutcome, Error> {
        for _ in 0..64 {
            self.expire_if_needed_async(&key).await;
            let key_bytes = self.mk(&key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let old_raw = observed.value().map(|value| value.to_vec());
            let old_header = old_raw.as_deref().and_then(decode_meta_header);
            let exists = old_header.is_some();

            let condition_matches = match condition {
                SetCondition::Always => true,
                SetCondition::Nx => !exists,
                SetCondition::Xx => exists,
            };
            if !condition_matches {
                return Ok(SetOutcome::NotSet);
            }

            let old_value = if return_old {
                match old_raw.as_deref() {
                    Some(raw) => {
                        let Some(header) = old_header else {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        };
                        if header.type_tag != TYPE_STRING {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        }
                        Some(
                            decode_string_bytes(raw)
                                .ok_or_else(|| Error::msg("Type parsing error"))?,
                        )
                    }
                    None => None,
                }
            } else {
                None
            };

            let expire_ms = match expiration {
                SetExpiration::Clear => 0,
                SetExpiration::KeepTtl => old_header.map_or(0, |header| header.expire_ms),
                SetExpiration::At(expire_ms) => expire_ms,
            };
            let mut batch = WriteBatch::new();
            self.cleanup_old_complex_subkeys_for_string_overwrite_range_to_batch(
                &mut batch,
                &key,
                old_raw.as_deref(),
            );
            if expire_ms > 0 && now_ms() >= expire_ms {
                batch.delete(&key_bytes);
                if let Some(header) = old_header
                    && header.expire_ms > 0
                {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        &key,
                    );
                }
            } else {
                self.write_string_to_batch_with_deferred_old_raw(
                    &mut batch,
                    &key,
                    &value,
                    expire_ms,
                    old_raw.as_deref(),
                );
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(SetOutcome::Set { old_value });
            }
        }
        Err(Error::msg("ERR string write conflict"))
    }

    pub(crate) async fn mutate_string_bytes_async<R, F>(
        &self,
        key: &str,
        mutation: F,
    ) -> Result<R, Error>
    where
        F: Fn(&mut Vec<u8>, bool) -> Result<R, Error>,
    {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let exists = observed.value().is_some();
            let (expire_ms, mut value) = match observed.value() {
                Some(raw) => {
                    let header =
                        decode_meta_header(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
                    if header.type_tag != TYPE_STRING {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let value =
                        decode_string_bytes(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
                    (header.expire_ms, value)
                }
                None => (0, Vec::new()),
            };
            let result = mutation(&mut value, exists)?;
            let mut batch = WriteBatch::new();
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                &value,
                expire_ms,
                observed.value().map(|raw| raw.as_ref()),
            );
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(result);
            }
        }
        Err(Error::msg("ERR string write conflict"))
    }

    pub fn insert_string_ref(&self, key: &str, value: &str) {
        self.insert_string_bytes_ref(key, value.as_bytes());
    }

    pub fn insert_string_bytes_ref(&self, key: &str, value: &[u8]) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.write_plain_string(key, value, 0);
    }
}

fn plain_set_expire_ms(expiration: SetExpiration) -> Option<u64> {
    match expiration {
        SetExpiration::Clear => Some(0),
        SetExpiration::At(expire_ms) => Some(expire_ms),
        SetExpiration::KeepTtl => None,
    }
}
