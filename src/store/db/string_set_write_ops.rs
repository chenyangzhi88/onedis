use super::*;

impl Db {
    pub fn insert_string(&self, key: String, value: String, ttl_ms: Option<u64>) {
        self.insert_string_bytes(key, value.into_bytes(), ttl_ms);
    }

    pub async fn insert_string_async(&self, key: String, value: String, ttl_ms: Option<u64>) {
        self.insert_string_bytes_async(key, value.into_bytes(), ttl_ms)
            .await;
    }

    pub fn insert_string_bytes(&self, key: String, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let expire_ms = ttl_ms.map_or(0, |ttl| now_ms().saturating_add(ttl));
        self.write_string(&key, &value, expire_ms);
    }

    pub async fn insert_string_bytes_async(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let expire_ms = ttl_ms.map_or(0, |ttl| now_ms().saturating_add(ttl));
        let old_raw = if self.version_counter.current() == 0 {
            None
        } else {
            self.store.get_raw_async(&self.mk(&key)).await
        };
        self.write_string_async(&key, &value, expire_ms, old_raw.as_deref())
            .await;
    }

    pub fn set_string_bytes(
        &self,
        key: String,
        value: Vec<u8>,
        expiration: SetExpiration,
        condition: SetCondition,
        return_old: bool,
    ) -> Result<SetOutcome, Error> {
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
        self.expire_if_needed_async(&key).await;
        let old_raw = self.store.get_raw_async(&self.mk(&key)).await;
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
            self.remove_internal_async(&key, false).await;
        } else {
            self.write_string_async(&key, &value, expire_ms, old_raw.as_deref())
                .await;
        }

        Ok(SetOutcome::Set { old_value })
    }

    pub fn insert_string_ref(&self, key: &str, value: &str) {
        self.insert_string_bytes_ref(key, value.as_bytes());
    }

    pub fn insert_string_bytes_ref(&self, key: &str, value: &[u8]) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.write_string(key, value, 0);
    }
}
