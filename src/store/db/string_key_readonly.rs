use super::*;

impl Db {
    pub fn get_string_entry_raw_bytes(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        let Some(raw) = self.read_live_raw_key_bytes(key) else {
            return Ok(None);
        };
        if decode_string_bytes_slice(&raw).is_some() {
            Ok(Some(raw))
        } else {
            Err(Error::msg(WRONG_TYPE_ERROR))
        }
    }

    pub async fn get_string_entry_raw_bytes_async(
        &self,
        key: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        let Some(raw) = self.read_live_raw_key_bytes_async(key).await else {
            return Ok(None);
        };
        if decode_string_bytes_slice(&raw).is_some() {
            Ok(Some(raw))
        } else {
            Err(Error::msg(WRONG_TYPE_ERROR))
        }
    }

    pub fn getex_string_bytes(
        &self,
        key: &str,
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw(key) else {
            return Ok(None);
        };
        let value = decode_string_bytes(&raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
        let Some(expiration) = expiration else {
            return Ok(Some(value));
        };

        match expiration {
            StringExpireUpdate::Persist => {
                self.persist(key);
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                self.expire(key.to_string(), ttl_ms);
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                if expire_ms <= now_ms() {
                    self.delete_key_internal(key, false);
                } else {
                    self.expire(key.to_string(), expire_ms - now_ms());
                }
            }
        }
        Ok(Some(value))
    }

    pub async fn getex_string_bytes_async(
        &self,
        key: &str,
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(expiration) = expiration else {
            let Some(raw) = self.read_live_raw_async(key).await else {
                return Ok(None);
            };
            return decode_string_bytes(&raw)
                .map(Some)
                .ok_or_else(|| Error::msg(WRONG_TYPE_ERROR));
        };

        let _write_guard = self.set_write_lock(key).lock().await;
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return Ok(None);
            };
            let value = decode_string_bytes(raw).ok_or_else(|| Error::msg(WRONG_TYPE_ERROR))?;
            let header =
                decode_meta_header(raw).ok_or_else(|| Error::msg("ERR invalid string metadata"))?;
            let mut batch = WriteBatch::new();

            match expiration {
                StringExpireUpdate::Persist => {
                    if header.expire_ms == 0 {
                        return Ok(Some(value));
                    }
                    let patched = patch_meta_expire_ms(raw, 0)
                        .ok_or_else(|| Error::msg("ERR invalid string metadata"))?;
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        key,
                    );
                }
                StringExpireUpdate::RelativeMs(ttl_ms) => {
                    if ttl_ms == 0 {
                        self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
                    } else {
                        let expire_ms = now_ms().saturating_add(ttl_ms);
                        let patched = patch_meta_expire_ms(raw, expire_ms)
                            .ok_or_else(|| Error::msg("ERR invalid string metadata"))?;
                        batch.put(&key_bytes, &patched);
                        if header.expire_ms != expire_ms {
                            self.ttl_manager.remove_known_to_batch(
                                &mut batch,
                                header.expire_ms,
                                self.db_index,
                                key,
                            );
                        }
                        self.ttl_manager
                            .add_to_batch(&mut batch, expire_ms, self.db_index, key);
                    }
                }
                StringExpireUpdate::AbsoluteMs(expire_ms) => {
                    if expire_ms <= now_ms() {
                        self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
                    } else {
                        let patched = patch_meta_expire_ms(raw, expire_ms)
                            .ok_or_else(|| Error::msg("ERR invalid string metadata"))?;
                        batch.put(&key_bytes, &patched);
                        if header.expire_ms != expire_ms {
                            self.ttl_manager.remove_known_to_batch(
                                &mut batch,
                                header.expire_ms,
                                self.db_index,
                                key,
                            );
                        }
                        self.ttl_manager
                            .add_to_batch(&mut batch, expire_ms, self.db_index, key);
                    }
                }
            }

            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => return Ok(Some(value)),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR GETEX write conflict"))
    }

    pub fn type_name_readonly(&self, key: &str) -> &'static str {
        let Some(raw) = self.read_live_raw(key) else {
            return "none";
        };
        let Some(header) = decode_meta_header(&raw) else {
            return "none";
        };
        match header.type_tag {
            TYPE_STRING => "string",
            TYPE_HASH => "hash",
            TYPE_SET => "set",
            TYPE_SORTED_SET => "zset",
            TYPE_LIST => "list",
            TYPE_STREAM => "stream",
            TYPE_VECTOR => "vector",
            TYPE_JSON => "json",
            _ => "none",
        }
    }

    pub async fn type_name_readonly_async(&self, key: &str) -> &'static str {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return "none";
        };
        let Some(header) = decode_meta_header(&raw) else {
            return "none";
        };
        match header.type_tag {
            TYPE_STRING => "string",
            TYPE_HASH => "hash",
            TYPE_SET => "set",
            TYPE_SORTED_SET => "zset",
            TYPE_LIST => "list",
            TYPE_STREAM => "stream",
            TYPE_VECTOR => "vector",
            TYPE_JSON => "json",
            _ => "none",
        }
    }

    pub fn exists_readonly(&self, key: &str) -> bool {
        self.read_live_raw(key).is_some()
    }

    pub async fn exists_readonly_async(&self, key: &str) -> bool {
        self.read_live_raw_async(key).await.is_some()
    }

    pub fn ttl_millis_readonly(&self, key: &str) -> i64 {
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return -1;
        }
        let now = now_ms();
        if now >= expire_ms {
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    pub async fn ttl_millis_readonly_async(&self, key: &str) -> i64 {
        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return -1;
        }
        let now = now_ms();
        if now >= expire_ms {
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    pub fn expire_time_millis_readonly(&self, key: &str) -> i64 {
        let Some(raw) = self.read_live_raw(key) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 { -1 } else { expire_ms as i64 }
    }

    pub async fn expire_time_millis_readonly_async(&self, key: &str) -> i64 {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 { -1 } else { expire_ms as i64 }
    }

    pub(in crate::store::db) fn read_live_raw(&self, key: &str) -> Option<Vec<u8>> {
        let raw = self.store.get_raw(&self.mk(key))?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    pub(in crate::store::db) async fn read_live_raw_async(&self, key: &str) -> Option<Vec<u8>> {
        let raw = self.store.get_raw_async(&self.mk(key)).await?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    pub(in crate::store::db) fn read_live_raw_key_bytes(&self, key: &[u8]) -> Option<Bytes> {
        let raw = self
            .store
            .get_raw_bytes(&main_key_bytes(self.db_index, key))?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }

    pub(in crate::store::db) async fn read_live_raw_key_bytes_async(
        &self,
        key: &[u8],
    ) -> Option<Bytes> {
        let raw = self
            .store
            .get_raw_bytes_async(&main_key_bytes(self.db_index, key))
            .await?;
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms > 0 && now_ms() >= expire_ms {
            return None;
        }
        Some(raw)
    }
}
