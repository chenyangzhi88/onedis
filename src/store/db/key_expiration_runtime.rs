use super::*;

impl Db {
    pub(in crate::store::db) fn append_fulltext_expiration_update_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        type_tag: u8,
    ) -> Result<(), Error> {
        match type_tag {
            TYPE_HASH => self.fulltext_enqueue_hash_upsert_to_batch(batch, key),
            TYPE_JSON => self.fulltext_enqueue_json_upsert_to_batch(batch, key),
            _ => Ok(()),
        }
    }

    pub(in crate::store::db) fn append_expiration_delete_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        key_bytes: &[u8],
        header: MetaHeader,
    ) -> Result<(), Error> {
        (batch.delete(key_bytes)).expect("write batch append invariant violated");
        self.ttl_manager
            .remove_known_to_batch(batch, header.expire_ms, self.db_index, key);
        match header.type_tag {
            TYPE_HASH => self.fulltext_enqueue_hash_delete_to_batch(batch, key)?,
            TYPE_JSON => self.fulltext_enqueue_json_delete_to_batch(batch, key)?,
            _ => {}
        }
        Ok(())
    }

    pub(in crate::store::db) fn refresh_after_expiration_delete(&self, key: &str, type_tag: u8) {
        let result = match type_tag {
            TYPE_HASH => self.fulltext_request_refresh(key),
            TYPE_JSON => self.fulltext_request_json_refresh(key),
            _ => Ok(()),
        };
        if let Err(error) = result {
            log::error!("failed to refresh fulltext expire for {key}: {error}");
        }
    }

    /**
     * 清理过期键
     */
    /**
     * 过期检测【惰性】
     */
    pub fn expire_if_needed(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self
                .store
                .get_raw_observed(&key_bytes)
                .map_err(|error| Error::msg(error.to_string()))?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(header) = decode_meta_header(raw) else {
                return Ok(());
            };
            if header.expire_ms == 0 || now_ms() < header.expire_ms {
                return Ok(());
            }

            let mut batch = WriteBatch::new();
            self.append_expiration_delete_to_batch(&mut batch, key, &key_bytes, header)?;
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
                Ok(true) => {
                    self.refresh_after_expiration_delete(key, header.type_tag);
                    return Ok(());
                }
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR expiration cleanup conflict"))
    }

    pub async fn expire_if_needed_async(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self
                .store
                .get_raw_observed_async(&key_bytes)
                .await
                .map_err(|error| Error::msg(error.to_string()))?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(header) = decode_meta_header(raw) else {
                return Ok(());
            };
            if header.expire_ms == 0 || now_ms() < header.expire_ms {
                return Ok(());
            }

            let mut batch = WriteBatch::new();
            self.append_expiration_delete_to_batch(&mut batch, key, &key_bytes, header)?;
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => {
                    self.refresh_after_expiration_delete(key, header.type_tag);
                    return Ok(());
                }
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR expiration cleanup conflict"))
    }

    /**
     * 获取过期毫秒数
     */
    pub fn ttl_millis(&self, key: &str) -> Result<i64, Error> {
        self.expire_if_needed(key)?;
        let key_bytes = self.mk(key);
        let Some(raw) = self
            .store
            .get_raw(&key_bytes)
            .map_err(|error| Error::msg(error.to_string()))?
        else {
            return Ok(-2);
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return Ok(-1); // 无过期
        }
        let now = now_ms();
        if now >= expire_ms {
            self.expire_if_needed(key)?;
            Ok(-2)
        } else {
            Ok((expire_ms - now) as i64)
        }
    }

    /**
     * 检查键是否存在
     */
    pub fn exists(&self, key: &str) -> Result<bool, Error> {
        self.expire_if_needed(key)?;
        self.store
            .contains_key(&self.mk(key))
            .map_err(|error| Error::msg(error.to_string()))
    }
}
