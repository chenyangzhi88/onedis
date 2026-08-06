use super::*;

impl Db {
    pub(in crate::store::db) fn append_expiration_delete_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        key_bytes: &[u8],
        header: MetaHeader,
    ) -> Result<(), Error> {
        batch.delete(key_bytes);
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
    pub fn expire_if_needed(&self, key: &str) {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return;
            };
            let Some(header) = decode_meta_header(raw) else {
                return;
            };
            if header.expire_ms == 0 || now_ms() < header.expire_ms {
                return;
            }

            let mut batch = WriteBatch::new();
            if let Err(error) =
                self.append_expiration_delete_to_batch(&mut batch, key, &key_bytes, header)
            {
                log::error!("failed to enqueue expiration cleanup for {key}: {error}");
                return;
            }
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
                Ok(true) => {
                    self.refresh_after_expiration_delete(key, header.type_tag);
                    return;
                }
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to delete expired key {key}: {error}");
                    return;
                }
            }
        }
        log::warn!("gave up deleting repeatedly modified expired key {key}");
    }

    pub async fn expire_if_needed_async(&self, key: &str) {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return;
            };
            let Some(header) = decode_meta_header(raw) else {
                return;
            };
            if header.expire_ms == 0 || now_ms() < header.expire_ms {
                return;
            }

            let mut batch = WriteBatch::new();
            if let Err(error) =
                self.append_expiration_delete_to_batch(&mut batch, key, &key_bytes, header)
            {
                log::error!("failed to enqueue expiration cleanup for {key}: {error}");
                return;
            }
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => {
                    self.refresh_after_expiration_delete(key, header.type_tag);
                    return;
                }
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to delete expired key {key}: {error}");
                    return;
                }
            }
        }
        log::warn!("gave up deleting repeatedly modified expired key {key}");
    }

    /**
     * 获取过期毫秒数
     */
    pub fn ttl_millis(&self, key: &str) -> i64 {
        self.expire_if_needed(key);
        let key_bytes = self.mk(key);
        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return -2;
        };
        let expire_ms = decode_expire_ms(&raw);
        if expire_ms == 0 {
            return -1; // 无过期
        }
        let now = now_ms();
        if now >= expire_ms {
            self.expire_if_needed(key);
            -2
        } else {
            (expire_ms - now) as i64
        }
    }

    /**
     * 检查键是否存在
     */
    pub fn exists(&self, key: &str) -> bool {
        self.expire_if_needed(key);
        self.store.contains_key(&self.mk(key))
    }
}
