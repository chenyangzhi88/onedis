use super::*;

impl Db {
    /**
     * 清理过期键
     */
    /**
     * 过期检测【惰性】
     */
    pub fn expire_if_needed(&self, key: &str) {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw)
                && header.expire_ms > 0
                && now_ms() >= header.expire_ms
            {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                    header.type_tag,
                );
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    key,
                );
                match header.type_tag {
                    TYPE_HASH => {
                        if let Err(err) =
                            self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)
                        {
                            log::error!(
                                "failed to enqueue fulltext delete for expired {key}: {err}"
                            );
                            return;
                        }
                    }
                    TYPE_JSON => {
                        if let Err(err) =
                            self.fulltext_enqueue_json_delete_to_batch(&mut batch, key)
                        {
                            log::error!(
                                "failed to enqueue fulltext JSON delete for expired {key}: {err}"
                            );
                            return;
                        }
                    }
                    _ => {}
                }
                self.write_batch_if_not_empty(&batch);
                let refresh = match header.type_tag {
                    TYPE_HASH => self.fulltext_request_refresh(key),
                    TYPE_JSON => self.fulltext_request_json_refresh(key),
                    _ => Ok(()),
                };
                if let Err(err) = refresh {
                    log::error!("failed to refresh fulltext expire for {key}: {err}");
                }
            }
        }
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
            batch.delete(&key_bytes);
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext delete for expired {key}: {err}");
                        return;
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                        log::error!(
                            "failed to enqueue fulltext JSON delete for expired {key}: {err}"
                        );
                        return;
                    }
                }
                _ => {}
            }
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => {
                    let refresh = match header.type_tag {
                        TYPE_HASH => self.fulltext_request_refresh(key),
                        TYPE_JSON => self.fulltext_request_json_refresh(key),
                        _ => Ok(()),
                    };
                    if let Err(err) = refresh {
                        log::error!("failed to refresh fulltext expire for {key}: {err}");
                    }
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
        let key_bytes = self.mk(key);
        if !self.store.contains_key(&key_bytes) {
            return -2;
        }
        let expire_ms = self.get_expire_ms(key);
        if expire_ms == 0 {
            return -1; // 无过期
        }
        let now = now_ms();
        if now >= expire_ms {
            self.remove_internal(key, false);
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
