use super::*;

impl Db {
    /**
     * 设置过期
     *
     * @param key 键名
     * @param ttl 距离现在多少【毫秒】后过期
     */
    pub fn expire(&self, key: String, ttl: u64) -> bool {
        self.expire_with_condition(key, ttl, ExpireCondition::Always)
    }

    pub async fn expire_async(&self, key: String, ttl: u64) -> bool {
        self.expire_with_condition_async(key, ttl, ExpireCondition::Always)
            .await
    }

    pub fn expire_with_condition(&self, key: String, ttl: u64, condition: ExpireCondition) -> bool {
        self.expire_if_needed(&key);
        let key_bytes = self.mk(&key);
        if let Some(raw) = self.store.get_raw(&key_bytes)
            && let Some(header) = decode_meta_header(&raw)
        {
            if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                return false;
            }
            if ttl == 0 {
                return self.remove_internal(&key, false).is_some();
            }
            let expire_ms = now_ms().saturating_add(ttl);
            if let Some(patched) = patch_meta_expire_ms(&raw, expire_ms) {
                let mut batch = WriteBatch::new();
                batch.put(&key_bytes, &patched);
                if header.expire_ms > 0 && header.expire_ms != expire_ms {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        &key,
                    );
                }
                self.ttl_manager
                    .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
                self.write_batch_if_not_empty(&batch);
                return true;
            }
        }
        false
    }

    pub async fn expire_with_condition_async(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> bool {
        let _write_guard = self.set_write_lock(&key).lock().await;
        self.expire_with_condition_async_unlocked(key, ttl, condition)
            .await
    }

    async fn expire_with_condition_async_unlocked(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> bool {
        let key_bytes = self.mk(&key);
        for _ in 0..64 {
            self.expire_if_needed_async(&key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return false;
            };
            let Some(header) = decode_meta_header(raw) else {
                return false;
            };
            if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                return false;
            }
            if ttl == 0 {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    &key,
                );
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    &key,
                    header.version,
                    header.type_tag,
                );
                if header.type_tag == TYPE_JSON {
                    delete_json_nodes_to_batch_async(
                        &self.store,
                        &mut batch,
                        self.db_index,
                        &key,
                        header.version,
                    )
                    .await;
                }
                match header.type_tag {
                    TYPE_HASH => {
                        if let Err(error) =
                            self.fulltext_enqueue_hash_delete_to_batch(&mut batch, &key)
                        {
                            log::error!(
                                "failed to enqueue fulltext delete for expired {key}: {error}"
                            );
                            return false;
                        }
                    }
                    TYPE_JSON => {
                        if let Err(error) =
                            self.fulltext_enqueue_json_delete_to_batch(&mut batch, &key)
                        {
                            log::error!(
                                "failed to enqueue fulltext JSON delete for expired {key}: {error}"
                            );
                            return false;
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
                            TYPE_HASH => self.fulltext_request_refresh(&key),
                            TYPE_JSON => self.fulltext_request_json_refresh(&key),
                            _ => Ok(()),
                        };
                        if let Err(error) = refresh {
                            log::error!(
                                "failed to refresh fulltext immediate expiration for {key}: {error}"
                            );
                        }
                        return true;
                    }
                    Ok(false) => continue,
                    Err(error) => {
                        log::error!("failed to immediately expire key {key}: {error}");
                        return false;
                    }
                }
            }
            let expire_ms = now_ms().saturating_add(ttl);
            let Some(patched) = patch_meta_expire_ms(raw, expire_ms) else {
                return false;
            };
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &patched);
            if header.expire_ms > 0 && header.expire_ms != expire_ms {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    &key,
                );
            }
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to update expiration for {key}: {error}");
                    return false;
                }
            }
        }
        log::warn!("gave up updating expiration for repeatedly modified key {key}");
        false
    }

    pub(in crate::store::db) fn expire_condition_matches(
        current_expire_ms: u64,
        ttl: u64,
        condition: ExpireCondition,
    ) -> bool {
        match condition {
            ExpireCondition::Always => true,
            ExpireCondition::Nx => current_expire_ms == 0,
            ExpireCondition::Xx => current_expire_ms > 0,
            ExpireCondition::Gt => {
                current_expire_ms > 0 && now_ms().saturating_add(ttl) > current_expire_ms
            }
            ExpireCondition::Lt => {
                current_expire_ms == 0 || now_ms().saturating_add(ttl) < current_expire_ms
            }
        }
    }

    /**
     * 移除过期时间（PERSIST 命令）
     */
    pub fn persist(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0
                && let Some(patched) = patch_meta_expire_ms(&raw, 0)
            {
                let mut batch = WriteBatch::new();
                batch.put(&key_bytes, &patched);
                self.ttl_manager
                    .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
                self.write_batch_if_not_empty(&batch);
                return true;
            }
        }
        false
    }

    pub async fn persist_async(&self, key: &str) -> bool {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.persist_async_unlocked(key).await
    }

    async fn persist_async_unlocked(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return false;
            };
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 {
                return false;
            }
            let Some(patched) = patch_meta_expire_ms(raw, 0) else {
                return false;
            };
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &patched);
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to persist key {key}: {error}");
                    return false;
                }
            }
        }
        log::warn!("gave up persisting repeatedly modified key {key}");
        false
    }

    /**
     * 删除键值
     *
     * @param key 键名
     * @return 如果删除成功，返回被删除的值
     */
    pub fn remove(&self, key: &str) -> Option<Structure> {
        self.remove_internal(key, true)
    }

    pub async fn remove_async(&self, key: &str) -> Option<Structure> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.remove_internal_async(key, true).await
    }

    pub fn delete_key(&self, key: &str) -> bool {
        self.delete_key_internal(key, true)
    }

    pub async fn delete_key_async(&self, key: &str) -> bool {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.delete_key_internal_async(key, true).await
    }

    pub fn touch(&self, key: &str) -> bool {
        self.read_live_raw(key).is_some()
    }

    pub async fn touch_async(&self, key: &str) -> bool {
        self.read_live_raw_async(key).await.is_some()
    }
}
