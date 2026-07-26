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
        let expire_ms = now_ms().saturating_add(ttl);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return false;
            };
            let Some(header) = decode_meta_header(raw) else {
                return false;
            };
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                self.expire_if_needed(&key);
                continue;
            }
            if !Self::expire_condition_matches(header.expire_ms, expire_ms, condition) {
                return false;
            }
            let mut batch = WriteBatch::new();
            if ttl == 0 {
                if let Err(error) =
                    self.append_expiration_delete_to_batch(&mut batch, &key, &key_bytes, header)
                {
                    log::error!("failed to enqueue immediate expiration for {key}: {error}");
                    return false;
                }
            } else {
                let Some(patched) = patch_meta_expire_ms(raw, expire_ms) else {
                    return false;
                };
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
            }
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
                Ok(true) => {
                    if ttl == 0 {
                        self.refresh_after_expiration_delete(&key, header.type_tag);
                    }
                    return true;
                }
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
        let expire_ms = now_ms().saturating_add(ttl);
        for _ in 0..64 {
            self.expire_if_needed_async(&key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return false;
            };
            let Some(header) = decode_meta_header(raw) else {
                return false;
            };
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                self.expire_if_needed_async(&key).await;
                continue;
            }
            if !Self::expire_condition_matches(header.expire_ms, expire_ms, condition) {
                return false;
            }
            if ttl == 0 {
                let mut batch = WriteBatch::new();
                if let Err(error) =
                    self.append_expiration_delete_to_batch(&mut batch, &key, &key_bytes, header)
                {
                    log::error!("failed to enqueue immediate expiration for {key}: {error}");
                    return false;
                }
                match self
                    .compare_and_write_batch_if_not_empty_async(
                        &[CompareCondition::from_observed(&observed)],
                        &batch,
                    )
                    .await
                {
                    Ok(true) => {
                        self.refresh_after_expiration_delete(&key, header.type_tag);
                        return true;
                    }
                    Ok(false) => continue,
                    Err(error) => {
                        log::error!("failed to immediately expire key {key}: {error}");
                        return false;
                    }
                }
            }
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
        expire_ms: u64,
        condition: ExpireCondition,
    ) -> bool {
        match condition {
            ExpireCondition::Always => true,
            ExpireCondition::Nx => current_expire_ms == 0,
            ExpireCondition::Xx => current_expire_ms > 0,
            ExpireCondition::Gt => current_expire_ms > 0 && expire_ms > current_expire_ms,
            ExpireCondition::Lt => current_expire_ms == 0 || expire_ms < current_expire_ms,
            ExpireCondition::XxGt => current_expire_ms > 0 && expire_ms > current_expire_ms,
            ExpireCondition::XxLt => current_expire_ms > 0 && expire_ms < current_expire_ms,
        }
    }

    /**
     * 移除过期时间（PERSIST 命令）
     */
    pub fn persist(&self, key: &str) -> bool {
        self.expire_if_needed(key);
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return false;
            };
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 {
                return false;
            }
            if now_ms() >= expire_ms {
                self.expire_if_needed(key);
                continue;
            }
            let Some(patched) = patch_meta_expire_ms(raw, 0) else {
                return false;
            };
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &patched);
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
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

    pub async fn persist_async(&self, key: &str) -> bool {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.persist_async_unlocked(key).await
    }

    async fn persist_async_unlocked(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return false;
            };
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 {
                return false;
            }
            if now_ms() >= expire_ms {
                self.expire_if_needed_async(key).await;
                continue;
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
