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
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw) {
                if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                    return false;
                }
                if ttl == 0 {
                    return self.remove_internal(&key, false).is_some();
                }
                let expire_ms = now_ms() + ttl;
                if let Some(patched) = patch_meta_expire_ms(&raw, expire_ms) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager
                        .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
                    self.write_batch_if_not_empty(&batch);
                    return true;
                }
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
        self.expire_if_needed_async(&key).await;
        let key_bytes = self.mk(&key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            if let Some(header) = decode_meta_header(&raw) {
                if !Self::expire_condition_matches(header.expire_ms, ttl, condition) {
                    return false;
                }
                if ttl == 0 {
                    return self.remove_internal_async(&key, false).await.is_some();
                }
                let expire_ms = now_ms() + ttl;
                if let Some(patched) = patch_meta_expire_ms(&raw, expire_ms) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager
                        .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
                    self.write_batch_if_not_empty_async(&batch).await;
                    return true;
                }
            }
        }
        false
    }

    fn expire_condition_matches(
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
            if expire_ms > 0 {
                if let Some(patched) = patch_meta_expire_ms(&raw, 0) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        expire_ms,
                        self.db_index,
                        key,
                    );
                    self.write_batch_if_not_empty(&batch);
                    return true;
                }
            }
        }
        false
    }

    pub async fn persist_async(&self, key: &str) -> bool {
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 {
                if let Some(patched) = patch_meta_expire_ms(&raw, 0) {
                    let mut batch = WriteBatch::new();
                    batch.put(&key_bytes, &patched);
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        expire_ms,
                        self.db_index,
                        key,
                    );
                    self.write_batch_if_not_empty_async(&batch).await;
                    return true;
                }
            }
        }
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
        self.remove_internal_async(key, true).await
    }

    pub fn delete_key(&self, key: &str) -> bool {
        self.delete_key_internal(key, true)
    }

    pub async fn delete_key_async(&self, key: &str) -> bool {
        self.delete_key_internal_async(key, true).await
    }

    pub fn touch(&self, key: &str) -> bool {
        self.read_live_raw(key).is_some()
    }

    pub async fn touch_async(&self, key: &str) -> bool {
        self.read_live_raw_async(key).await.is_some()
    }
}
