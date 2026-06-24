impl Db {
    pub fn keys(&self, pattern_str: &str) -> Vec<String> {
        let now = now_ms();
        self.logical_keys()
            .into_iter()
            .filter(|key| {
                // skip expired keys lazily
                if let Some(raw) = self.store.get_raw(&self.mk(key)) {
                    let expire_ms = decode_expire_ms(&raw);
                    if expire_ms > 0 && now >= expire_ms {
                        return false;
                    }
                }
                pattern_str == "*" || pattern::is_match(key, pattern_str)
            })
            .collect()
    }

    pub async fn keys_async(&self, pattern_str: &str) -> Vec<String> {
        let now = now_ms();
        let keys = self.logical_keys_async().await;
        let mut result = Vec::new();
        for key in keys {
            if let Some(raw) = self.store.get_raw(&self.mk(&key)) {
                let expire_ms = decode_expire_ms(&raw);
                if expire_ms > 0 && now >= expire_ms {
                    continue;
                }
            }
            if pattern_str == "*" || pattern::is_match(&key, pattern_str) {
                result.push(key);
            }
        }
        result
    }

    /**
     * 随机返回一个键
     */
    pub fn random_key(&self) -> Option<String> {
        let keys = self.keys("*");
        if keys.is_empty() {
            return None;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let random_index = (now as usize) % keys.len();
        Some(keys[random_index].clone())
    }

    pub async fn random_key_async(&self) -> Option<String> {
        let keys = self.keys_async("*").await;
        if keys.is_empty() {
            return None;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let random_index = (now as usize) % keys.len();
        Some(keys[random_index].clone())
    }

    /**
     * 获取键值对数量
     */
    pub fn len(&self) -> usize {
        self.keys("*").len()
    }

    pub async fn len_async(&self) -> usize {
        self.keys_async("*").await.len()
    }

    /**
     * 清空所有数据
     */
    pub fn clear(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        for (key, _) in self.store.scan_prefix_raw(&prefix) {
            batch.delete(&key);
        }
        self.ttl_manager.remove_db_to_batch(&mut batch, self.db_index);
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }

    pub async fn clear_async(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        for (key, _) in self.store.scan_prefix_raw_async(&prefix).await {
            batch.delete(&key);
        }
        self.ttl_manager
            .remove_db_to_batch_async(&mut batch, self.db_index)
            .await;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }
}
