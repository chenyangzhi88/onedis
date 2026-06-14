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
        self.ttl_manager
            .remove_db_to_batch(&mut batch, self.db_index);
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

    pub(crate) fn move_key_between_dbs(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        );
        Self::delete_structure_for_db_to_batch(
            &mut batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw)) {
            if header.expire_ms > 0 {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
        }
        store.write_batch(&batch);
        Ok(true)
    }

    #[allow(dead_code)]
    pub(crate) async fn move_key_between_dbs_async(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch_async(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(
            &mut batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw)) {
            if header.expire_ms > 0 {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
        }
        store.write_batch(&batch);
        Ok(true)
    }

    pub(crate) fn copy_key_between_dbs(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        replace: bool,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };
        let target_raw =
            Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key);
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                target_db_index,
                target_key,
                target_raw,
            );
            if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(target_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    target_db_index,
                    target_key,
                );
            }
        }
        Self::copy_structure_between_dbs_to_batch(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
        }
        store.write_batch(&batch);
        Ok(true)
    }

    pub(crate) async fn copy_key_between_dbs_async(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        replace: bool,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };
        let target_raw =
            Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key);
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                target_db_index,
                target_key,
                target_raw,
            );
            if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(target_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    target_db_index,
                    target_key,
                );
            }
        }
        Self::copy_structure_between_dbs_to_batch_async(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        )
        .await;
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
        }
        store.write_batch(&batch);
        Ok(true)
    }

    pub fn move_key_to_db(&self, target_db_index: u16, key: &str) -> Result<bool, Error> {
        if self.db_index == target_db_index {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(&self.store, target_db_index, key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            target_db_index,
            key,
            &source_raw,
            &self.version_counter,
        );
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
            && header.expire_ms > 0
        {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            self.ttl_manager
                .add_to_batch(&mut batch, header.expire_ms, target_db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }

    pub async fn move_key_to_db_async(
        &self,
        target_db_index: u16,
        key: &str,
    ) -> Result<bool, Error> {
        if self.db_index == target_db_index {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(&self.store, target_db_index, key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch_async(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            target_db_index,
            key,
            &source_raw,
            &self.version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
            && header.expire_ms > 0
        {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            self.ttl_manager
                .add_to_batch(&mut batch, header.expire_ms, target_db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }

    pub async fn rename_key_async(
        &self,
        old_key: &str,
        new_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        if old_key == new_key {
            if Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, old_key)
                .is_some()
            {
                return Ok(true);
            }
            return Err(Error::msg("ERR no such key"));
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, old_key)
        else {
            return Err(Error::msg("ERR no such key"));
        };
        let target_raw =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, new_key);
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, new_key, target_raw);
            if let Some(header) = decode_meta_header(target_raw)
                && header.expire_ms > 0
            {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    new_key,
                );
            }
        }
        Self::copy_structure_between_dbs_to_batch_async(
            &self.store,
            &mut batch,
            self.db_index,
            old_key,
            self.db_index,
            new_key,
            &source_raw,
            &self.version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, old_key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
            && header.expire_ms > 0
        {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                old_key,
            );
            self.ttl_manager
                .add_to_batch(&mut batch, header.expire_ms, self.db_index, new_key);
        }
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    pub fn copy_key_to_db(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        Self::copy_key_between_dbs(
            &self.store,
            self.db_index,
            source_key,
            target_db_index,
            target_key,
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
    }

    pub async fn copy_key_to_db_async(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        Self::copy_key_between_dbs_async(
            &self.store,
            self.db_index,
            source_key,
            target_db_index,
            target_key,
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
        .await
    }

}
