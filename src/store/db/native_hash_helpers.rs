impl Db {
    fn hash_expire_ms(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return Ok(None);
        };

        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    async fn hash_expire_ms_async(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&key_bytes).await else {
            return Ok(None);
        };
        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    fn hash_entries_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = hash_field_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect()
    }

    fn hash_live_entries_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.hash_entries_raw(key, version)
            .into_iter()
            .filter_map(|(field, value)| {
                let field_text = String::from_utf8_lossy(&field);
                self.hash_field_is_live(key, version, &field_text)
                    .then_some((field, value))
            })
            .collect()
    }

    async fn hash_live_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        for (field, value) in self.hash_entries_raw_async(key, version).await {
            let field_text = String::from_utf8_lossy(&field);
            if self
                .hash_field_is_live_async(key, version, &field_text)
                .await
            {
                entries.push((field, value));
            }
        }
        entries
    }

    fn hash_field_is_live(&self, key: &str, version: u64, field: &str) -> bool {
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let Some(raw) = self.store.get_raw(&expire_key) else {
            return true;
        };
        let Some(expire_ms) = decode_u64_be(&raw) else {
            return true;
        };
        if expire_ms == 0 || now_ms() < expire_ms {
            return true;
        }

        let mut batch = WriteBatch::new();
        batch.delete(&hash_field_key(self.db_index, key, version, field));
        batch.delete(&expire_key);
        self.write_batch_if_not_empty(&batch);
        false
    }

    async fn hash_field_is_live_async(&self, key: &str, version: u64, field: &str) -> bool {
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let Some(raw) = self.store.get_raw_async(&expire_key).await else {
            return true;
        };
        let Some(expire_ms) = decode_u64_be(&raw) else {
            return true;
        };
        if expire_ms == 0 || now_ms() < expire_ms {
            return true;
        }

        let mut batch = WriteBatch::new();
        batch.delete(&hash_field_key(self.db_index, key, version, field));
        batch.delete(&expire_key);
        self.write_batch_if_not_empty_async(&batch).await;
        false
    }

    fn hash_live_field_value(&self, key: &str, version: u64, field: &str) -> Option<Vec<u8>> {
        if !self.hash_field_is_live(key, version, field) {
            return None;
        }
        self.store
            .get_raw(&hash_field_key(self.db_index, key, version, field))
    }

    async fn hash_live_field_value_async(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> Option<Vec<u8>> {
        if !self.hash_field_is_live_async(key, version, field).await {
            return None;
        }
        self.store
            .get_raw_async(&hash_field_key(self.db_index, key, version, field))
            .await
    }

    async fn hash_live_field_observed_async(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> kv_engine::db::ObservedKvValue {
        let _ = self.hash_field_is_live_async(key, version, field).await;
        self.store
            .get_raw_observed_async(&hash_field_key(self.db_index, key, version, field))
            .await
    }

    async fn hash_entries_raw_async(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = hash_field_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect()
    }
}
