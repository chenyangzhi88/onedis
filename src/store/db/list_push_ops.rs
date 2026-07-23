use super::*;

impl Db {
    pub fn list_push_left(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_left_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_left_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_persisted_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            meta.head -= 1;
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.head),
                value,
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_left_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let _guard = self.set_write_lock(key).lock().await;
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value().map(|value| value.to_vec());
            let mut meta = match raw_meta.as_deref() {
                Some(raw) => {
                    if let Some(meta) = decode_list_meta(raw) {
                        meta
                    } else {
                        let Some(header) = decode_meta_header(raw) else {
                            return Err(Error::msg("Failed to decode list metadata"));
                        };
                        if header.type_tag != TYPE_LIST {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        }
                        return Err(Error::msg("Failed to decode list metadata"));
                    }
                }
                None if only_if_exists => return Ok(0),
                None => ListMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version_async().await,
                    head: 0,
                    tail: 0,
                },
            };
            let mut batch = WriteBatch::new();
            for value in values {
                meta.head -= 1;
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, meta.head),
                    value,
                );
            }
            batch.put(
                &key_bytes,
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
            let len = (meta.tail - meta.head) as usize;
            let condition = CompareCondition::from_observed(&observed_meta);
            if self
                .compare_and_write_batch_if_not_empty_async(&[condition], &batch)
                .await?
            {
                self.cache_list_meta_if_non_transactional(key, meta);
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(len);
            }
        }
        Err(Error::msg("ERR list write conflict"))
    }

    /// 右侧批量入队。
    pub fn list_push_right(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_right_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_right_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_persisted_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.tail),
                value,
            );
            meta.tail += 1;
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_right_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let _guard = self.set_write_lock(key).lock().await;
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value().map(|value| value.to_vec());
            let mut meta = match raw_meta.as_deref() {
                Some(raw) => {
                    if let Some(meta) = decode_list_meta(raw) {
                        meta
                    } else {
                        let Some(header) = decode_meta_header(raw) else {
                            return Err(Error::msg("Failed to decode list metadata"));
                        };
                        if header.type_tag != TYPE_LIST {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        }
                        return Err(Error::msg("Failed to decode list metadata"));
                    }
                }
                None if only_if_exists => return Ok(0),
                None => ListMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version_async().await,
                    head: 0,
                    tail: 0,
                },
            };
            let mut batch = WriteBatch::new();
            for value in values {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, meta.tail),
                    value,
                );
                meta.tail += 1;
            }
            batch.put(
                &key_bytes,
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
            let len = (meta.tail - meta.head) as usize;
            let condition = CompareCondition::from_observed(&observed_meta);
            if self
                .compare_and_write_batch_if_not_empty_async(&[condition], &batch)
                .await?
            {
                self.cache_list_meta_if_non_transactional(key, meta);
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(len);
            }
        }
        Err(Error::msg("ERR list write conflict"))
    }
}
