impl Db {
    /// 左侧出队。
    pub fn list_pop_left(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        let item_key = list_item_key(self.db_index, key, meta.version, meta.head);
        let value = self
            .store
            .get_raw(&item_key)
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        meta.head += 1;
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty(&batch);
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    pub async fn list_pop_left_async(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        let item_key = list_item_key(self.db_index, key, meta.version, meta.head);
        let value = self
            .store
            .get_raw_async(&item_key)
            .await
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        meta.head += 1;
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    /// 右侧出队。
    pub fn list_pop_right(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        meta.tail -= 1;
        let item_key = list_item_key(self.db_index, key, meta.version, meta.tail);
        let value = self
            .store
            .get_raw(&item_key)
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty(&batch);
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    pub async fn list_pop_right_async(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        meta.tail -= 1;
        let item_key = list_item_key(self.db_index, key, meta.version, meta.tail);
        let value = self
            .store
            .get_raw_async(&item_key)
            .await
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }
}
