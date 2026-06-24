impl Db {
    /// 设置指定下标的元素。
    pub fn list_set(&self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        let meta = self
            .list_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        let storage_index = self
            .resolve_list_index(meta, index)
            .ok_or_else(|| Error::msg("ERR index out of range"))?;

        let mut batch = WriteBatch::new();
        batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        );
        self.write_batch_if_not_empty(&batch);
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn list_set_async(&self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        let meta = self
            .list_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        let storage_index = self
            .resolve_list_index(meta, index)
            .ok_or_else(|| Error::msg("ERR index out of range"))?;

        let mut batch = WriteBatch::new();
        batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// 保留指定范围，其余元素删除。
    pub fn list_trim(&self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let mut batch = WriteBatch::new();
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            for storage_index in meta.head..meta.tail {
                batch.delete(&list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    storage_index,
                ));
            }
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        for storage_index in meta.head..storage_start {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for storage_index in (storage_end + 1)..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        );
        self.write_batch_if_not_empty(&batch);
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn list_trim_async(&self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let mut batch = WriteBatch::new();
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            for storage_index in meta.head..meta.tail {
                batch.delete(&list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    storage_index,
                ));
            }
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        for storage_index in meta.head..storage_start {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for storage_index in (storage_end + 1)..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn list_remove(&self, key: &str, count: i64, element: &str) -> Result<usize, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self.list_range(key, 0, -1)?;
        let mut removed = 0usize;
        let mut keep = Vec::with_capacity(items.len());
        if count >= 0 {
            let limit = if count == 0 {
                usize::MAX
            } else {
                count as usize
            };
            for item in items {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    keep.push(item);
                }
            }
        } else {
            let limit = count.unsigned_abs() as usize;
            let mut rev_keep = Vec::with_capacity(items.len());
            for item in items.into_iter().rev() {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    rev_keep.push(item);
                }
            }
            keep = rev_keep.into_iter().rev().collect();
        }
        if removed == 0 {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        if keep.is_empty() {
            batch.delete(&self.mk(key));
        } else {
            for (index, value) in keep.iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, index as i64),
                    value.as_bytes(),
                );
            }
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, 0, keep.len() as i64),
            );
        }
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

    pub async fn list_remove_async(
        &self,
        key: &str,
        count: i64,
        element: &str,
    ) -> Result<usize, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self.list_range_async(key, 0, -1).await?;
        let mut removed = 0usize;
        let mut keep = Vec::with_capacity(items.len());
        if count >= 0 {
            let limit = if count == 0 {
                usize::MAX
            } else {
                count as usize
            };
            for item in items {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    keep.push(item);
                }
            }
        } else {
            let limit = count.unsigned_abs() as usize;
            let mut rev_keep = Vec::with_capacity(items.len());
            for item in items.into_iter().rev() {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    rev_keep.push(item);
                }
            }
            keep = rev_keep.into_iter().rev().collect();
        }
        if removed == 0 {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        if keep.is_empty() {
            batch.delete(&self.mk(key));
        } else {
            for (index, value) in keep.iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, index as i64),
                    value.as_bytes(),
                );
            }
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, 0, keep.len() as i64),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }
}
