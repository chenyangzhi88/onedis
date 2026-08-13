use super::*;

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
        let _write_guard = self.set_write_lock(key).lock().await;
        let meta = self
            .list_meta_async(key)
            .await?
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
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                meta.tail,
            );
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
            self.write_batch_if_not_empty(&batch);
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            meta.head,
            storage_start,
        );
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            storage_end.saturating_add(1),
            meta.tail,
        );
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
        let _write_guard = self.set_write_lock(key).lock().await;
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let mut batch = WriteBatch::new();
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                meta.tail,
            );
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
            self.write_batch_if_not_empty_async(&batch).await;
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            meta.head,
            storage_start,
        );
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            storage_end.saturating_add(1),
            meta.tail,
        );
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
        let _write_guard = self.set_write_lock(key).lock().await;
        let meta = match self.list_meta_async(key).await? {
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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

/// Delete the half-open logical index interval `[start, end)` with at most two range tombstones.
/// Signed big-endian indices place non-negative values before negative values in byte order, so a
/// range crossing zero must be split at that boundary.
fn delete_list_storage_range_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    start: i64,
    end: i64,
) {
    if start >= end {
        return;
    }
    if start < 0 {
        let negative_end = end.min(0);
        let lower = list_item_key(db_index, key, version, start);
        let upper = if negative_end == 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(db_index, key, version))
                .expect("list item prefix has an exclusive upper bound")
        } else {
            list_item_key(db_index, key, version, negative_end)
        };
        batch.delete_range(&lower, &upper);
    }
    if end > 0 {
        let positive_start = start.max(0);
        let lower = list_item_key(db_index, key, version, positive_start);
        let upper = list_item_key(db_index, key, version, end);
        batch.delete_range(&lower, &upper);
    }
}
