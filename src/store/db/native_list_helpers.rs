use super::*;

impl Db {
    pub(in crate::store::db) fn list_meta(&self, key: &str) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional() {
            if let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry) {
                if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                    return Ok(Some(meta));
                }
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    meta.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty(&batch);
                self.list_meta_cache.remove(&key_bytes);
                return Ok(None);
            }
        }
        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return Ok(None);
        };
        if let Some(header) = decode_meta_header(&raw) {
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                    header.type_tag,
                );
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty(&batch);
                return Ok(None);
            }
        }

        if let Some(meta) = decode_list_meta(&raw) {
            self.cache_list_meta_if_non_transactional(key, meta);
            return Ok(Some(meta));
        }

        let Some((_, version, structure)) = decode_entry(&raw) else {
            return Err(Error::msg("Failed to decode list metadata"));
        };
        match structure {
            Structure::List(list) => {
                let meta = ListMeta {
                    expire_ms: decode_expire_ms(&raw),
                    version,
                    head: 0,
                    tail: list.len() as i64,
                };
                self.cache_list_meta_if_non_transactional(key, meta);
                Ok(Some(meta))
            }
            _ => Err(Error::msg(WRONG_TYPE_ERROR)),
        }
    }

    pub(in crate::store::db) async fn list_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional() {
            if let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry) {
                if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                    return Ok(Some(meta));
                }
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    meta.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty_async(&batch).await;
                self.list_meta_cache.remove(&key_bytes);
                return Ok(None);
            }
        }
        let Some(raw) = self.store.get_raw_async(&key_bytes).await else {
            return Ok(None);
        };
        if let Some(header) = decode_meta_header(&raw) {
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                    header.type_tag,
                );
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty_async(&batch).await;
                return Ok(None);
            }
        }

        if let Some(meta) = decode_list_meta(&raw) {
            self.cache_list_meta_if_non_transactional(key, meta);
            return Ok(Some(meta));
        }

        let Some((_, version, structure)) = decode_entry(&raw) else {
            return Err(Error::msg("Failed to decode list metadata"));
        };
        match structure {
            Structure::List(list) => {
                let meta = ListMeta {
                    expire_ms: decode_expire_ms(&raw),
                    version,
                    head: 0,
                    tail: list.len() as i64,
                };
                self.cache_list_meta_if_non_transactional(key, meta);
                Ok(Some(meta))
            }
            _ => Err(Error::msg(WRONG_TYPE_ERROR)),
        }
    }

    pub(in crate::store::db) fn resolve_list_index(
        &self,
        meta: ListMeta,
        index: i64,
    ) -> Option<i64> {
        let len = meta.tail - meta.head;
        if len <= 0 {
            return None;
        }

        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            return None;
        }

        Some(meta.head + normalized)
    }

    pub(in crate::store::db) fn resolve_list_range(
        &self,
        meta: ListMeta,
        start: i64,
        stop: i64,
    ) -> Option<(i64, i64)> {
        let len = meta.tail - meta.head;
        if len <= 0 {
            return None;
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };

        normalized_start = normalized_start.max(0);
        normalized_stop = normalized_stop.min(len - 1);

        if normalized_start > normalized_stop || normalized_start >= len || normalized_stop < 0 {
            return None;
        }

        Some((meta.head + normalized_start, meta.head + normalized_stop))
    }

    pub(in crate::store::db) fn list_range_raw_values(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
    ) -> Vec<Vec<u8>> {
        let len = (storage_end - storage_start + 1) as usize;
        let mut values = Vec::with_capacity(len);
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            self.append_list_range_raw_values(
                key,
                version,
                storage_start,
                negative_end,
                len.saturating_sub(values.len()),
                &mut values,
            );
        }
        if storage_end >= 0 {
            let positive_start = storage_start.max(0);
            self.append_list_range_raw_values(
                key,
                version,
                positive_start,
                storage_end,
                len.saturating_sub(values.len()),
                &mut values,
            );
        }
        values
    }

    pub(in crate::store::db) async fn list_range_raw_values_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
    ) -> Vec<Vec<u8>> {
        let len = (storage_end - storage_start + 1) as usize;
        let mut values = Vec::with_capacity(len);
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            self.append_list_range_raw_values_async(
                key,
                version,
                storage_start,
                negative_end,
                len.saturating_sub(values.len()),
                &mut values,
            )
            .await;
        }
        if storage_end >= 0 {
            let positive_start = storage_start.max(0);
            self.append_list_range_raw_values_async(
                key,
                version,
                positive_start,
                storage_end,
                len.saturating_sub(values.len()),
                &mut values,
            )
            .await;
        }
        values
    }

    pub(in crate::store::db) async fn list_range_raw_values_visit_async<F>(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8]) -> bool + Send,
    {
        let len = (storage_end - storage_start + 1) as usize;
        let mut seen = 0usize;
        let mut visitor = visitor;
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            seen += self
                .append_list_range_raw_values_visit_async(
                    key,
                    version,
                    storage_start,
                    negative_end,
                    len.saturating_sub(seen),
                    &mut visitor,
                )
                .await;
        }
        if storage_end >= 0 && seen < len {
            let positive_start = storage_start.max(0);
            seen += self
                .append_list_range_raw_values_visit_async(
                    key,
                    version,
                    positive_start,
                    storage_end,
                    len.saturating_sub(seen),
                    &mut visitor,
                )
                .await;
        }
        seen
    }

    pub(in crate::store::db) fn append_list_range_raw_values(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        values: &mut Vec<Vec<u8>>,
    ) {
        if storage_start > storage_end || limit == 0 {
            return;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        values.extend(
            self.store
                .scan_range_raw_limited(&lower_bound, upper_bound, limit)
                .into_iter()
                .map(|(_, value)| value),
        );
    }

    pub(in crate::store::db) async fn append_list_range_raw_values_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        values: &mut Vec<Vec<u8>>,
    ) {
        if storage_start > storage_end || limit == 0 {
            return;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        values.extend(
            self.store
                .scan_range_raw_limited_async(&lower_bound, upper_bound, limit)
                .await
                .into_iter()
                .map(|(_, value)| value),
        );
    }

    pub(in crate::store::db) async fn append_list_range_raw_values_visit_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        visitor: &mut (dyn FnMut(&[u8]) -> bool + Send),
    ) -> usize {
        if storage_start > storage_end || limit == 0 {
            return 0;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return 0;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        self.store
            .scan_range_raw_visit_async(&lower_bound, upper_bound, limit, |_, value| visitor(value))
            .await
    }
}
