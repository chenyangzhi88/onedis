use super::*;

impl Db {
    pub(in crate::store::db) fn promote_packed_list(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(items) = decode_packed_list(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode list metadata"))?;
            let version = self.next_version();
            let mut batch = WriteBatch::new();
            batch.put(
                &key_bytes,
                &encode_list_meta(header.expire_ms, version, 0, items.len() as i64),
            )?;
            for (index, item) in items.into_iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, version, index as i64),
                    &item,
                )?;
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                return Ok(());
            }
        }
        Err(Error::msg("ERR list layout promotion conflict"))
    }

    pub(in crate::store::db) async fn promote_packed_list_async(
        &self,
        key: &str,
    ) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(items) = decode_packed_list(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode list metadata"))?;
            let version = self.next_version_async().await;
            let mut batch = WriteBatch::new();
            batch.put(
                &key_bytes,
                &encode_list_meta(header.expire_ms, version, 0, items.len() as i64),
            )?;
            for (index, item) in items.into_iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, version, index as i64),
                    &item,
                )?;
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::msg("ERR list layout promotion conflict"))
    }

    pub(in crate::store::db) fn list_meta(&self, key: &str) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional()
            && let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry)
        {
            if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                return Ok(Some(meta));
            }
            self.list_meta_cache.remove(&key_bytes);
        }

        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value().map(|value| value.as_ref()) else {
                return Ok(None);
            };
            if let Some(header) = decode_meta_header(raw)
                && header.expire_ms > 0
                && now_ms() >= header.expire_ms
            {
                let mut batch = WriteBatch::new();
                (batch.delete(&key_bytes)).expect("write batch append invariant violated");
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
                if self.compare_and_write_batch_if_not_empty(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )? {
                    self.list_meta_cache.remove(&key_bytes);
                    return Ok(None);
                }
                continue;
            }

            if let Some(meta) = decode_list_meta(raw) {
                self.cache_list_meta_if_non_transactional(key, meta);
                return Ok(Some(meta));
            }

            let Some((_, version, structure)) = decode_entry(raw) else {
                return Err(Error::msg("Failed to decode list metadata"));
            };
            match structure {
                Structure::List(list) => {
                    let meta = ListMeta {
                        expire_ms: decode_expire_ms(raw),
                        version,
                        head: 0,
                        tail: list.len() as i64,
                    };
                    self.cache_list_meta_if_non_transactional(key, meta);
                    return Ok(Some(meta));
                }
                _ => return Err(Error::msg(WRONG_TYPE_ERROR)),
            }
        }
        Err(Error::msg("ERR list metadata read conflict"))
    }

    pub(in crate::store::db) async fn list_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional()
            && let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry)
        {
            if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                return Ok(Some(meta));
            }
            self.list_meta_cache.remove(&key_bytes);
        }

        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value().map(|value| value.as_ref()) else {
                return Ok(None);
            };
            if let Some(header) = decode_meta_header(raw)
                && header.expire_ms > 0
                && now_ms() >= header.expire_ms
            {
                let mut batch = WriteBatch::new();
                (batch.delete(&key_bytes)).expect("write batch append invariant violated");
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
                if self
                    .compare_and_write_batch_if_not_empty_async(
                        &[CompareCondition::from_observed(&observed)],
                        &batch,
                    )
                    .await?
                {
                    self.list_meta_cache.remove(&key_bytes);
                    return Ok(None);
                }
                continue;
            }

            if let Some(meta) = decode_list_meta(raw) {
                self.cache_list_meta_if_non_transactional(key, meta);
                return Ok(Some(meta));
            }

            let Some((_, version, structure)) = decode_entry(raw) else {
                return Err(Error::msg("Failed to decode list metadata"));
            };
            match structure {
                Structure::List(list) => {
                    let meta = ListMeta {
                        expire_ms: decode_expire_ms(raw),
                        version,
                        head: 0,
                        tail: list.len() as i64,
                    };
                    self.cache_list_meta_if_non_transactional(key, meta);
                    return Ok(Some(meta));
                }
                _ => return Err(Error::msg(WRONG_TYPE_ERROR)),
            }
        }
        Err(Error::msg("ERR list metadata read conflict"))
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
        if version == 0 {
            let items = self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_list)
                .unwrap_or_default();
            let start = storage_start.max(0) as usize;
            let end = storage_end.max(-1).saturating_add(1) as usize;
            return items
                .get(start..end.min(items.len()))
                .unwrap_or_default()
                .to_vec();
        }
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
        if version == 0 {
            let items = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_list)
                .unwrap_or_default();
            let start = storage_start.max(0) as usize;
            let end = storage_end.max(-1).saturating_add(1) as usize;
            return items
                .get(start..end.min(items.len()))
                .unwrap_or_default()
                .to_vec();
        }
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
        self.list_range_raw_values_visit_chunked_async(
            key,
            version,
            storage_start,
            storage_end,
            1024,
            visitor,
        )
        .await
    }

    pub(in crate::store::db) async fn list_range_raw_values_visit_chunked_async<F>(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        batch_size: usize,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8]) -> bool + Send,
    {
        if version == 0 {
            let values = self
                .list_range_raw_values_async(key, version, storage_start, storage_end)
                .await;
            let mut visitor = visitor;
            let mut seen = 0usize;
            for value in values {
                seen += 1;
                if !visitor(&value) {
                    break;
                }
            }
            return seen;
        }
        let len = (storage_end - storage_start + 1) as usize;
        let mut seen = 0usize;
        let mut visitor = visitor;
        let mut keep_scanning = true;
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            let lower_bound = list_item_key(self.db_index, key, version, storage_start);
            let upper_bound = if negative_end < -1 {
                Some(list_item_key(self.db_index, key, version, negative_end + 1))
            } else {
                prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
            };
            seen += self
                .store
                .scan_range_raw_visit_chunked_async(
                    &lower_bound,
                    upper_bound,
                    len.saturating_sub(seen),
                    batch_size,
                    |_, value| {
                        keep_scanning = visitor(value);
                        keep_scanning
                    },
                )
                .await;
        }
        if keep_scanning && storage_end >= 0 && seen < len {
            let positive_start = storage_start.max(0);
            let lower_bound = list_item_key(self.db_index, key, version, positive_start);
            let upper_bound = if storage_end == i64::MAX {
                prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
            } else {
                Some(list_item_key(self.db_index, key, version, storage_end + 1))
            };
            seen += self
                .store
                .scan_range_raw_visit_chunked_async(
                    &lower_bound,
                    upper_bound,
                    len.saturating_sub(seen),
                    batch_size,
                    |_, value| visitor(value),
                )
                .await;
        }
        seen
    }

    pub(in crate::store::db) async fn list_range_raw_values_visit_reverse_async<F>(
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
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut keep_scanning = true;
        if storage_end >= 0 {
            let positive_start = storage_start.max(0);
            let lower_bound = list_item_key(self.db_index, key, version, positive_start);
            let upper_bound = if storage_end == i64::MAX {
                prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
            } else {
                Some(list_item_key(self.db_index, key, version, storage_end + 1))
            };
            seen += self
                .store
                .scan_range_raw_visit_reverse_async(
                    &lower_bound,
                    upper_bound,
                    len.saturating_sub(seen),
                    |_, value| {
                        keep_scanning = visitor(value);
                        keep_scanning
                    },
                )
                .await;
        }
        if keep_scanning && storage_start < 0 && seen < len {
            let negative_end = storage_end.min(-1);
            let lower_bound = list_item_key(self.db_index, key, version, storage_start);
            let upper_bound = if negative_end < -1 {
                Some(list_item_key(self.db_index, key, version, negative_end + 1))
            } else {
                prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
            };
            seen += self
                .store
                .scan_range_raw_visit_reverse_async(
                    &lower_bound,
                    upper_bound,
                    len.saturating_sub(seen),
                    |_, value| visitor(value),
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
}
