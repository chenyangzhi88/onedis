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
        (batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        ))
        .expect("write batch append invariant violated");
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
        (batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        ))
        .expect("write batch append invariant violated");
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
        (batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        ))
        .expect("write batch append invariant violated");
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
        (batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        ))
        .expect("write batch append invariant violated");
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
        let items = self.list_range_raw_values(key, meta.version, meta.head, meta.tail - 1);
        let (removed, batch, updated) =
            self.build_list_remove_batch(key, meta, &items, count, element);
        if removed == 0 {
            return Ok(0);
        }
        self.write_batch_if_not_empty(&batch);
        if let Some(updated) = updated {
            self.cache_list_meta_if_non_transactional(key, updated);
        } else {
            self.remove_list_meta_cache_if_non_transactional(key);
        }
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
        let len = (meta.tail - meta.head) as usize;
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let target = element.as_bytes();
        let mut position = if count < 0 { len } else { 0 };
        let mut matches = Vec::new();
        if count < 0 {
            self.list_range_raw_values_visit_reverse_async(
                key,
                meta.version,
                meta.head,
                meta.tail - 1,
                |value| {
                    if matches.len() == limit {
                        return false;
                    }
                    position -= 1;
                    if value == target {
                        matches.push(position);
                    }
                    matches.len() != limit
                },
            )
            .await;
            matches.reverse();
        } else {
            self.list_range_raw_values_visit_chunked_async(
                key,
                meta.version,
                meta.head,
                meta.tail - 1,
                4096,
                |value| {
                    if matches.len() == limit {
                        return false;
                    }
                    let current = position;
                    position += 1;
                    if value == target {
                        matches.push(current);
                    }
                    matches.len() != limit
                },
            )
            .await;
        }
        let removed = matches.len();
        if removed == 0 {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        if removed == len {
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                meta.tail,
            );
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
            self.write_existing_version_batch_if_not_empty_async(&batch)
                .await;
            self.remove_list_meta_cache_if_non_transactional(key);
            self.changes.fetch_add(1, Ordering::Relaxed);
            return Ok(removed);
        }

        let first_removed = matches[0];
        let last_removed = *matches
            .last()
            .expect("removed list item has a last position");
        let left_moves = len - first_removed - removed;
        let right_moves = last_removed + 1 - removed;
        let mut updated = meta;
        if left_moves <= right_moves {
            let scan_start = meta.head + first_removed as i64;
            let mut rewrite = ListRemoveRewriteState {
                batch,
                db_index: self.db_index,
                key: key.to_string(),
                version: meta.version,
                head: meta.head,
                position: first_removed,
                destination: scan_start,
                matches,
            };
            self.list_range_raw_values_visit_async(
                key,
                meta.version,
                scan_start,
                meta.tail - 1,
                |value| write_list_remove_survivor(&mut rewrite, value),
            )
            .await;
            batch = rewrite.batch;
            updated.tail -= removed as i64;
            debug_assert_eq!(rewrite.destination, updated.tail);
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                updated.tail,
                meta.tail,
            );
        } else {
            let source_end = meta.head + last_removed as i64;
            let values = self
                .list_range_raw_values_async(key, meta.version, meta.head, source_end)
                .await;
            let mut destination = source_end + 1;
            let mut removed_cursor = matches.len();
            for position in (0..=last_removed).rev() {
                if removed_cursor > 0 && matches[removed_cursor - 1] == position {
                    removed_cursor -= 1;
                    continue;
                }
                destination -= 1;
                let source = meta.head + position as i64;
                if source != destination {
                    (batch.put(
                        &list_item_key(self.db_index, key, meta.version, destination),
                        &values[position],
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            updated.head += removed as i64;
            debug_assert_eq!(destination, updated.head);
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                updated.head,
            );
        }
        (batch.put(
            &self.mk(key),
            &encode_list_meta(
                updated.expire_ms,
                updated.version,
                updated.head,
                updated.tail,
            ),
        ))
        .expect("write batch append invariant violated");
        self.write_existing_version_batch_if_not_empty_async(&batch)
            .await;
        self.cache_list_meta_if_non_transactional(key, updated);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

    /// Compact toward the cheaper end after marking Redis' direction-sensitive matches. This
    /// leaves the list contiguous while rewriting only the values displaced by a removed item.
    fn build_list_remove_batch(
        &self,
        key: &str,
        meta: ListMeta,
        items: &[Vec<u8>],
        count: i64,
        element: &str,
    ) -> (usize, WriteBatch, Option<ListMeta>) {
        let mut removed_positions = vec![false; items.len()];
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let target = element.as_bytes();
        let mut removed = 0usize;
        if count >= 0 {
            for (position, item) in items.iter().enumerate() {
                if removed == limit {
                    break;
                }
                if item.as_slice() == target {
                    removed_positions[position] = true;
                    removed += 1;
                }
            }
        } else {
            for position in (0..items.len()).rev() {
                if removed == limit {
                    break;
                }
                if items[position].as_slice() == target {
                    removed_positions[position] = true;
                    removed += 1;
                }
            }
        }
        let mut batch = WriteBatch::new();
        if removed == 0 {
            return (0, batch, Some(meta));
        }
        if removed == items.len() {
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                meta.tail,
            );
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
            return (removed, batch, None);
        }

        let first_removed = removed_positions
            .iter()
            .position(|removed| *removed)
            .expect("a removed list item has a first position");
        let last_removed = removed_positions
            .iter()
            .rposition(|removed| *removed)
            .expect("a removed list item has a last position");
        let left_moves = removed_positions[first_removed..]
            .iter()
            .filter(|removed| !**removed)
            .count();
        let right_moves = removed_positions[..=last_removed]
            .iter()
            .filter(|removed| !**removed)
            .count();
        let mut updated = meta;
        if left_moves <= right_moves {
            let mut destination = meta.head;
            for (position, item) in items.iter().enumerate() {
                if removed_positions[position] {
                    continue;
                }
                let source = meta.head + position as i64;
                if source != destination {
                    (batch.put(
                        &list_item_key(self.db_index, key, meta.version, destination),
                        item,
                    ))
                    .expect("write batch append invariant violated");
                }
                destination += 1;
            }
            updated.tail = destination;
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                updated.tail,
                meta.tail,
            );
        } else {
            updated.head = meta.tail - (items.len() - removed) as i64;
            let mut destination = meta.tail;
            for position in (0..items.len()).rev() {
                if removed_positions[position] {
                    continue;
                }
                destination -= 1;
                let source = meta.head + position as i64;
                if source != destination {
                    (batch.put(
                        &list_item_key(self.db_index, key, meta.version, destination),
                        &items[position],
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            debug_assert_eq!(destination, updated.head);
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                key,
                meta.version,
                meta.head,
                updated.head,
            );
        }
        (batch.put(
            &self.mk(key),
            &encode_list_meta(
                updated.expire_ms,
                updated.version,
                updated.head,
                updated.tail,
            ),
        ))
        .expect("write batch append invariant violated");
        (removed, batch, Some(updated))
    }
}

struct ListRemoveRewriteState {
    batch: WriteBatch,
    db_index: u16,
    key: String,
    version: u64,
    head: i64,
    position: usize,
    destination: i64,
    matches: Vec<usize>,
}

fn write_list_remove_survivor(state: &mut ListRemoveRewriteState, value: &[u8]) -> bool {
    let position = state.position;
    let source = state.head + position as i64;
    state.position += 1;
    if state.matches.binary_search(&position).is_ok() {
        return true;
    }
    if source != state.destination {
        (state.batch.put(
            &list_item_key(state.db_index, &state.key, state.version, state.destination),
            value,
        ))
        .expect("write batch append invariant violated");
    }
    state.destination += 1;
    true
}

/// Delete the half-open logical index interval `[start, end)` with at most two range tombstones.
/// Signed big-endian indices place non-negative values before negative values in byte order, so a
/// range crossing zero must be split at that boundary.
pub(in crate::store::db) fn delete_list_storage_range_to_batch(
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
        (batch.delete_range(&lower, &upper)).expect("write batch append invariant violated");
    }
    if end > 0 {
        let positive_start = start.max(0);
        let lower = list_item_key(db_index, key, version, positive_start);
        let upper = list_item_key(db_index, key, version, end);
        (batch.delete_range(&lower, &upper)).expect("write batch append invariant violated");
    }
}
