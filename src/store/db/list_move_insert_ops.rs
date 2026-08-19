use super::*;

impl Db {
    pub fn list_move(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
        self.promote_packed_list(source)?;
        if source != destination {
            self.promote_packed_list(destination)?;
        }
        let Some(mut source_meta) = self.list_meta(source)? else {
            return Ok(None);
        };
        if source_meta.head >= source_meta.tail {
            return Ok(None);
        }
        let mut destination_meta = if source == destination {
            source_meta
        } else {
            self.list_meta(destination)?.unwrap_or(ListMeta {
                expire_ms: 0,
                version: self.next_version(),
                head: 0,
                tail: 0,
            })
        };
        let source_index = if source_left {
            source_meta.head
        } else {
            source_meta.tail - 1
        };
        let Some(raw_value) = self.store.get_raw(&list_item_key(
            self.db_index,
            source,
            source_meta.version,
            source_index,
        )) else {
            return Ok(None);
        };
        let value = String::from_utf8(raw_value)
            .map_err(|_| Error::msg("ERR list element is not valid UTF-8"))?;

        let mut batch = WriteBatch::new();
        (batch.delete(&list_item_key(
            self.db_index,
            source,
            source_meta.version,
            source_index,
        )))
        .expect("write batch append invariant violated");
        if source_left {
            source_meta.head += 1;
        } else {
            source_meta.tail -= 1;
        }

        if source == destination {
            destination_meta = source_meta;
        } else if source_meta.head >= source_meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, source, source_meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(source),
                &encode_list_meta(
                    source_meta.expire_ms,
                    source_meta.version,
                    source_meta.head,
                    source_meta.tail,
                ),
            ))
            .expect("write batch append invariant violated");
        }

        let destination_index = if destination_left {
            destination_meta.head -= 1;
            destination_meta.head
        } else {
            let index = destination_meta.tail;
            destination_meta.tail += 1;
            index
        };
        (batch.put(
            &list_item_key(
                self.db_index,
                destination,
                destination_meta.version,
                destination_index,
            ),
            value.as_bytes(),
        ))
        .expect("write batch append invariant violated");
        (batch.put(
            &self.mk(destination),
            &encode_list_meta(
                destination_meta.expire_ms,
                destination_meta.version,
                destination_meta.head,
                destination_meta.tail,
            ),
        ))
        .expect("write batch append invariant violated");
        self.write_batch_if_not_empty(&batch);
        if source != destination {
            if source_meta.head >= source_meta.tail {
                self.remove_list_meta_cache_if_non_transactional(source);
            } else {
                self.cache_list_meta_if_non_transactional(source, source_meta);
            }
        }
        self.cache_list_meta_if_non_transactional(destination, destination_meta);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(Some(value))
    }

    pub async fn list_move_async(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
        let source_shard = key_write_lock_shard(self.db_index, source);
        let destination_shard = key_write_lock_shard(self.db_index, destination);
        if source_shard == destination_shard {
            let _guard = self.key_write_locks[source_shard].lock().await;
            self.list_move_async_unlocked(source, destination, source_left, destination_left)
                .await
        } else if source_shard < destination_shard {
            let _source_guard = self.key_write_locks[source_shard].lock().await;
            let _destination_guard = self.key_write_locks[destination_shard].lock().await;
            self.list_move_async_unlocked(source, destination, source_left, destination_left)
                .await
        } else {
            let _destination_guard = self.key_write_locks[destination_shard].lock().await;
            let _source_guard = self.key_write_locks[source_shard].lock().await;
            self.list_move_async_unlocked(source, destination, source_left, destination_left)
                .await
        }
    }

    async fn list_move_async_unlocked(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
        self.promote_packed_list_async(source).await?;
        if source != destination {
            self.promote_packed_list_async(destination).await?;
        }
        let Some(mut source_meta) = self.list_meta_async(source).await? else {
            return Ok(None);
        };
        if source_meta.head >= source_meta.tail {
            return Ok(None);
        }
        let mut destination_meta = if source == destination {
            source_meta
        } else {
            self.list_meta_async(destination)
                .await?
                .unwrap_or(ListMeta {
                    expire_ms: 0,
                    version: self.next_version_async().await,
                    head: 0,
                    tail: 0,
                })
        };
        let source_index = if source_left {
            source_meta.head
        } else {
            source_meta.tail - 1
        };
        let Some(raw_value) = self
            .store
            .get_raw_async(&list_item_key(
                self.db_index,
                source,
                source_meta.version,
                source_index,
            ))
            .await
        else {
            return Ok(None);
        };
        let value = String::from_utf8(raw_value)
            .map_err(|_| Error::msg("ERR list element is not valid UTF-8"))?;

        let mut batch = WriteBatch::new();
        (batch.delete(&list_item_key(
            self.db_index,
            source,
            source_meta.version,
            source_index,
        )))
        .expect("write batch append invariant violated");
        if source_left {
            source_meta.head += 1;
        } else {
            source_meta.tail -= 1;
        }

        if source == destination {
            destination_meta = source_meta;
        } else if source_meta.head >= source_meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, source, source_meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(source),
                &encode_list_meta(
                    source_meta.expire_ms,
                    source_meta.version,
                    source_meta.head,
                    source_meta.tail,
                ),
            ))
            .expect("write batch append invariant violated");
        }

        let destination_index = if destination_left {
            destination_meta.head -= 1;
            destination_meta.head
        } else {
            let index = destination_meta.tail;
            destination_meta.tail += 1;
            index
        };
        (batch.put(
            &list_item_key(
                self.db_index,
                destination,
                destination_meta.version,
                destination_index,
            ),
            value.as_bytes(),
        ))
        .expect("write batch append invariant violated");
        (batch.put(
            &self.mk(destination),
            &encode_list_meta(
                destination_meta.expire_ms,
                destination_meta.version,
                destination_meta.head,
                destination_meta.tail,
            ),
        ))
        .expect("write batch append invariant violated");
        self.write_batch_if_not_empty_async(&batch).await;
        if source != destination {
            if source_meta.head >= source_meta.tail {
                self.remove_list_meta_cache_if_non_transactional(source);
            } else {
                self.cache_list_meta_if_non_transactional(source, source_meta);
            }
        }
        self.cache_list_meta_if_non_transactional(destination, destination_meta);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(Some(value))
    }

    pub fn list_insert(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        element: &str,
    ) -> Result<i64, Error> {
        self.promote_packed_list(key)?;
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self.list_range_raw_values(key, meta.version, meta.head, meta.tail - 1);
        let Some(pivot_index) = items
            .iter()
            .position(|value| value.as_slice() == pivot.as_bytes())
        else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        let (batch, updated) =
            self.build_list_insert_batch(key, meta, &items, insert_index, element)?;
        self.write_batch_if_not_empty(&batch);
        self.cache_list_meta_if_non_transactional(key, updated);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok((updated.tail - updated.head) as i64)
    }

    pub async fn list_insert_async(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        element: &str,
    ) -> Result<i64, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.promote_packed_list_async(key).await?;
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self
            .list_range_raw_values_async(key, meta.version, meta.head, meta.tail - 1)
            .await;
        let Some(pivot_index) = items
            .iter()
            .position(|value| value.as_slice() == pivot.as_bytes())
        else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        let (batch, updated) =
            self.build_list_insert_batch(key, meta, &items, insert_index, element)?;
        self.write_existing_version_batch_if_not_empty_async(&batch)
            .await;
        self.cache_list_meta_if_non_transactional(key, updated);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok((updated.tail - updated.head) as i64)
    }

    /// Shift only the shorter side of the insertion point. The previous implementation rewrote
    /// and re-indexed the complete list even when the pivot was next to one end.
    fn build_list_insert_batch(
        &self,
        key: &str,
        meta: ListMeta,
        items: &[Vec<u8>],
        insert_index: usize,
        element: &str,
    ) -> Result<(WriteBatch, ListMeta), Error> {
        let can_grow_left = meta.head > i64::MIN;
        let can_grow_right = meta.tail < i64::MAX;
        if !can_grow_left && !can_grow_right {
            return Err(Error::msg("ERR list index space exhausted"));
        }
        let prefix_len = insert_index;
        let suffix_len = items.len().saturating_sub(insert_index);
        let grow_left = can_grow_left && (!can_grow_right || prefix_len <= suffix_len);
        let mut updated = meta;
        let mut batch = WriteBatch::new();
        if grow_left {
            updated.head -= 1;
            for (offset, value) in items[..insert_index].iter().enumerate() {
                (batch.put(
                    &list_item_key(
                        self.db_index,
                        key,
                        meta.version,
                        meta.head + offset as i64 - 1,
                    ),
                    value,
                ))
                .expect("write batch append invariant violated");
            }
            (batch.put(
                &list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    meta.head + insert_index as i64 - 1,
                ),
                element.as_bytes(),
            ))
            .expect("write batch append invariant violated");
        } else {
            for (offset, value) in items[insert_index..].iter().enumerate() {
                (batch.put(
                    &list_item_key(
                        self.db_index,
                        key,
                        meta.version,
                        meta.head + insert_index as i64 + offset as i64 + 1,
                    ),
                    value,
                ))
                .expect("write batch append invariant violated");
            }
            (batch.put(
                &list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    meta.head + insert_index as i64,
                ),
                element.as_bytes(),
            ))
            .expect("write batch append invariant violated");
            updated.tail += 1;
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
        Ok((batch, updated))
    }

    pub fn list_multi_pop(
        &self,
        keys: &[String],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        for key in keys {
            let values = self.list_pop_many(key, left, count)?;
            if !values.is_empty() {
                return Ok(Some((key.clone(), values)));
            }
        }
        Ok(None)
    }

    pub async fn list_multi_pop_async(
        &self,
        keys: &[String],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        if keys.len() == 1 && !self.store.is_transactional() {
            let values = self
                .list_pop_merged_async(&keys[0], left, count)
                .await?
                .into_iter()
                .filter_map(|value| String::from_utf8(value).ok())
                .collect::<Vec<_>>();
            return Ok((!values.is_empty()).then(|| (keys[0].clone(), values)));
        }
        let mut shards = keys
            .iter()
            .map(|key| key_write_lock_shard(self.db_index, key))
            .collect::<Vec<_>>();
        shards.sort_unstable();
        shards.dedup();
        let mut guards = Vec::with_capacity(shards.len());
        for shard in shards {
            guards.push(self.key_write_locks[shard].lock().await);
        }

        for key in keys {
            let values = self.list_pop_many_async_unlocked(key, left, count).await?;
            if !values.is_empty() {
                return Ok(Some((key.clone(), values)));
            }
        }
        Ok(None)
    }

    pub fn list_blocking_pop_once(
        &self,
        keys: &[String],
        left: bool,
    ) -> Result<Option<(String, String)>, Error> {
        for key in keys {
            let value = if left {
                self.list_pop_left(key)?
            } else {
                self.list_pop_right(key)?
            };
            if let Some(value) = value {
                return Ok(Some((key.clone(), value)));
            }
        }
        Ok(None)
    }
}
