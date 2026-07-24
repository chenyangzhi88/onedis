use super::*;

impl Db {
    pub fn list_move(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
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
                version: self.next_persisted_version(),
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
        batch.delete(&list_item_key(
            self.db_index,
            source,
            source_meta.version,
            source_index,
        ));
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
            batch.put(
                &self.mk(source),
                &encode_list_meta(
                    source_meta.expire_ms,
                    source_meta.version,
                    source_meta.head,
                    source_meta.tail,
                ),
            );
        }

        let destination_index = if destination_left {
            destination_meta.head -= 1;
            destination_meta.head
        } else {
            let index = destination_meta.tail;
            destination_meta.tail += 1;
            index
        };
        batch.put(
            &list_item_key(
                self.db_index,
                destination,
                destination_meta.version,
                destination_index,
            ),
            value.as_bytes(),
        );
        batch.put(
            &self.mk(destination),
            &encode_list_meta(
                destination_meta.expire_ms,
                destination_meta.version,
                destination_meta.head,
                destination_meta.tail,
            ),
        );
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
        let source_shard = set_write_lock_shard(self.db_index, source);
        let destination_shard = set_write_lock_shard(self.db_index, destination);
        if source_shard == destination_shard {
            let _guard = self.set_write_locks[source_shard].lock().await;
            self.list_move_async_unlocked(source, destination, source_left, destination_left)
                .await
        } else if source_shard < destination_shard {
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            let _destination_guard = self.set_write_locks[destination_shard].lock().await;
            self.list_move_async_unlocked(source, destination, source_left, destination_left)
                .await
        } else {
            let _destination_guard = self.set_write_locks[destination_shard].lock().await;
            let _source_guard = self.set_write_locks[source_shard].lock().await;
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
                    version: self.next_persisted_version_async().await,
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
        batch.delete(&list_item_key(
            self.db_index,
            source,
            source_meta.version,
            source_index,
        ));
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
            batch.put(
                &self.mk(source),
                &encode_list_meta(
                    source_meta.expire_ms,
                    source_meta.version,
                    source_meta.head,
                    source_meta.tail,
                ),
            );
        }

        let destination_index = if destination_left {
            destination_meta.head -= 1;
            destination_meta.head
        } else {
            let index = destination_meta.tail;
            destination_meta.tail += 1;
            index
        };
        batch.put(
            &list_item_key(
                self.db_index,
                destination,
                destination_meta.version,
                destination_index,
            ),
            value.as_bytes(),
        );
        batch.put(
            &self.mk(destination),
            &encode_list_meta(
                destination_meta.expire_ms,
                destination_meta.version,
                destination_meta.head,
                destination_meta.tail,
            ),
        );
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
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let mut items = self.list_range(key, 0, -1)?;
        let Some(pivot_index) = items.iter().position(|value| value == pivot) else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        items.insert(insert_index, element.to_string());

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for (index, value) in items.iter().enumerate() {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, index as i64),
                value.as_bytes(),
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, 0, items.len() as i64),
        );
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(items.len() as i64)
    }

    pub async fn list_insert_async(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        element: &str,
    ) -> Result<i64, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let mut items = self.list_range_async(key, 0, -1).await?;
        let Some(pivot_index) = items.iter().position(|value| value == pivot) else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        items.insert(insert_index, element.to_string());

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for (index, value) in items.iter().enumerate() {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, index as i64),
                value.as_bytes(),
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, 0, items.len() as i64),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(items.len() as i64)
    }

    pub fn list_multi_pop(
        &self,
        keys: &[String],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        for key in keys {
            if self.list_len(key)? == 0 {
                continue;
            }

            let mut values = Vec::new();
            for _ in 0..count {
                let value = if left {
                    self.list_pop_left(key)?
                } else {
                    self.list_pop_right(key)?
                };
                match value {
                    Some(value) => values.push(value),
                    None => break,
                }
            }
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
        for key in keys {
            if self.list_len_async(key).await? == 0 {
                continue;
            }

            let mut values = Vec::new();
            for _ in 0..count {
                let value = if left {
                    self.list_pop_left_async(key).await?
                } else {
                    self.list_pop_right_async(key).await?
                };
                match value {
                    Some(value) => values.push(value),
                    None => break,
                }
            }
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
