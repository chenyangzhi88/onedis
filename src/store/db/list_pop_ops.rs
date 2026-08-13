use super::*;

impl Db {
    pub(in crate::store::db) fn list_pop_many(
        &self,
        key: &str,
        left: bool,
        count: usize,
    ) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let Some(mut meta) = self.list_meta(key)? else {
            return Ok(Vec::new());
        };
        let pop_count = count.min((meta.tail - meta.head).max(0) as usize);
        if pop_count == 0 {
            return Ok(Vec::new());
        }
        let item_keys = (0..pop_count)
            .map(|offset| {
                let index = if left {
                    meta.head + offset as i64
                } else {
                    meta.tail - 1 - offset as i64
                };
                list_item_key(self.db_index, key, meta.version, index)
            })
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw(&item_keys);
        let mut batch = WriteBatch::new();
        for item_key in &item_keys {
            batch.delete(item_key);
        }
        if left {
            meta.head += pop_count as i64;
        } else {
            meta.tail -= pop_count as i64;
        }
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
        let values = values
            .into_iter()
            .flatten()
            .filter_map(|value| String::from_utf8(value).ok())
            .collect::<Vec<_>>();
        self.changes
            .fetch_add(values.len() as u64, Ordering::Relaxed);
        Ok(values)
    }

    pub(in crate::store::db) async fn list_pop_many_async_unlocked(
        &self,
        key: &str,
        left: bool,
        count: usize,
    ) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let Some(mut meta) = self.list_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        let pop_count = count.min((meta.tail - meta.head).max(0) as usize);
        if pop_count == 0 {
            return Ok(Vec::new());
        }
        let item_keys = (0..pop_count)
            .map(|offset| {
                let index = if left {
                    meta.head + offset as i64
                } else {
                    meta.tail - 1 - offset as i64
                };
                list_item_key(self.db_index, key, meta.version, index)
            })
            .collect::<Vec<_>>();
        let values = self.store.multi_get_raw_async(&item_keys).await;
        let mut batch = WriteBatch::new();
        for item_key in &item_keys {
            batch.delete(item_key);
        }
        if left {
            meta.head += pop_count as i64;
        } else {
            meta.tail -= pop_count as i64;
        }
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
        let values = values
            .into_iter()
            .flatten()
            .filter_map(|value| String::from_utf8(value).ok())
            .collect::<Vec<_>>();
        self.changes
            .fetch_add(values.len() as u64, Ordering::Relaxed);
        Ok(values)
    }

    /// Apply ordered LPOP/RPOP commands by moving each key's metadata once per pipeline.
    pub(crate) async fn list_pop_batch_async(
        &self,
        commands: &[(&str, bool)],
    ) -> Vec<Result<Option<Vec<u8>>, Error>> {
        let commands = commands
            .iter()
            .map(|(key, left)| (*key, *left, 1))
            .collect::<Vec<_>>();
        self.list_pop_many_batch_async(&commands)
            .await
            .into_iter()
            .map(|reply| reply.map(|values| values.into_iter().next()))
            .collect()
    }

    /// Apply ordered LPOP/RPOP commands, including their optional COUNT, by moving each
    /// distinct key's metadata once and committing all item deletes in one batch.
    pub(crate) async fn list_pop_many_batch_async(
        &self,
        commands: &[(&str, bool, usize)],
    ) -> Vec<Result<Vec<Vec<u8>>, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }

        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::with_capacity(commands.len());
        for (key, _, _) in commands {
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _write_guards = self.lock_write_shards(&shards).await;

        for _ in 0..64 {
            for key in &keys {
                self.expire_if_needed_async(key).await;
            }
            let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
            let mut states = observations
                .iter()
                .map(|observed| ListPopBatchState::from_raw(observed.value().map(AsRef::as_ref)))
                .collect::<Vec<_>>();
            let mut plans = Vec::with_capacity(commands.len());
            let mut item_keys = Vec::with_capacity(commands.len());

            for (key, left, count) in commands {
                let position = key_positions[key];
                match &mut states[position] {
                    Err(error) => plans.push(ListPopPlan::Error(error.to_string())),
                    Ok(state) if *count == 0 || state.head >= state.tail => {
                        plans.push(ListPopPlan::Items {
                            lookup: item_keys.len(),
                            count: 0,
                        })
                    }
                    Ok(state) => {
                        let pop_count = (*count).min((state.tail - state.head) as usize);
                        state.touched = true;
                        let lookup = item_keys.len();
                        for offset in 0..pop_count {
                            let index = if *left {
                                state.head + offset as i64
                            } else {
                                state.tail - 1 - offset as i64
                            };
                            item_keys.push(list_item_key(self.db_index, key, state.version, index));
                        }
                        if *left {
                            state.head += pop_count as i64;
                        } else {
                            state.tail -= pop_count as i64;
                        }
                        plans.push(ListPopPlan::Items {
                            lookup,
                            count: pop_count,
                        });
                    }
                }
            }

            let item_values = self.store.multi_get_raw_async(&item_keys).await;
            let replies = plans
                .iter()
                .map(|plan| match plan {
                    ListPopPlan::Error(message) => Err(Error::msg(message.clone())),
                    ListPopPlan::Items { lookup, count } => Ok(item_values
                        [*lookup..lookup.saturating_add(*count)]
                        .iter()
                        .flatten()
                        .cloned()
                        .collect()),
                })
                .collect::<Vec<_>>();
            let dirty_positions = states
                .iter()
                .enumerate()
                .filter_map(|(position, state)| {
                    state
                        .as_ref()
                        .ok()
                        .is_some_and(|state| state.touched)
                        .then_some(position)
                })
                .collect::<Vec<_>>();
            if dirty_positions.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            for item_key in &item_keys {
                batch.delete(item_key);
            }
            for &position in &dirty_positions {
                let state = states[position]
                    .as_ref()
                    .expect("dirty list pop state is valid");
                if state.head >= state.tail {
                    self.delete_main_key_with_ttl_to_batch(
                        &mut batch,
                        keys[position],
                        state.expire_ms,
                    );
                } else {
                    batch.put(
                        &self.mk(keys[position]),
                        &encode_list_meta(state.expire_ms, state.version, state.head, state.tail),
                    );
                }
            }
            let conditions = dirty_positions
                .iter()
                .map(|&position| CompareCondition::from_observed(&observations[position]))
                .collect::<Vec<_>>();
            match self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await
            {
                Ok(true) => {
                    let changed = replies
                        .iter()
                        .filter_map(|reply| reply.as_ref().ok())
                        .map(Vec::len)
                        .sum::<usize>() as u64;
                    self.changes.fetch_add(changed, Ordering::Relaxed);
                    for &position in &dirty_positions {
                        let state = states[position]
                            .as_ref()
                            .expect("dirty list pop state is valid");
                        if state.head >= state.tail {
                            self.remove_list_meta_cache_if_non_transactional(keys[position]);
                        } else {
                            self.cache_list_meta_if_non_transactional(
                                keys[position],
                                ListMeta {
                                    expire_ms: state.expire_ms,
                                    version: state.version,
                                    head: state.head,
                                    tail: state.tail,
                                },
                            );
                        }
                    }
                    return replies;
                }
                Ok(false) => continue,
                Err(error) => {
                    let message = error.to_string();
                    return commands
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }

        commands
            .iter()
            .map(|_| Err(Error::msg("ERR list pop batch write conflict")))
            .collect()
    }

    /// 左侧出队。
    pub fn list_pop_left(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.list_pop_left_async_unlocked(key).await
    }

    pub(in crate::store::db) async fn list_pop_left_async_unlocked(
        &self,
        key: &str,
    ) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.list_pop_right_async_unlocked(key).await
    }

    pub(in crate::store::db) async fn list_pop_right_async_unlocked(
        &self,
        key: &str,
    ) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
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

struct ListPopBatchState {
    expire_ms: u64,
    version: u64,
    head: i64,
    tail: i64,
    touched: bool,
}

impl ListPopBatchState {
    fn from_raw(raw: Option<&[u8]>) -> Result<Self, Error> {
        let Some(raw) = raw else {
            return Ok(Self {
                expire_ms: 0,
                version: 0,
                head: 0,
                tail: 0,
                touched: false,
            });
        };
        if let Some(meta) = decode_list_meta(raw) {
            return Ok(Self {
                expire_ms: meta.expire_ms,
                version: meta.version,
                head: meta.head,
                tail: meta.tail,
                touched: false,
            });
        }
        if decode_meta_header(raw).is_some_and(|header| header.type_tag != TYPE_LIST) {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Err(Error::msg("Failed to decode list metadata"))
    }
}

enum ListPopPlan {
    Error(String),
    Items { lookup: usize, count: usize },
}
