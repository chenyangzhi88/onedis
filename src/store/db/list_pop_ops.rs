use super::list_update_trim_remove::delete_list_storage_range_to_batch;
use super::*;

const LIST_POP_MERGE_BATCH_MAX: usize = 256;
const LIST_POP_MERGE_ITEM_MAX: usize = 4_096;

impl Db {
    fn try_list_pop_packed(
        &self,
        key: &str,
        left: bool,
        count: usize,
    ) -> Result<Option<Vec<Vec<u8>>>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            self.expire_if_needed(key);
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(Some(Vec::new()));
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode list metadata"))?;
            if header.type_tag != TYPE_LIST {
                return Err(Error::msg(WRONG_TYPE_ERROR));
            }
            let Some(mut items) = decode_packed_list(raw) else {
                return Ok(None);
            };
            let pop_count = count.min(items.len());
            let popped = if left {
                items.drain(..pop_count).collect::<Vec<_>>()
            } else {
                (0..pop_count)
                    .filter_map(|_| items.pop())
                    .collect::<Vec<_>>()
            };
            if popped.is_empty() {
                return Ok(Some(popped));
            }
            let mut batch = WriteBatch::new();
            if items.is_empty() {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
            } else {
                batch.put(
                    &key_bytes,
                    &encode_packed_list(header.expire_ms, &items)
                        .expect("removing items cannot overflow packed list"),
                )?;
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes
                    .fetch_add(popped.len() as u64, Ordering::Relaxed);
                return Ok(Some(popped));
            }
        }
        self.promote_packed_list(key)?;
        Ok(None)
    }

    async fn try_list_pop_batch_packed_async<'a>(
        &self,
        commands: &[(&'a str, bool, usize)],
        keys: &[&'a str],
        key_positions: &HashMap<&'a str, usize>,
    ) -> Result<Option<Vec<Result<Vec<Vec<u8>>, Error>>>, Error> {
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            for key in keys {
                self.expire_if_needed_async(key).await;
            }
            let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
            let mut states = Vec::with_capacity(keys.len());
            for observed in &observations {
                states.push(match observed.value() {
                    None => Ok((0u64, PackedListItems::new(), false)),
                    Some(raw) => {
                        let Some(header) = decode_meta_header(raw) else {
                            return Ok(None);
                        };
                        if header.type_tag != TYPE_LIST {
                            Err(WRONG_TYPE_ERROR.to_string())
                        } else if let Some(items) = decode_packed_list(raw) {
                            Ok((header.expire_ms, items, false))
                        } else {
                            return Ok(None);
                        }
                    }
                });
            }
            let mut replies = Vec::with_capacity(commands.len());
            for (key, left, count) in commands {
                let state = &mut states[key_positions[key]];
                replies.push(match state {
                    Err(message) => Err(Error::msg(message.clone())),
                    Ok((_, items, dirty)) => {
                        let pop_count = (*count).min(items.len());
                        let popped = if *left {
                            items.drain(..pop_count).collect::<Vec<_>>()
                        } else {
                            (0..pop_count)
                                .filter_map(|_| items.pop())
                                .collect::<Vec<_>>()
                        };
                        *dirty |= !popped.is_empty();
                        Ok(popped)
                    }
                });
            }
            let dirty_positions = states
                .iter()
                .enumerate()
                .filter_map(|(position, state)| {
                    state
                        .as_ref()
                        .ok()
                        .is_some_and(|(_, _, dirty)| *dirty)
                        .then_some(position)
                })
                .collect::<Vec<_>>();
            if dirty_positions.is_empty() {
                return Ok(Some(replies));
            }
            let mut batch = WriteBatch::new();
            let mut conditions = Vec::with_capacity(dirty_positions.len());
            for position in dirty_positions {
                let (expire_ms, items, _) = states[position]
                    .as_ref()
                    .expect("dirty packed list state is valid");
                if items.is_empty() {
                    self.delete_main_key_with_ttl_to_batch(&mut batch, keys[position], *expire_ms);
                } else {
                    batch.put(
                        &raw_keys[position],
                        &encode_packed_list(*expire_ms, items)
                            .expect("removing items cannot overflow packed list"),
                    )?;
                }
                conditions.push(CompareCondition::from_observed(&observations[position]));
            }
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                let changed = replies
                    .iter()
                    .filter_map(|reply| reply.as_ref().ok())
                    .map(Vec::len)
                    .sum::<usize>() as u64;
                self.changes.fetch_add(changed, Ordering::Relaxed);
                return Ok(Some(replies));
            }
        }
        for key in keys {
            self.promote_packed_list_async(key).await?;
        }
        Ok(None)
    }

    pub(in crate::store::db) fn list_pop_many(
        &self,
        key: &str,
        left: bool,
        count: usize,
    ) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if let Some(values) = self.try_list_pop_packed(key, left, count)? {
            return Ok(values
                .into_iter()
                .filter_map(|value| String::from_utf8(value).ok())
                .collect());
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
        let initial_head = meta.head;
        let initial_tail = meta.tail;
        if left {
            meta.head += pop_count as i64;
        } else {
            meta.tail -= pop_count as i64;
        }
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            initial_head,
            meta.head,
        );
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            meta.tail.max(meta.head),
            initial_tail,
        );
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            ))
            .expect("write batch append invariant violated");
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
        self.promote_packed_list_async(key).await?;
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
        let initial_head = meta.head;
        let initial_tail = meta.tail;
        if left {
            meta.head += pop_count as i64;
        } else {
            meta.tail -= pop_count as i64;
        }
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            initial_head,
            meta.head,
        );
        delete_list_storage_range_to_batch(
            &mut batch,
            self.db_index,
            key,
            meta.version,
            meta.tail.max(meta.head),
            initial_tail,
        );
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            ))
            .expect("write batch append invariant violated");
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

        match self
            .try_list_pop_batch_packed_async(commands, &keys, &key_positions)
            .await
        {
            Ok(Some(replies)) => return replies,
            Ok(None) => {}
            Err(error) => {
                let message = error.to_string();
                return commands
                    .iter()
                    .map(|_| Err(Error::msg(message.clone())))
                    .collect();
            }
        }
        for key in &keys {
            if let Err(error) = self.promote_packed_list_async(key).await {
                let message = error.to_string();
                return commands
                    .iter()
                    .map(|_| Err(Error::msg(message.clone())))
                    .collect();
            }
        }

        for key in &keys {
            self.expire_if_needed_async(key).await;
        }
        let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let raw_values = self.store.multi_get_raw_async(&raw_keys).await;
        let mut states = raw_values
            .iter()
            .map(|raw| ListPopBatchState::from_raw(raw.as_deref()))
            .collect::<Vec<_>>();
        let mut plans = Vec::with_capacity(commands.len());

        for (key, left, count) in commands {
            let position = key_positions[key];
            match &mut states[position] {
                Err(error) => plans.push(ListPopPlan::Error(error.to_string())),
                Ok(state) if *count == 0 || state.head >= state.tail => {
                    plans.push(ListPopPlan::Items {
                        key_position: position,
                        start: state.head,
                        count: 0,
                        reverse: false,
                    })
                }
                Ok(state) => {
                    let pop_count = (*count).min((state.tail - state.head) as usize);
                    state.touched = true;
                    let start = if *left {
                        let start = state.head;
                        state.head += pop_count as i64;
                        start
                    } else {
                        state.tail -= pop_count as i64;
                        state.tail
                    };
                    plans.push(ListPopPlan::Items {
                        key_position: position,
                        start,
                        count: pop_count,
                        reverse: !*left,
                    });
                }
            }
        }

        // Commands on one list consume at most two contiguous intervals: one from the original
        // head and one from the original tail. Read those intervals as range scans instead of
        // issuing thousands of independent point lookups for COUNT pipelines.
        let mut popped_values = Vec::with_capacity(states.len());
        for (position, state) in states.iter().enumerate() {
            let Ok(state) = state else {
                popped_values.push(ListPopBatchValues::default());
                continue;
            };
            let left = if state.initial_head < state.head {
                self.list_range_raw_values_async(
                    keys[position],
                    state.version,
                    state.initial_head,
                    state.head - 1,
                )
                .await
            } else {
                Vec::new()
            };
            let right = if state.tail < state.initial_tail {
                self.list_range_raw_values_async(
                    keys[position],
                    state.version,
                    state.tail,
                    state.initial_tail - 1,
                )
                .await
            } else {
                Vec::new()
            };
            popped_values.push(ListPopBatchValues {
                left_start: state.initial_head,
                left: left.into_iter().map(Some).collect(),
                right_start: state.tail,
                right: right.into_iter().map(Some).collect(),
            });
        }
        let mut replies = Vec::with_capacity(plans.len());
        for plan in &plans {
            replies.push(match plan {
                ListPopPlan::Error(message) => Err(Error::msg(message.clone())),
                ListPopPlan::Items {
                    key_position,
                    start,
                    count,
                    reverse,
                } => {
                    let values = &mut popped_values[*key_position];
                    Ok((0..*count)
                        .filter_map(|offset| {
                            let offset = if *reverse { count - 1 - offset } else { offset };
                            values.take(start.saturating_add(offset as i64))
                        })
                        .collect())
                }
            });
        }
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
        for &position in &dirty_positions {
            let state = states[position]
                .as_ref()
                .expect("dirty list pop state is valid");
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                keys[position],
                state.version,
                state.initial_head,
                state.head,
            );
            delete_list_storage_range_to_batch(
                &mut batch,
                self.db_index,
                keys[position],
                state.version,
                state.tail.max(state.head),
                state.initial_tail,
            );
            if state.head >= state.tail {
                self.delete_main_key_with_ttl_to_batch(&mut batch, keys[position], state.expire_ms);
            } else {
                (batch.put(
                    &self.mk(keys[position]),
                    &encode_list_meta(state.expire_ms, state.version, state.head, state.tail),
                ))
                .expect("write batch append invariant violated");
            }
        }
        self.write_existing_version_batch_if_not_empty_async(&batch)
            .await;
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
        replies
    }

    /// 左侧出队。
    pub fn list_pop_left(&self, key: &str) -> Result<Option<String>, Error> {
        if let Some(values) = self.try_list_pop_packed(key, true, 1)? {
            return Ok(values
                .into_iter()
                .next()
                .and_then(|value| String::from_utf8(value).ok()));
        }
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
        (batch.delete(&item_key)).expect("write batch append invariant violated");
        meta.head += 1;
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            ))
            .expect("write batch append invariant violated");
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
        Ok(self
            .list_pop_merged_async(key, true, 1)
            .await?
            .into_iter()
            .next()
            .and_then(|value| String::from_utf8(value).ok()))
    }

    /// 右侧出队。
    pub fn list_pop_right(&self, key: &str) -> Result<Option<String>, Error> {
        if let Some(values) = self.try_list_pop_packed(key, false, 1)? {
            return Ok(values
                .into_iter()
                .next()
                .and_then(|value| String::from_utf8(value).ok()));
        }
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
        (batch.delete(&item_key)).expect("write batch append invariant violated");
        if meta.head >= meta.tail {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
        } else {
            (batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            ))
            .expect("write batch append invariant violated");
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
        Ok(self
            .list_pop_merged_async(key, false, 1)
            .await?
            .into_iter()
            .next()
            .and_then(|value| String::from_utf8(value).ok()))
    }

    pub(in crate::store::db) async fn list_pop_merged_async(
        &self,
        key: &str,
        left: bool,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.list_pop_many_merged_async(&[(key, left, count)])
            .await
            .into_iter()
            .next()
            .expect("one list pop command has one reply")
    }

    /// Queue all commands before awaiting any reply so pipelined hot-key pops can be merged
    /// across connections instead of each connection waiting for its own storage commit.
    pub(crate) async fn list_pop_many_merged_async(
        &self,
        commands: &[(&str, bool, usize)],
    ) -> Vec<Result<Vec<Vec<u8>>, Error>> {
        if self.store.is_transactional() {
            return self.list_pop_many_batch_async(commands).await;
        }

        let mut results = Vec::with_capacity(commands.len());
        for (key, left, count) in commands {
            let queue_key = (self.db_index, key.as_bytes().to_vec());
            let queue = self
                .counter_cache
                .list_pop_queues
                .entry(queue_key)
                .or_insert_with(|| Arc::new(ListPopMergeQueue::default()))
                .clone();
            let (reply, result) = tokio::sync::oneshot::channel();
            queue
                .pending
                .lock()
                .expect("list pop merge queue mutex poisoned")
                .push_back(ListPopMergeRequest {
                    left: *left,
                    count: *count,
                    reply,
                });
            if queue
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.spawn_list_pop_merge_worker((*key).to_string(), queue.clone());
            }
            results.push(result);
        }

        let mut replies = Vec::with_capacity(results.len());
        for result in results {
            replies.push(
                result
                    .await
                    .unwrap_or_else(|_| Err(Error::msg("ERR list pop merger stopped"))),
            );
        }
        replies
    }

    fn spawn_list_pop_merge_worker(&self, key: String, queue: Arc<ListPopMergeQueue>) {
        let db = self.shared_task_view();
        let queue_key = (self.db_index, key.as_bytes().to_vec());
        tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                let requests = {
                    let mut pending = queue
                        .pending
                        .lock()
                        .expect("list pop merge queue mutex poisoned");
                    let mut count = 0usize;
                    let mut items = 0usize;
                    for request in pending.iter().take(LIST_POP_MERGE_BATCH_MAX) {
                        let request_items = request.count.min(LIST_POP_MERGE_ITEM_MAX);
                        if count != 0
                            && items.saturating_add(request_items) > LIST_POP_MERGE_ITEM_MAX
                        {
                            break;
                        }
                        count += 1;
                        items = items.saturating_add(request_items);
                    }
                    pending.drain(..count).collect::<Vec<_>>()
                };
                if !requests.is_empty() {
                    let commands = requests
                        .iter()
                        .map(|request| (key.as_str(), request.left, request.count))
                        .collect::<Vec<_>>();
                    let replies = db.list_pop_many_batch_async(&commands).await;
                    for (request, reply) in requests.into_iter().zip(replies) {
                        let _ = request.reply.send(reply);
                    }
                }
                let pending = queue
                    .pending
                    .lock()
                    .expect("list pop merge queue mutex poisoned");
                if pending.is_empty() {
                    queue.running.store(false, Ordering::Release);
                    db.counter_cache
                        .list_pop_queues
                        .remove_if(&queue_key, |_, existing| {
                            Arc::ptr_eq(existing, &queue) && Arc::strong_count(existing) == 2
                        });
                    break;
                }
                drop(pending);
            }
        });
    }
}

struct ListPopBatchState {
    expire_ms: u64,
    version: u64,
    head: i64,
    tail: i64,
    initial_head: i64,
    initial_tail: i64,
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
                initial_head: 0,
                initial_tail: 0,
                touched: false,
            });
        };
        if let Some(meta) = decode_list_meta(raw) {
            return Ok(Self {
                expire_ms: meta.expire_ms,
                version: meta.version,
                head: meta.head,
                tail: meta.tail,
                initial_head: meta.head,
                initial_tail: meta.tail,
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
    Items {
        key_position: usize,
        start: i64,
        count: usize,
        reverse: bool,
    },
}

#[derive(Default)]
struct ListPopBatchValues {
    left_start: i64,
    left: Vec<Option<Vec<u8>>>,
    right_start: i64,
    right: Vec<Option<Vec<u8>>>,
}

impl ListPopBatchValues {
    fn take(&mut self, index: i64) -> Option<Vec<u8>> {
        let left_offset = index.checked_sub(self.left_start)?;
        if let Ok(left_offset) = usize::try_from(left_offset)
            && let Some(value) = self.left.get_mut(left_offset)
        {
            return value.take();
        }
        let right_offset = usize::try_from(index.checked_sub(self.right_start)?).ok()?;
        self.right.get_mut(right_offset)?.take()
    }
}
