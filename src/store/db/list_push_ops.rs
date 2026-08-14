use super::*;

const LIST_PUSH_MERGE_BATCH_MAX: usize = 256;

enum ListPushBatchState {
    Valid {
        meta: Option<ListMeta>,
        initially_exists: bool,
        dirty: bool,
    },
    Error(String),
}

impl Db {
    pub fn list_push_left(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_left_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_left_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            meta.head -= 1;
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.head),
                value,
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_left_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        self.list_push_bytes_merged_async(key, true, values, only_if_exists)
            .await
    }

    /// 右侧批量入队。
    pub fn list_push_right(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_right_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_right_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.tail),
                value,
            );
            meta.tail += 1;
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_right_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        self.list_push_bytes_merged_async(key, false, values, only_if_exists)
            .await
    }

    async fn list_push_bytes_merged_async(
        &self,
        key: &str,
        left: bool,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        if self.store.is_transactional() {
            let command = (key, left, values.to_vec(), only_if_exists);
            return self
                .list_push_batch_async(&[command])
                .await
                .into_iter()
                .next()
                .expect("one list push command has one reply");
        }
        let queue_key = (self.db_index, key.as_bytes().to_vec());
        let queue = self
            .counter_cache
            .list_push_queues
            .entry(queue_key)
            .or_insert_with(|| Arc::new(ListPushMergeQueue::default()))
            .clone();
        let (reply, result) = tokio::sync::oneshot::channel();
        queue
            .pending
            .lock()
            .expect("list push merge queue mutex poisoned")
            .push_back(ListPushMergeRequest {
                left,
                values: values.iter().map(|value| value.to_vec()).collect(),
                only_if_exists,
                reply,
            });
        if queue
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.spawn_list_push_merge_worker(key.to_string(), queue.clone());
        }
        drop(queue);
        result
            .await
            .map_err(|_| Error::msg("ERR list push merger stopped"))?
    }

    fn spawn_list_push_merge_worker(&self, key: String, queue: Arc<ListPushMergeQueue>) {
        let db = self.shared_task_view();
        let queue_key = (self.db_index, key.as_bytes().to_vec());
        tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                let requests = {
                    let mut pending = queue
                        .pending
                        .lock()
                        .expect("list push merge queue mutex poisoned");
                    let count = pending.len().min(LIST_PUSH_MERGE_BATCH_MAX);
                    pending.drain(..count).collect::<Vec<_>>()
                };
                if !requests.is_empty() {
                    let commands = requests
                        .iter()
                        .map(|request| {
                            (
                                key.as_str(),
                                request.left,
                                request.values.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                                request.only_if_exists,
                            )
                        })
                        .collect::<Vec<_>>();
                    let replies = db.list_push_batch_async(&commands).await;
                    for (request, reply) in requests.into_iter().zip(replies) {
                        let _ = request.reply.send(reply);
                    }
                }
                let pending = queue
                    .pending
                    .lock()
                    .expect("list push merge queue mutex poisoned");
                if pending.is_empty() {
                    queue.running.store(false, Ordering::Release);
                    db.counter_cache
                        .list_push_queues
                        .remove_if(&queue_key, |_, existing| {
                            Arc::ptr_eq(existing, &queue) && Arc::strong_count(existing) == 2
                        });
                    break;
                }
                drop(pending);
            }
        });
    }

    pub(in crate::store::db) async fn list_push_batch_async<'a>(
        &self,
        commands: &[(&'a str, bool, Vec<&'a [u8]>, bool)],
    ) -> Vec<Result<usize, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }
        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::new();
        for (key, _, _, _) in commands {
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _guards = self.lock_write_shards(&shards).await;
        let mut states = Vec::with_capacity(keys.len());
        for key in &keys {
            states.push(match self.list_meta_async(key).await {
                Ok(meta) => ListPushBatchState::Valid {
                    initially_exists: meta.is_some(),
                    meta,
                    dirty: false,
                },
                Err(error) => ListPushBatchState::Error(error.to_string()),
            });
        }
        let mut batch = WriteBatch::new();
        let mut replies = Vec::with_capacity(commands.len());
        let mut changed = 0u64;
        for (key, left, values, only_if_exists) in commands {
            let position = key_positions[key];
            let ListPushBatchState::Valid { meta, dirty, .. } = &mut states[position] else {
                let ListPushBatchState::Error(message) = &states[position] else {
                    unreachable!()
                };
                replies.push(Err(Error::msg(message.clone())));
                continue;
            };
            if meta.is_none() && *only_if_exists {
                replies.push(Ok(0));
                continue;
            }
            if meta.is_none() {
                *meta = Some(ListMeta {
                    expire_ms: 0,
                    version: self.next_version_async().await,
                    head: 0,
                    tail: 0,
                });
            }
            let meta = meta
                .as_mut()
                .expect("missing list metadata was initialized");
            for value in values {
                if *left {
                    meta.head -= 1;
                    batch.put(
                        &list_item_key(self.db_index, key, meta.version, meta.head),
                        value,
                    );
                } else {
                    batch.put(
                        &list_item_key(self.db_index, key, meta.version, meta.tail),
                        value,
                    );
                    meta.tail += 1;
                }
            }
            if !values.is_empty() {
                *dirty = true;
                changed += 1;
            }
            replies.push(Ok((meta.tail - meta.head) as usize));
        }
        let mut has_new_version = false;
        for (position, state) in states.iter().enumerate() {
            if let ListPushBatchState::Valid {
                meta: Some(meta),
                initially_exists,
                dirty: true,
            } = state
            {
                batch.put(
                    &self.mk(keys[position]),
                    &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
                );
                has_new_version |= !initially_exists;
            }
        }
        if changed > 0 {
            if has_new_version {
                self.write_batch_if_not_empty_async(&batch).await;
            } else {
                self.write_existing_version_batch_if_not_empty_async(&batch)
                    .await;
            }
            self.changes.fetch_add(changed, Ordering::Relaxed);
            for (position, state) in states.iter().enumerate() {
                if let ListPushBatchState::Valid {
                    meta: Some(meta),
                    dirty: true,
                    ..
                } = state
                {
                    self.cache_list_meta_if_non_transactional(keys[position], *meta);
                }
            }
        }
        replies
    }
}
