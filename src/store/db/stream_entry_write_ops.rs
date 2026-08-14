use super::*;

const STREAM_ADD_MERGE_BATCH_MAX: usize = 256;

impl Db {
    /// Apply a pipelined set of XADD commands with one metadata read and one storage batch per
    /// request. Commands retain their original order, including explicit-ID validation against
    /// earlier successful commands in the same pipeline.
    pub(crate) async fn stream_add_batch_async<'a>(
        &self,
        additions: &[(&'a str, Option<StreamId>, Vec<(&'a str, &'a str)>)],
    ) -> Vec<Result<StreamId, Error>> {
        if additions.is_empty() {
            return Vec::new();
        }
        let mut key_positions = HashMap::<&str, usize>::with_capacity(additions.len());
        let mut keys = Vec::<&str>::with_capacity(additions.len());
        for (key, _, _) in additions {
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _write_guards = self.lock_write_shards(&shards).await;

        let mut states = Vec::with_capacity(keys.len());
        for key in &keys {
            states.push(self.stream_meta_async(key).await);
        }
        let initially_exists = states
            .iter()
            .map(|state| matches!(state, Ok(Some(_))))
            .collect::<Vec<_>>();
        let mut dirty = vec![false; keys.len()];
        let mut batch = WriteBatch::new();
        let mut replies = Vec::with_capacity(additions.len());
        let mut changed = 0u64;

        for (key, requested_id, fields) in additions {
            let position = key_positions[key];
            let state = &mut states[position];
            let result = (|| -> Result<StreamId, Error> {
                let meta = match state {
                    Ok(meta) => meta,
                    Err(error) => return Err(Error::msg(error.to_string())),
                };
                if meta.is_none() {
                    *meta = Some(StreamMeta {
                        expire_ms: 0,
                        version: self.next_version(),
                        last_id: StreamId { ms: 0, seq: 0 },
                        length: 0,
                        entries_added: 0,
                    });
                }
                let meta = meta
                    .as_mut()
                    .expect("missing stream metadata was initialized");
                let id = match requested_id {
                    Some(id) if id.ms == 0 && id.seq == 0 => {
                        return Err(Error::msg(
                            "ERR The ID specified in XADD must be greater than 0-0",
                        ));
                    }
                    Some(id) if *id <= meta.last_id => {
                        return Err(Error::msg(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                        ));
                    }
                    Some(id) => *id,
                    None => self.next_stream_id(meta.last_id),
                };
                meta.last_id = id;
                meta.length += 1;
                meta.entries_added += 1;
                batch.put(
                    &stream_entry_key(self.db_index, key, meta.version, id),
                    &encode_stream_entry_refs(fields),
                );
                dirty[position] = true;
                changed += 1;
                Ok(id)
            })();
            replies.push(result);
        }

        for (position, key) in keys.iter().enumerate() {
            if dirty[position]
                && let Ok(Some(meta)) = states[position]
            {
                batch.put(&self.mk(key), &encode_stream_meta(meta));
            }
        }
        if changed > 0 {
            let has_new_version = dirty
                .iter()
                .enumerate()
                .any(|(position, dirty)| *dirty && !initially_exists[position]);
            if has_new_version {
                self.write_batch_if_not_empty_async(&batch).await;
            } else {
                self.write_existing_version_batch_if_not_empty_async(&batch)
                    .await;
            }
            self.changes.fetch_add(changed, Ordering::Relaxed);
        }
        replies
    }

    pub fn stream_add(
        &self,
        key: &str,
        requested_id: Option<StreamId>,
        fields: &[(String, String)],
    ) -> Result<StreamId, Error> {
        if fields.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }

        let mut meta = match self.stream_meta(key)? {
            Some(meta) => meta,
            None => StreamMeta {
                expire_ms: 0,
                version: self.next_version(),
                last_id: StreamId { ms: 0, seq: 0 },
                length: 0,
                entries_added: 0,
            },
        };

        let id = match requested_id {
            Some(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD must be greater than 0-0",
                    ));
                }
                if id <= meta.last_id {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
                id
            }
            None => self.next_stream_id(meta.last_id),
        };

        meta.last_id = id;
        meta.length += 1;
        meta.entries_added += 1;

        let mut batch = WriteBatch::new();
        batch.put(
            &stream_entry_key(self.db_index, key, meta.version, id),
            &encode_stream_entry(fields),
        );
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    pub async fn stream_add_async(
        &self,
        key: &str,
        requested_id: Option<StreamId>,
        fields: &[(String, String)],
    ) -> Result<StreamId, Error> {
        if fields.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }

        if !self.store.is_transactional() {
            return self
                .enqueue_stream_add(key, requested_id, fields.to_vec())
                .await
                .map_err(|_| Error::msg("ERR stream add merger stopped"))?;
        }

        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let mut meta = match self.stream_meta_async(key).await? {
            Some(meta) => meta,
            None => StreamMeta {
                expire_ms: 0,
                version: self.next_version_async().await,
                last_id: StreamId { ms: 0, seq: 0 },
                length: 0,
                entries_added: 0,
            },
        };

        let id = match requested_id {
            Some(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD must be greater than 0-0",
                    ));
                }
                if id <= meta.last_id {
                    return Err(Error::msg(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
                id
            }
            None => self.next_stream_id(meta.last_id),
        };

        meta.last_id = id;
        meta.length += 1;
        meta.entries_added += 1;

        let mut batch = WriteBatch::new();
        batch.put(
            &stream_entry_key(self.db_index, key, meta.version, id),
            &encode_stream_entry(fields),
        );
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Queue every command before awaiting so a same-key pipeline can share storage commits with
    /// pipelines from other clients. Mixed-key batches keep the existing direct batch path.
    pub(crate) async fn stream_add_many_merged_async<'a>(
        &self,
        additions: &[(&'a str, Option<StreamId>, Vec<(&'a str, &'a str)>)],
    ) -> Vec<Result<StreamId, Error>> {
        let Some((first_key, _, _)) = additions.first() else {
            return Vec::new();
        };
        if self.store.is_transactional() || additions.iter().any(|(key, _, _)| key != first_key) {
            return self.stream_add_batch_async(additions).await;
        }

        let mut receivers = Vec::with_capacity(additions.len());
        for (_, requested_id, fields) in additions {
            let fields = fields
                .iter()
                .map(|(field, value)| ((*field).to_string(), (*value).to_string()))
                .collect();
            receivers.push(self.enqueue_stream_add(first_key, *requested_id, fields));
        }
        let mut replies = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            replies.push(
                receiver
                    .await
                    .unwrap_or_else(|_| Err(Error::msg("ERR stream add merger stopped"))),
            );
        }
        replies
    }

    fn enqueue_stream_add(
        &self,
        key: &str,
        requested_id: Option<StreamId>,
        fields: Vec<(String, String)>,
    ) -> tokio::sync::oneshot::Receiver<Result<StreamId, Error>> {
        let queue_key = (self.db_index, key.as_bytes().to_vec());
        let queue = self
            .counter_cache
            .stream_add_queues
            .entry(queue_key)
            .or_insert_with(|| Arc::new(StreamAddMergeQueue::default()))
            .clone();
        let (reply, result) = tokio::sync::oneshot::channel();
        queue
            .pending
            .lock()
            .expect("stream add merge queue mutex poisoned")
            .push_back(StreamAddMergeRequest {
                requested_id,
                fields,
                reply,
            });
        if queue
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.spawn_stream_add_merge_worker(key.to_string(), queue.clone());
        }
        drop(queue);
        result
    }

    fn spawn_stream_add_merge_worker(&self, key: String, queue: Arc<StreamAddMergeQueue>) {
        let db = self.shared_task_view();
        let queue_key = (self.db_index, key.as_bytes().to_vec());
        tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                let requests = {
                    let mut pending = queue
                        .pending
                        .lock()
                        .expect("stream add merge queue mutex poisoned");
                    let count = pending.len().min(STREAM_ADD_MERGE_BATCH_MAX);
                    pending.drain(..count).collect::<Vec<_>>()
                };
                if !requests.is_empty() {
                    let commands = requests
                        .iter()
                        .map(|request| {
                            (
                                key.as_str(),
                                request.requested_id,
                                request
                                    .fields
                                    .iter()
                                    .map(|(field, value)| (field.as_str(), value.as_str()))
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let replies = db.stream_add_batch_async(&commands).await;
                    for (request, reply) in requests.into_iter().zip(replies) {
                        let _ = request.reply.send(reply);
                    }
                }
                let pending = queue
                    .pending
                    .lock()
                    .expect("stream add merge queue mutex poisoned");
                if pending.is_empty() {
                    queue.running.store(false, Ordering::Release);
                    db.counter_cache
                        .stream_add_queues
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

fn encode_stream_entry_refs(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for (field, value) in fields {
        buf.extend_from_slice(&(field.len() as u32).to_be_bytes());
        buf.extend_from_slice(field.as_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    buf
}
