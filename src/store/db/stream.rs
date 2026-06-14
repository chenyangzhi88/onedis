impl Db {
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

        let mut created_stream = false;
        let mut meta = match self.stream_meta(key)? {
            Some(meta) => meta,
            None => {
                created_stream = true;
                StreamMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version(),
                    last_id: StreamId { ms: 0, seq: 0 },
                    length: 0,
                    entries_added: 0,
                }
            }
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
        if created_stream {}
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

        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let mut meta = match self.stream_meta_async(key).await? {
            Some(meta) => meta,
            None => StreamMeta {
                expire_ms: 0,
                version: self.next_persisted_version_async().await,
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

    pub fn stream_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .stream_meta(key)?
            .map(|meta| meta.length as usize)
            .unwrap_or(0))
    }

    pub async fn stream_len_async(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .stream_meta_async(key)
            .await?
            .map(|meta| meta.length as usize)
            .unwrap_or(0))
    }

    pub fn stream_range(
        &self,
        key: &str,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        let lower = start.unwrap_or(StreamId { ms: 0, seq: 0 });
        let upper = end.unwrap_or(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        });
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = self.stream_entries_between(key, meta.version, lower, upper);
        if reverse {
            entries.reverse();
        }
        entries.truncate(limit);
        Ok(entries)
    }

    pub async fn stream_range_async(
        &self,
        key: &str,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        let lower = start.unwrap_or(StreamId { ms: 0, seq: 0 });
        let upper = end.unwrap_or(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        });
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = self
            .stream_entries_between_async(key, meta.version, lower, upper)
            .await;
        if reverse {
            entries.reverse();
        }
        entries.truncate(limit);
        Ok(entries)
    }

    pub fn stream_read(
        &self,
        requests: &[(String, StreamReadStart)],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }

        for (key, start) in requests {
            let Some(meta) = self.stream_meta(key)? else {
                continue;
            };
            let lower = match start {
                StreamReadStart::Id(id) => *id,
                StreamReadStart::Latest => meta.last_id,
            };
            let upper = StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            };
            let mut entries = self
                .stream_entries_between(key, meta.version, lower, upper)
                .into_iter()
                .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                .collect::<Vec<_>>();
            entries.truncate(limit);
            if !entries.is_empty() {
                result.push((key.clone(), entries));
            }
        }
        Ok(result)
    }

    pub async fn stream_read_async(
        &self,
        requests: &[(String, StreamReadStart)],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }

        for (key, start) in requests {
            let Some(meta) = self.stream_meta_async(key).await? else {
                continue;
            };
            let lower = match start {
                StreamReadStart::Id(id) => *id,
                StreamReadStart::Latest => meta.last_id,
            };
            let upper = StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            };
            let mut entries = self
                .stream_entries_between_async(key, meta.version, lower, upper)
                .await
                .into_iter()
                .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                .collect::<Vec<_>>();
            entries.truncate(limit);
            if !entries.is_empty() {
                result.push((key.clone(), entries));
            }
        }
        Ok(result)
    }

    pub fn stream_delete(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            if self.store.get_raw(&entry_key).is_some() {
                batch.delete(&entry_key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            batch.put(&self.mk(key), &encode_stream_meta(meta));
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub async fn stream_delete_async(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            if self.store.get_raw_async(&entry_key).await.is_some() {
                batch.delete(&entry_key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            batch.put(&self.mk(key), &encode_stream_meta(meta));
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub fn stream_set_id(&self, key: &str, id: StreamId) -> Result<(), Error> {
        let mut meta = self
            .stream_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        meta.last_id = id;
        let mut batch = WriteBatch::new();
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stream_set_id_async(&self, key: &str, id: StreamId) -> Result<(), Error> {
        let mut meta = self
            .stream_meta_async(key)
            .await?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        meta.last_id = id;
        let mut batch = WriteBatch::new();
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn stream_ack_delete(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        self.stream_ack(key, group, ids)?;
        self.stream_delete(key, ids)
    }

    pub async fn stream_ack_delete_async(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        self.stream_ack_async(key, group, ids).await?;
        self.stream_delete_async(key, ids).await
    }

    pub fn stream_trim_maxlen(&self, key: &str, max_len: usize) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let entries = self.stream_entries_raw(key, meta.version);
        if entries.len() <= max_len {
            return Ok(0);
        }
        let delete_count = entries.len() - max_len;
        let mut batch = WriteBatch::new();
        for (id, _) in entries.into_iter().take(delete_count) {
            batch.delete(&stream_entry_key(self.db_index, key, meta.version, id));
        }
        meta.length = meta.length.saturating_sub(delete_count as u64);
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(delete_count)
    }

    pub async fn stream_trim_maxlen_async(
        &self,
        key: &str,
        max_len: usize,
    ) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        let entries = self.stream_entries_raw_async(key, meta.version).await;
        if entries.len() <= max_len {
            return Ok(0);
        }
        let delete_count = entries.len() - max_len;
        let mut batch = WriteBatch::new();
        for (id, _) in entries.into_iter().take(delete_count) {
            batch.delete(&stream_entry_key(self.db_index, key, meta.version, id));
        }
        meta.length = meta.length.saturating_sub(delete_count as u64);
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(delete_count)
    }

    pub fn stream_group_create(
        &self,
        key: &str,
        group: &str,
        id: StreamId,
        mkstream: bool,
    ) -> Result<(), Error> {
        let meta = match self.stream_meta(key)? {
            Some(meta) => meta,
            None if mkstream => {
                let meta = StreamMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version(),
                    last_id: StreamId { ms: 0, seq: 0 },
                    length: 0,
                    entries_added: 0,
                };
                let mut batch = WriteBatch::new();
                batch.put(&self.mk(key), &encode_stream_meta(meta));
                self.write_batch_if_not_empty(&batch);
                meta
            }
            None => {
                return Err(Error::msg(
                    "ERR The XGROUP subcommand requires the key to exist",
                ));
            }
        };
        let id = if id.ms == u64::MAX && id.seq == u64::MAX {
            meta.last_id
        } else {
            id
        };
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw(&group_key).is_some() {
            return Err(Error::msg("BUSYGROUP Consumer Group name already exists"));
        }
        let id = if id.ms == u64::MAX && id.seq == u64::MAX {
            meta.last_id
        } else {
            id
        };
        let state = StreamGroupState {
            last_delivered_id: id,
            entries_read: 0,
        };
        let mut batch = WriteBatch::new();
        batch.put(&group_key, &encode_stream_group_state(&state));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stream_group_create_async(
        &self,
        key: &str,
        group: &str,
        id: StreamId,
        mkstream: bool,
    ) -> Result<(), Error> {
        let meta = match self.stream_meta_async(key).await? {
            Some(meta) => meta,
            None if mkstream => {
                let meta = StreamMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version_async().await,
                    last_id: StreamId { ms: 0, seq: 0 },
                    length: 0,
                    entries_added: 0,
                };
                let mut batch = WriteBatch::new();
                batch.put(&self.mk(key), &encode_stream_meta(meta));
                self.write_batch_if_not_empty_async(&batch).await;
                meta
            }
            None => {
                return Err(Error::msg(
                    "ERR The XGROUP subcommand requires the key to exist",
                ));
            }
        };
        let id = if id.ms == u64::MAX && id.seq == u64::MAX {
            meta.last_id
        } else {
            id
        };
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw_async(&group_key).await.is_some() {
            return Err(Error::msg("BUSYGROUP Consumer Group name already exists"));
        }
        let state = StreamGroupState {
            last_delivered_id: id,
            entries_read: 0,
        };
        let mut batch = WriteBatch::new();
        batch.put(&group_key, &encode_stream_group_state(&state));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn stream_group_set_id(&self, key: &str, group: &str, id: StreamId) -> Result<(), Error> {
        let meta = self
            .stream_meta(key)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw(&group_key).is_none() {
            return Err(Error::msg("NOGROUP No such key or consumer group"));
        }
        let state = StreamGroupState {
            last_delivered_id: id,
            entries_read: 0,
        };
        let mut batch = WriteBatch::new();
        batch.put(&group_key, &encode_stream_group_state(&state));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stream_group_set_id_async(
        &self,
        key: &str,
        group: &str,
        id: StreamId,
    ) -> Result<(), Error> {
        let meta = self
            .stream_meta_async(key)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw_async(&group_key).await.is_none() {
            return Err(Error::msg("NOGROUP No such key or consumer group"));
        }
        let state = StreamGroupState {
            last_delivered_id: id,
            entries_read: 0,
        };
        let mut batch = WriteBatch::new();
        batch.put(&group_key, &encode_stream_group_state(&state));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn stream_group_destroy(&self, key: &str, group: &str) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw(&group_key).is_none() {
            return Ok(0);
        }
        let mut batch = WriteBatch::new();
        batch.delete(&group_key);
        let pel_prefix = stream_pel_group_prefix(self.db_index, key, meta.version, group);
        if let Some(end) = prefix_exclusive_upper_bound(&pel_prefix) {
            batch.delete_range(&pel_prefix, &end);
        }
        let consumer_prefix = stream_consumer_group_prefix(self.db_index, key, meta.version, group);
        if let Some(end) = prefix_exclusive_upper_bound(&consumer_prefix) {
            batch.delete_range(&consumer_prefix, &end);
        }
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(1)
    }

    pub async fn stream_group_destroy_async(&self, key: &str, group: &str) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        let group_key = stream_group_key(self.db_index, key, meta.version, group);
        if self.store.get_raw_async(&group_key).await.is_none() {
            return Ok(0);
        }
        let mut batch = WriteBatch::new();
        batch.delete(&group_key);
        let pel_prefix = stream_pel_group_prefix(self.db_index, key, meta.version, group);
        if let Some(end) = prefix_exclusive_upper_bound(&pel_prefix) {
            batch.delete_range(&pel_prefix, &end);
        }
        let consumer_prefix = stream_consumer_group_prefix(self.db_index, key, meta.version, group);
        if let Some(end) = prefix_exclusive_upper_bound(&consumer_prefix) {
            batch.delete_range(&consumer_prefix, &end);
        }
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(1)
    }

    pub fn stream_group_create_consumer(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Err(Error::msg("NOGROUP No such key or consumer group"));
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let consumer_key = stream_consumer_key(self.db_index, key, meta.version, group, consumer);
        if self.store.get_raw(&consumer_key).is_some() {
            return Ok(0);
        }
        let mut batch = WriteBatch::new();
        batch.put(
            &consumer_key,
            &encode_stream_consumer_state(&StreamConsumerState {
                last_seen_ms: now_ms(),
            }),
        );
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(1)
    }

    pub fn stream_group_delete_consumer(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let mut removed = 0usize;
        let mut batch = WriteBatch::new();
        let consumer_key = stream_consumer_key(self.db_index, key, meta.version, group, consumer);
        if self.store.get_raw(&consumer_key).is_some() {
            batch.delete(&consumer_key);
        }
        for (id, pel) in self.stream_pending_raw(key, meta.version, group) {
            if pel.consumer == consumer {
                batch.delete(&stream_pel_key(self.db_index, key, meta.version, group, id));
                removed += 1;
            }
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub async fn stream_group_delete_consumer_async(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let mut removed = 0usize;
        let mut batch = WriteBatch::new();
        let consumer_key = stream_consumer_key(self.db_index, key, meta.version, group, consumer);
        if self.store.get_raw_async(&consumer_key).await.is_some() {
            batch.delete(&consumer_key);
        }
        for (id, pel) in self
            .stream_pending_raw_async(key, meta.version, group)
            .await
        {
            if pel.consumer == consumer {
                batch.delete(&stream_pel_key(self.db_index, key, meta.version, group, id));
                removed += 1;
            }
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub fn stream_read_group(
        &self,
        group: &str,
        consumer: &str,
        requests: &[(String, StreamReadGroupStart)],
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }
        for (key, start) in requests {
            let Some(meta) = self.stream_meta(key)? else {
                continue;
            };
            let Some(mut group_state) = self.stream_group_state(key, group)? else {
                return Err(Error::msg("NOGROUP No such key or consumer group"));
            };
            let entries = match start {
                StreamReadGroupStart::New => {
                    let lower = group_state.last_delivered_id;
                    let mut entries = self
                        .stream_entries_between(
                            key,
                            meta.version,
                            lower,
                            StreamId {
                                ms: u64::MAX,
                                seq: u64::MAX,
                            },
                        )
                        .into_iter()
                        .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                        .collect::<Vec<_>>();
                    entries.truncate(limit);
                    entries
                }
                StreamReadGroupStart::Id(id) => {
                    let pending = self.stream_pending_raw(key, meta.version, group);
                    let mut entries = Vec::new();
                    for (pending_id, pel) in pending {
                        if pending_id <= *id || pel.consumer != consumer {
                            continue;
                        }
                        if let Some(entry) = self.stream_entry_by_id(key, meta.version, pending_id)
                        {
                            entries.push(entry);
                            if entries.len() >= limit {
                                break;
                            }
                        }
                    }
                    entries
                }
            };
            if entries.is_empty() {
                continue;
            }
            let mut batch = WriteBatch::new();
            let now = now_ms();
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            if matches!(start, StreamReadGroupStart::New) {
                if let Some(last) = entries.last().and_then(|entry| parse_stream_id(&entry.id)) {
                    group_state.last_delivered_id = last;
                    group_state.entries_read += entries.len() as u64;
                    batch.put(
                        &stream_group_key(self.db_index, key, meta.version, group),
                        &encode_stream_group_state(&group_state),
                    );
                }
            }
            if !noack {
                for entry in &entries {
                    if let Some(id) = parse_stream_id(&entry.id) {
                        batch.put(
                            &stream_pel_key(self.db_index, key, meta.version, group, id),
                            &encode_stream_pel_state(&StreamPelState {
                                consumer: consumer.to_string(),
                                last_delivery_ms: now,
                                deliveries: 1,
                            }),
                        );
                    }
                }
            }
            self.write_batch_if_not_empty(&batch);
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            result.push((key.clone(), entries));
        }
        Ok(result)
    }

    pub async fn stream_read_group_async(
        &self,
        group: &str,
        consumer: &str,
        requests: &[(String, StreamReadGroupStart)],
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, Error> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(result);
        }
        for (key, start) in requests {
            let Some(meta) = self.stream_meta_async(key).await? else {
                continue;
            };
            let Some(mut group_state) = self.stream_group_state_async(key, group).await? else {
                return Err(Error::msg("NOGROUP No such key or consumer group"));
            };
            let entries = match start {
                StreamReadGroupStart::New => {
                    let lower = group_state.last_delivered_id;
                    let mut entries = self
                        .stream_entries_between_async(
                            key,
                            meta.version,
                            lower,
                            StreamId {
                                ms: u64::MAX,
                                seq: u64::MAX,
                            },
                        )
                        .await
                        .into_iter()
                        .filter(|entry| parse_stream_id(&entry.id).is_some_and(|id| id > lower))
                        .collect::<Vec<_>>();
                    entries.truncate(limit);
                    entries
                }
                StreamReadGroupStart::Id(id) => {
                    let pending = self
                        .stream_pending_raw_async(key, meta.version, group)
                        .await;
                    let mut entries = Vec::new();
                    for (pending_id, pel) in pending {
                        if pending_id <= *id || pel.consumer != consumer {
                            continue;
                        }
                        if let Some(entry) = self
                            .stream_entry_by_id_async(key, meta.version, pending_id)
                            .await
                        {
                            entries.push(entry);
                            if entries.len() >= limit {
                                break;
                            }
                        }
                    }
                    entries
                }
            };
            if entries.is_empty() {
                continue;
            }
            let mut batch = WriteBatch::new();
            let now = now_ms();
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            if matches!(start, StreamReadGroupStart::New) {
                if let Some(last) = entries.last().and_then(|entry| parse_stream_id(&entry.id)) {
                    group_state.last_delivered_id = last;
                    group_state.entries_read += entries.len() as u64;
                    batch.put(
                        &stream_group_key(self.db_index, key, meta.version, group),
                        &encode_stream_group_state(&group_state),
                    );
                }
            }
            if !noack {
                for entry in &entries {
                    if let Some(id) = parse_stream_id(&entry.id) {
                        batch.put(
                            &stream_pel_key(self.db_index, key, meta.version, group, id),
                            &encode_stream_pel_state(&StreamPelState {
                                consumer: consumer.to_string(),
                                last_delivery_ms: now,
                                deliveries: 1,
                            }),
                        );
                    }
                }
            }
            self.write_batch_if_not_empty_async(&batch).await;
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            result.push((key.clone(), entries));
        }
        Ok(result)
    }

    pub fn stream_ack(&self, key: &str, group: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let mut acked = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let key = stream_pel_key(self.db_index, key, meta.version, group, *id);
            if self.store.get_raw(&key).is_some() {
                batch.delete(&key);
                acked += 1;
            }
        }
        if acked > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(acked)
    }

    pub async fn stream_ack_async(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let mut acked = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let key = stream_pel_key(self.db_index, key, meta.version, group, *id);
            if self.store.get_raw_async(&key).await.is_some() {
                batch.delete(&key);
                acked += 1;
            }
        }
        if acked > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(acked)
    }

    pub fn stream_pending_summary(
        &self,
        key: &str,
        group: &str,
    ) -> Result<StreamPendingSummary, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(StreamPendingSummary {
                total: 0,
                smallest_id: None,
                greatest_id: None,
                consumers: Vec::new(),
            });
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let pending = self.stream_pending_raw(key, meta.version, group);
        let mut by_consumer: BTreeMap<String, usize> = BTreeMap::new();
        for (_, pel) in &pending {
            *by_consumer.entry(pel.consumer.clone()).or_default() += 1;
        }
        Ok(StreamPendingSummary {
            total: pending.len(),
            smallest_id: pending.first().map(|(id, _)| id.to_redis_id()),
            greatest_id: pending.last().map(|(id, _)| id.to_redis_id()),
            consumers: by_consumer.into_iter().collect(),
        })
    }

    pub async fn stream_pending_summary_async(
        &self,
        key: &str,
        group: &str,
    ) -> Result<StreamPendingSummary, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(StreamPendingSummary {
                total: 0,
                smallest_id: None,
                greatest_id: None,
                consumers: Vec::new(),
            });
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let pending = self
            .stream_pending_raw_async(key, meta.version, group)
            .await;
        let mut by_consumer: BTreeMap<String, usize> = BTreeMap::new();
        for (_, pel) in &pending {
            *by_consumer.entry(pel.consumer.clone()).or_default() += 1;
        }
        Ok(StreamPendingSummary {
            total: pending.len(),
            smallest_id: pending.first().map(|(id, _)| id.to_redis_id()),
            greatest_id: pending.last().map(|(id, _)| id.to_redis_id()),
            consumers: by_consumer.into_iter().collect(),
        })
    }

    pub fn stream_pending_range(
        &self,
        key: &str,
        group: &str,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&str>,
    ) -> Result<Vec<StreamPendingEntry>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        Ok(self
            .stream_pending_raw(key, meta.version, group)
            .into_iter()
            .filter(|(id, pel)| {
                *id >= start && *id <= end && consumer.is_none_or(|name| pel.consumer == name)
            })
            .take(count)
            .map(|(id, pel)| StreamPendingEntry {
                id: id.to_redis_id(),
                consumer: pel.consumer,
                idle_ms: now.saturating_sub(pel.last_delivery_ms),
                deliveries: pel.deliveries,
            })
            .collect())
    }

    pub async fn stream_pending_range_async(
        &self,
        key: &str,
        group: &str,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&str>,
    ) -> Result<Vec<StreamPendingEntry>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        Ok(self
            .stream_pending_raw_async(key, meta.version, group)
            .await
            .into_iter()
            .filter(|(id, pel)| {
                *id >= start && *id <= end && consumer.is_none_or(|name| pel.consumer == name)
            })
            .take(count)
            .map(|(id, pel)| StreamPendingEntry {
                id: id.to_redis_id(),
                consumer: pel.consumer,
                idle_ms: now.saturating_sub(pel.last_delivery_ms),
                deliveries: pel.deliveries,
            })
            .collect())
    }

    pub fn stream_claim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[StreamId],
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut claimed = Vec::new();
        let mut batch = WriteBatch::new();
        for id in ids {
            let pel_key = stream_pel_key(self.db_index, key, meta.version, group, *id);
            let Some(raw) = self.store.get_raw(&pel_key) else {
                continue;
            };
            let Some(mut pel) = decode_stream_pel_state(&raw) else {
                continue;
            };
            if now.saturating_sub(pel.last_delivery_ms) < min_idle_ms {
                continue;
            }
            let Some(entry) = self.stream_entry_by_id(key, meta.version, *id) else {
                continue;
            };
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            batch.put(&pel_key, &encode_stream_pel_state(&pel));
            claimed.push(entry);
        }
        if batch.count() > 0 {
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(claimed)
    }

    pub async fn stream_claim_async(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[StreamId],
    ) -> Result<Vec<StreamEntry>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut claimed = Vec::new();
        let mut batch = WriteBatch::new();
        for id in ids {
            let pel_key = stream_pel_key(self.db_index, key, meta.version, group, *id);
            let Some(raw) = self.store.get_raw_async(&pel_key).await else {
                continue;
            };
            let Some(mut pel) = decode_stream_pel_state(&raw) else {
                continue;
            };
            if now.saturating_sub(pel.last_delivery_ms) < min_idle_ms {
                continue;
            }
            let Some(entry) = self.stream_entry_by_id_async(key, meta.version, *id).await else {
                continue;
            };
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            batch.put(&pel_key, &encode_stream_pel_state(&pel));
            claimed.push(entry);
        }
        if batch.count() > 0 {
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(claimed)
    }

    pub fn stream_auto_claim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start: StreamId,
        count: usize,
    ) -> Result<StreamClaimedEntries, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(StreamClaimedEntries {
                next_id: "0-0".to_string(),
                entries: Vec::new(),
            });
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut entries = Vec::new();
        let mut next_id = StreamId { ms: 0, seq: 0 };
        let mut batch = WriteBatch::new();
        for (id, mut pel) in self.stream_pending_raw(key, meta.version, group) {
            if id < start {
                continue;
            }
            next_id = id;
            if now.saturating_sub(pel.last_delivery_ms) < min_idle_ms {
                continue;
            }
            let Some(entry) = self.stream_entry_by_id(key, meta.version, id) else {
                continue;
            };
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            batch.put(
                &stream_pel_key(self.db_index, key, meta.version, group, id),
                &encode_stream_pel_state(&pel),
            );
            entries.push(entry);
            if entries.len() >= count {
                break;
            }
        }
        if batch.count() > 0 {
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(StreamClaimedEntries {
            next_id: next_id.to_redis_id(),
            entries,
        })
    }

    pub async fn stream_auto_claim_async(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start: StreamId,
        count: usize,
    ) -> Result<StreamClaimedEntries, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(StreamClaimedEntries {
                next_id: "0-0".to_string(),
                entries: Vec::new(),
            });
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut entries = Vec::new();
        let mut next_id = StreamId { ms: 0, seq: 0 };
        let mut batch = WriteBatch::new();
        for (id, mut pel) in self
            .stream_pending_raw_async(key, meta.version, group)
            .await
        {
            if id < start {
                continue;
            }
            next_id = id;
            if now.saturating_sub(pel.last_delivery_ms) < min_idle_ms {
                continue;
            }
            let Some(entry) = self.stream_entry_by_id_async(key, meta.version, id).await else {
                continue;
            };
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            batch.put(
                &stream_pel_key(self.db_index, key, meta.version, group, id),
                &encode_stream_pel_state(&pel),
            );
            entries.push(entry);
            if entries.len() >= count {
                break;
            }
        }
        if batch.count() > 0 {
            batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            );
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(StreamClaimedEntries {
            next_id: next_id.to_redis_id(),
            entries,
        })
    }

    pub fn stream_groups(&self, key: &str) -> Result<Vec<StreamGroupInfo>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        let prefix = stream_group_prefix(self.db_index, key, meta.version);
        let mut groups = Vec::new();
        for (group_key, raw) in self.store.scan_prefix_raw(&prefix) {
            let name = String::from_utf8(group_key[prefix.len()..].to_vec()).unwrap_or_default();
            let Some(state) = decode_stream_group_state(&raw) else {
                continue;
            };
            let pending = self.stream_pending_raw(key, meta.version, &name);
            let mut consumers = self
                .stream_consumers_raw(key, meta.version, &name)
                .into_keys()
                .collect::<HashSet<_>>();
            consumers.extend(pending.iter().map(|(_, pel)| pel.consumer.clone()));
            let consumers = consumers.len();
            groups.push(StreamGroupInfo {
                name,
                consumers,
                pending: pending.len(),
                last_delivered_id: state.last_delivered_id.to_redis_id(),
                entries_read: state.entries_read,
            });
        }
        Ok(groups)
    }

    pub async fn stream_groups_async(&self, key: &str) -> Result<Vec<StreamGroupInfo>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        let prefix = stream_group_prefix(self.db_index, key, meta.version);
        let mut groups = Vec::new();
        for (group_key, raw) in self.store.scan_prefix_raw_async(&prefix).await {
            let name = String::from_utf8(group_key[prefix.len()..].to_vec()).unwrap_or_default();
            let Some(state) = decode_stream_group_state(&raw) else {
                continue;
            };
            let pending = self
                .stream_pending_raw_async(key, meta.version, &name)
                .await;
            let mut consumers = self
                .stream_consumers_raw_async(key, meta.version, &name)
                .await
                .into_keys()
                .collect::<HashSet<_>>();
            consumers.extend(pending.iter().map(|(_, pel)| pel.consumer.clone()));
            let consumers = consumers.len();
            groups.push(StreamGroupInfo {
                name,
                consumers,
                pending: pending.len(),
                last_delivered_id: state.last_delivered_id.to_redis_id(),
                entries_read: state.entries_read,
            });
        }
        Ok(groups)
    }

    pub fn stream_consumers(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Vec<StreamConsumerInfo>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(Vec::new());
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut consumers: BTreeMap<String, (usize, u64)> = self
            .stream_consumers_raw(key, meta.version, group)
            .into_iter()
            .map(|(name, state)| (name, (0, state.last_seen_ms)))
            .collect();
        for (_, pel) in self.stream_pending_raw(key, meta.version, group) {
            let entry = consumers
                .entry(pel.consumer)
                .or_insert((0, pel.last_delivery_ms));
            entry.0 += 1;
        }
        Ok(consumers
            .into_iter()
            .map(|(name, (pending, last_seen))| StreamConsumerInfo {
                name,
                pending,
                idle_ms: now.saturating_sub(last_seen),
            })
            .collect())
    }

    pub async fn stream_consumers_async(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Vec<StreamConsumerInfo>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(Vec::new());
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let now = now_ms();
        let mut consumers: BTreeMap<String, (usize, u64)> = self
            .stream_consumers_raw_async(key, meta.version, group)
            .await
            .into_iter()
            .map(|(name, state)| (name, (0, state.last_seen_ms)))
            .collect();
        for (_, pel) in self
            .stream_pending_raw_async(key, meta.version, group)
            .await
        {
            let entry = consumers
                .entry(pel.consumer)
                .or_insert((0, pel.last_delivery_ms));
            entry.0 += 1;
        }
        Ok(consumers
            .into_iter()
            .map(|(name, (pending, last_seen))| StreamConsumerInfo {
                name,
                pending,
                idle_ms: now.saturating_sub(last_seen),
            })
            .collect())
    }

}
