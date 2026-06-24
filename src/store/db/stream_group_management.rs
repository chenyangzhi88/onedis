impl Db {
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

}
