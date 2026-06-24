impl Db {
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

}
