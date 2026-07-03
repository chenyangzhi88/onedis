impl Db {
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
        global_metrics().record_stream_claim();
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
        global_metrics().record_stream_claim();
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
        global_metrics().record_stream_autoclaim();
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
        global_metrics().record_stream_autoclaim();
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
}
