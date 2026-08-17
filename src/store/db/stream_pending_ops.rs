use super::*;

impl Db {
    pub fn stream_ack(&self, key: &str, group: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;
        let mut acked = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        for id in ids {
            if !seen_ids.insert(*id) {
                continue;
            }
            let key = stream_pel_key(self.db_index, key, meta.version, group, *id);
            if self.store.get_raw(&key).is_some() {
                (batch.delete(&key)).expect("write batch append invariant violated");
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
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        self.stream_ack_async_unlocked(key, group, ids).await
    }

    pub(in crate::store::db) async fn stream_ack_async_unlocked(
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
        let mut seen_ids = std::collections::BTreeSet::new();
        let pel_keys = ids
            .iter()
            .copied()
            .filter(|id| seen_ids.insert(*id))
            .map(|id| stream_pel_key(self.db_index, key, meta.version, group, id))
            .collect::<Vec<_>>();
        let existing = self.store.multi_get_raw_async(&pel_keys).await;
        for (pel_key, value) in pel_keys.into_iter().zip(existing) {
            if value.is_some() {
                (batch.delete(&pel_key)).expect("write batch append invariant violated");
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
        let mut entries = Vec::with_capacity(count.min(1024));
        let mut scan_start = start;
        while entries.len() < count && scan_start <= end {
            let scan_limit = if consumer.is_some() {
                count
                    .saturating_sub(entries.len())
                    .saturating_mul(2)
                    .clamp(64, 1024)
            } else {
                count.saturating_sub(entries.len())
            };
            let pending = self.stream_pending_between_limited(
                key,
                meta.version,
                group,
                scan_start,
                end,
                scan_limit,
            );
            if pending.is_empty() {
                break;
            }
            let exhausted = pending.len() < scan_limit;
            let last_id = pending.last().map(|(id, _)| *id);
            entries.extend(
                pending
                    .into_iter()
                    .filter(|(_, pel)| consumer.is_none_or(|name| pel.consumer == name))
                    .map(|(id, pel)| StreamPendingEntry {
                        id: id.to_redis_id(),
                        consumer: pel.consumer,
                        idle_ms: now.saturating_sub(pel.last_delivery_ms),
                        deliveries: pel.deliveries,
                    })
                    .take(count.saturating_sub(entries.len())),
            );
            if entries.len() >= count || exhausted {
                break;
            }
            let Some(next) = last_id.and_then(native_stream_helpers::stream_id_successor) else {
                break;
            };
            scan_start = next;
        }
        Ok(entries)
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
        let mut entries = Vec::with_capacity(count.min(1024));
        let mut scan_start = start;
        while entries.len() < count && scan_start <= end {
            let scan_limit = if consumer.is_some() {
                count
                    .saturating_sub(entries.len())
                    .saturating_mul(2)
                    .clamp(64, 1024)
            } else {
                count.saturating_sub(entries.len())
            };
            let pending = self
                .stream_pending_between_limited_async(
                    key,
                    meta.version,
                    group,
                    scan_start,
                    end,
                    scan_limit,
                )
                .await;
            if pending.is_empty() {
                break;
            }
            let exhausted = pending.len() < scan_limit;
            let last_id = pending.last().map(|(id, _)| *id);
            entries.extend(
                pending
                    .into_iter()
                    .filter(|(_, pel)| consumer.is_none_or(|name| pel.consumer == name))
                    .map(|(id, pel)| StreamPendingEntry {
                        id: id.to_redis_id(),
                        consumer: pel.consumer,
                        idle_ms: now.saturating_sub(pel.last_delivery_ms),
                        deliveries: pel.deliveries,
                    })
                    .take(count.saturating_sub(entries.len())),
            );
            if entries.len() >= count || exhausted {
                break;
            }
            let Some(next) = last_id.and_then(native_stream_helpers::stream_id_successor) else {
                break;
            };
            scan_start = next;
        }
        Ok(entries)
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
            (batch.put(&pel_key, &encode_stream_pel_state(&pel)))
                .expect("write batch append invariant violated");
            claimed.push(entry);
        }
        if batch.count() > 0 {
            (batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            ))
            .expect("write batch append invariant violated");
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
        let _stream_write_guard = self.set_write_lock(key).lock().await;
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
        let pel_keys = ids
            .iter()
            .map(|id| stream_pel_key(self.db_index, key, meta.version, group, *id))
            .collect::<Vec<_>>();
        let entry_keys = ids
            .iter()
            .map(|id| stream_entry_key(self.db_index, key, meta.version, *id))
            .collect::<Vec<_>>();
        let mut lookup_keys = pel_keys.clone();
        lookup_keys.extend(entry_keys);
        let mut lookup_values = self.store.multi_get_raw_async(&lookup_keys).await;
        let entry_values = lookup_values.split_off(pel_keys.len());
        let pel_values = lookup_values;
        for (((id, pel_key), raw), entry_raw) in
            ids.iter().zip(pel_keys).zip(pel_values).zip(entry_values)
        {
            let Some(raw) = raw else {
                continue;
            };
            let Some(mut pel) = decode_stream_pel_state(&raw) else {
                continue;
            };
            if now.saturating_sub(pel.last_delivery_ms) < min_idle_ms {
                continue;
            }
            let Some(entry_raw) = entry_raw else {
                continue;
            };
            let Some(fields) = decode_stream_entry(&entry_raw) else {
                continue;
            };
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            (batch.put(&pel_key, &encode_stream_pel_state(&pel)))
                .expect("write batch append invariant violated");
            claimed.push(StreamEntry {
                id: id.to_redis_id(),
                fields,
            });
        }
        if batch.count() > 0 {
            (batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            ))
            .expect("write batch append invariant violated");
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
        if count == 0 {
            return Ok(StreamClaimedEntries {
                next_id: start.to_redis_id(),
                entries: Vec::new(),
            });
        }
        let now = now_ms();
        let attempts = count.saturating_mul(10).max(1);
        let pending = self.stream_pending_between_limited(
            key,
            meta.version,
            group,
            start,
            StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            },
            attempts.saturating_add(1),
        );
        let mut entry_lookup = vec![None; pending.len().min(attempts)];
        let mut entry_keys = Vec::new();
        for (position, (id, pel)) in pending.iter().take(attempts).enumerate() {
            if now.saturating_sub(pel.last_delivery_ms) >= min_idle_ms {
                entry_lookup[position] = Some(entry_keys.len());
                entry_keys.push(stream_entry_key(self.db_index, key, meta.version, *id));
            }
        }
        let entry_values = self.store.multi_get_raw(&entry_keys);
        let mut entries = Vec::with_capacity(count);
        let mut processed = 0usize;
        let mut batch = WriteBatch::new();
        for (position, (id, pel)) in pending.iter().take(attempts).enumerate() {
            processed = position + 1;
            let Some(lookup) = entry_lookup[position] else {
                continue;
            };
            let Some(entry_raw) = entry_values[lookup].as_deref() else {
                continue;
            };
            let Some(fields) = decode_stream_entry(entry_raw) else {
                continue;
            };
            let mut pel = pel.clone();
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            (batch.put(
                &stream_pel_key(self.db_index, key, meta.version, group, *id),
                &encode_stream_pel_state(&pel),
            ))
            .expect("write batch append invariant violated");
            entries.push(StreamEntry {
                id: id.to_redis_id(),
                fields,
            });
            if entries.len() >= count {
                break;
            }
        }
        let next_id = pending
            .get(processed)
            .map(|(id, _)| *id)
            .unwrap_or(StreamId { ms: 0, seq: 0 });
        if batch.count() > 0 {
            (batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            ))
            .expect("write batch append invariant violated");
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
        let _stream_write_guard = self.set_write_lock(key).lock().await;
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
        if count == 0 {
            return Ok(StreamClaimedEntries {
                next_id: start.to_redis_id(),
                entries: Vec::new(),
            });
        }
        let now = now_ms();
        let attempts = count.saturating_mul(10).max(1);
        let pending = self
            .stream_pending_between_limited_async(
                key,
                meta.version,
                group,
                start,
                StreamId {
                    ms: u64::MAX,
                    seq: u64::MAX,
                },
                attempts.saturating_add(1),
            )
            .await;
        let mut entry_lookup = vec![None; pending.len().min(attempts)];
        let mut entry_keys = Vec::new();
        for (position, (id, pel)) in pending.iter().take(attempts).enumerate() {
            if now.saturating_sub(pel.last_delivery_ms) >= min_idle_ms {
                entry_lookup[position] = Some(entry_keys.len());
                entry_keys.push(stream_entry_key(self.db_index, key, meta.version, *id));
            }
        }
        let entry_values = self.store.multi_get_raw_async(&entry_keys).await;
        let mut entries = Vec::with_capacity(count);
        let mut processed = 0usize;
        let mut batch = WriteBatch::new();
        for (position, (id, pel)) in pending.iter().take(attempts).enumerate() {
            processed = position + 1;
            let Some(lookup) = entry_lookup[position] else {
                continue;
            };
            let Some(entry_raw) = entry_values[lookup].as_deref() else {
                continue;
            };
            let Some(fields) = decode_stream_entry(entry_raw) else {
                continue;
            };
            let mut pel = pel.clone();
            pel.consumer = consumer.to_string();
            pel.last_delivery_ms = now;
            pel.deliveries = pel.deliveries.saturating_add(1);
            (batch.put(
                &stream_pel_key(self.db_index, key, meta.version, group, *id),
                &encode_stream_pel_state(&pel),
            ))
            .expect("write batch append invariant violated");
            entries.push(StreamEntry {
                id: id.to_redis_id(),
                fields,
            });
            if entries.len() >= count {
                break;
            }
        }
        let next_id = pending
            .get(processed)
            .map(|(id, _)| *id)
            .unwrap_or(StreamId { ms: 0, seq: 0 });
        if batch.count() > 0 {
            (batch.put(
                &stream_consumer_key(self.db_index, key, meta.version, group, consumer),
                &encode_stream_consumer_state(&StreamConsumerState { last_seen_ms: now }),
            ))
            .expect("write batch append invariant violated");
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(StreamClaimedEntries {
            next_id: next_id.to_redis_id(),
            entries,
        })
    }
}
