use super::*;

impl Db {
    pub(in crate::store::db) fn promote_packed_stream(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some((mut meta, entries)) = decode_packed_stream(raw) else {
                return Ok(());
            };
            meta.version = self.next_version();
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encode_stream_meta(meta))?;
            for (id, value) in entries {
                batch.put(
                    &stream_entry_key(self.db_index, key, meta.version, id),
                    &value,
                )?;
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                return Ok(());
            }
        }
        Err(Error::msg("ERR stream layout promotion conflict"))
    }

    pub(in crate::store::db) async fn promote_packed_stream_async(
        &self,
        key: &str,
    ) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some((mut meta, entries)) = decode_packed_stream(raw) else {
                return Ok(());
            };
            meta.version = self.next_version_async().await;
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encode_stream_meta(meta))?;
            for (id, value) in entries {
                batch.put(
                    &stream_entry_key(self.db_index, key, meta.version, id),
                    &value,
                )?;
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::msg("ERR stream layout promotion conflict"))
    }

    pub(in crate::store::db) fn stream_meta(&self, key: &str) -> Result<Option<StreamMeta>, Error> {
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        if let Some(meta) = decode_stream_meta(&raw) {
            return Ok(Some(meta));
        }

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode stream metadata"));
        };
        if header.type_tag != TYPE_STREAM {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Err(Error::msg("Failed to decode stream metadata"))
    }

    pub(in crate::store::db) async fn stream_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<StreamMeta>, Error> {
        self.expire_if_needed_async(key).await;
        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        if let Some(meta) = decode_stream_meta(&raw) {
            return Ok(Some(meta));
        }

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode stream metadata"));
        };
        if header.type_tag != TYPE_STREAM {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Err(Error::msg("Failed to decode stream metadata"))
    }

    pub(in crate::store::db) fn next_stream_id(&self, last_id: StreamId) -> StreamId {
        let now = now_ms();
        if now > last_id.ms {
            StreamId { ms: now, seq: 0 }
        } else {
            StreamId {
                ms: last_id.ms,
                seq: last_id.seq.saturating_add(1),
            }
        }
    }

    pub(in crate::store::db) fn stream_entries_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(StreamId, Vec<u8>)> {
        if version == 0 {
            return self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_stream)
                .map(|(_, entries)| entries)
                .unwrap_or_default();
        }
        let prefix = stream_entry_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(entry_key, value)| {
                decode_stream_entry_id(&prefix, &entry_key).map(|id| (id, value))
            })
            .collect()
    }

    pub(in crate::store::db) async fn stream_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(StreamId, Vec<u8>)> {
        if version == 0 {
            return self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_stream)
                .map(|(_, entries)| entries)
                .unwrap_or_default();
        }
        let prefix = stream_entry_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(entry_key, value)| {
                decode_stream_entry_id(&prefix, &entry_key).map(|id| (id, value))
            })
            .collect()
    }

    pub(in crate::store::db) fn stream_entries_between(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
    ) -> Vec<StreamEntry> {
        self.stream_entries_raw(key, version)
            .into_iter()
            .filter(|(id, _)| *id >= start && *id <= end)
            .filter_map(|(id, value)| {
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    pub(in crate::store::db) async fn stream_entries_between_async(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
    ) -> Vec<StreamEntry> {
        self.stream_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter(|(id, _)| *id >= start && *id <= end)
            .filter_map(|(id, value)| {
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    /// Scan only the requested inclusive stream-ID window and stop after `limit` entries.
    /// Stream entry IDs are the final, big-endian 16 bytes of the storage key, so the Redis
    /// ordering and the storage ordering are identical.
    pub(in crate::store::db) async fn stream_entries_between_limited_async(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
        limit: usize,
    ) -> Vec<StreamEntry> {
        if limit == 0 || start > end {
            return Vec::new();
        }
        if version == 0 {
            return self
                .stream_entries_between_async(key, version, start, end)
                .await
                .into_iter()
                .take(limit)
                .collect();
        }
        let prefix = stream_entry_prefix(self.db_index, key, version);
        let lower = stream_entry_key(self.db_index, key, version, start);
        let upper = if end.ms == u64::MAX && end.seq == u64::MAX {
            prefix_exclusive_upper_bound(&prefix)
        } else {
            prefix_exclusive_upper_bound(&stream_entry_key(self.db_index, key, version, end))
        };
        self.store
            .scan_range_raw_limited_async(&lower, upper, limit)
            .await
            .into_iter()
            .filter_map(|(entry_key, value)| {
                let id = decode_stream_entry_id(&prefix, &entry_key)?;
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    /// Scan an inclusive stream-ID window in descending order and stop at `limit`.
    pub(in crate::store::db) fn stream_entries_between_limited_reverse(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
        limit: usize,
    ) -> Vec<StreamEntry> {
        if limit == 0 || start > end {
            return Vec::new();
        }
        if version == 0 {
            let mut entries = self.stream_entries_between(key, version, start, end);
            entries.reverse();
            entries.truncate(limit);
            return entries;
        }
        let prefix = stream_entry_prefix(self.db_index, key, version);
        let lower = stream_entry_key(self.db_index, key, version, start);
        let upper = if end.ms == u64::MAX && end.seq == u64::MAX {
            prefix_exclusive_upper_bound(&prefix)
        } else {
            prefix_exclusive_upper_bound(&stream_entry_key(self.db_index, key, version, end))
        };
        self.store
            .scan_range_raw_limited_reverse(&lower, upper, limit)
            .into_iter()
            .filter_map(|(entry_key, value)| {
                let id = decode_stream_entry_id(&prefix, &entry_key)?;
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    /// Async counterpart of the reverse bounded stream scan.
    pub(in crate::store::db) async fn stream_entries_between_limited_reverse_async(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
        limit: usize,
    ) -> Vec<StreamEntry> {
        if limit == 0 || start > end {
            return Vec::new();
        }
        if version == 0 {
            let mut entries = self
                .stream_entries_between_async(key, version, start, end)
                .await;
            entries.reverse();
            entries.truncate(limit);
            return entries;
        }
        let prefix = stream_entry_prefix(self.db_index, key, version);
        let lower = stream_entry_key(self.db_index, key, version, start);
        let upper = if end.ms == u64::MAX && end.seq == u64::MAX {
            prefix_exclusive_upper_bound(&prefix)
        } else {
            prefix_exclusive_upper_bound(&stream_entry_key(self.db_index, key, version, end))
        };
        self.store
            .scan_range_raw_limited_reverse_async(&lower, upper, limit)
            .await
            .into_iter()
            .filter_map(|(entry_key, value)| {
                let id = decode_stream_entry_id(&prefix, &entry_key)?;
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    pub(in crate::store::db) fn stream_entry_by_id(
        &self,
        key: &str,
        version: u64,
        id: StreamId,
    ) -> Option<StreamEntry> {
        if version == 0 {
            let (_, entries) = self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_stream)?;
            let (_, raw) = entries.into_iter().find(|(entry_id, _)| *entry_id == id)?;
            return Some(StreamEntry {
                id: id.to_redis_id(),
                fields: decode_stream_entry(&raw)?,
            });
        }
        let raw = self
            .store
            .get_raw(&stream_entry_key(self.db_index, key, version, id))?;
        Some(StreamEntry {
            id: id.to_redis_id(),
            fields: decode_stream_entry(&raw)?,
        })
    }

    pub(in crate::store::db) async fn stream_entry_by_id_async(
        &self,
        key: &str,
        version: u64,
        id: StreamId,
    ) -> Option<StreamEntry> {
        if version == 0 {
            let (_, entries) = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_stream)?;
            let (_, raw) = entries.into_iter().find(|(entry_id, _)| *entry_id == id)?;
            return Some(StreamEntry {
                id: id.to_redis_id(),
                fields: decode_stream_entry(&raw)?,
            });
        }
        let raw = self
            .store
            .get_raw_async(&stream_entry_key(self.db_index, key, version, id))
            .await?;
        Some(StreamEntry {
            id: id.to_redis_id(),
            fields: decode_stream_entry(&raw)?,
        })
    }

    pub(in crate::store::db) fn stream_group_state(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Option<StreamGroupState>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(None);
        };
        Ok(self
            .store
            .get_raw(&stream_group_key(self.db_index, key, meta.version, group))
            .and_then(|raw| decode_stream_group_state(&raw)))
    }

    pub(in crate::store::db) async fn stream_group_state_async(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Option<StreamGroupState>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(None);
        };
        Ok(self
            .store
            .get_raw_async(&stream_group_key(self.db_index, key, meta.version, group))
            .await
            .and_then(|raw| decode_stream_group_state(&raw)))
    }

    pub(in crate::store::db) fn stream_pending_raw(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> Vec<(StreamId, StreamPelState)> {
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }

    pub(in crate::store::db) fn stream_consumers_raw(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> BTreeMap<String, StreamConsumerState> {
        let prefix = stream_consumer_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(consumer_key, raw)| {
                let suffix = consumer_key.strip_prefix(prefix.as_slice())?;
                let name = String::from_utf8(suffix.to_vec()).ok()?;
                let state = decode_stream_consumer_state(&raw)?;
                Some((name, state))
            })
            .collect()
    }

    pub(in crate::store::db) async fn stream_consumers_raw_async(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> BTreeMap<String, StreamConsumerState> {
        let prefix = stream_consumer_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(consumer_key, raw)| {
                let suffix = consumer_key.strip_prefix(prefix.as_slice())?;
                let name = String::from_utf8(suffix.to_vec()).ok()?;
                let state = decode_stream_consumer_state(&raw)?;
                Some((name, state))
            })
            .collect()
    }

    pub(in crate::store::db) async fn stream_pending_raw_async(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> Vec<(StreamId, StreamPelState)> {
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }

    /// Read a bounded inclusive PEL ID window without scanning entries before `start`.
    pub(in crate::store::db) fn stream_pending_between_limited(
        &self,
        key: &str,
        version: u64,
        group: &str,
        start: StreamId,
        end: StreamId,
        limit: usize,
    ) -> Vec<(StreamId, StreamPelState)> {
        if limit == 0 || start > end {
            return Vec::new();
        }
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        let lower = stream_pel_key(self.db_index, key, version, group, start);
        let upper = if end.ms == u64::MAX && end.seq == u64::MAX {
            prefix_exclusive_upper_bound(&prefix)
        } else {
            prefix_exclusive_upper_bound(&stream_pel_key(self.db_index, key, version, group, end))
        };
        self.store
            .scan_range_raw_limited(&lower, upper, limit)
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }

    /// Async counterpart of the bounded PEL scan.
    pub(in crate::store::db) async fn stream_pending_between_limited_async(
        &self,
        key: &str,
        version: u64,
        group: &str,
        start: StreamId,
        end: StreamId,
        limit: usize,
    ) -> Vec<(StreamId, StreamPelState)> {
        if limit == 0 || start > end {
            return Vec::new();
        }
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        let lower = stream_pel_key(self.db_index, key, version, group, start);
        let upper = if end.ms == u64::MAX && end.seq == u64::MAX {
            prefix_exclusive_upper_bound(&prefix)
        } else {
            prefix_exclusive_upper_bound(&stream_pel_key(self.db_index, key, version, group, end))
        };
        self.store
            .scan_range_raw_limited_async(&lower, upper, limit)
            .await
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }
}

pub(in crate::store::db) fn stream_id_successor(id: StreamId) -> Option<StreamId> {
    if id.seq < u64::MAX {
        Some(StreamId {
            ms: id.ms,
            seq: id.seq + 1,
        })
    } else if id.ms < u64::MAX {
        Some(StreamId {
            ms: id.ms + 1,
            seq: 0,
        })
    } else {
        None
    }
}
