use super::*;

impl Db {
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

    pub(in crate::store::db) fn stream_entry_by_id(
        &self,
        key: &str,
        version: u64,
        id: StreamId,
    ) -> Option<StreamEntry> {
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
}
