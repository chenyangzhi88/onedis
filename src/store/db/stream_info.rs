use super::*;

impl Db {
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

    pub fn stream_observability_snapshot(&self) -> StreamObservabilitySnapshot {
        let mut snapshot = StreamObservabilitySnapshot::default();
        let now = now_ms();
        for key in self.logical_keys() {
            let Some(raw) = self.store.get_raw(&self.mk(&key)) else {
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                continue;
            };
            if header.type_tag != TYPE_STREAM {
                continue;
            }
            if header.expire_ms > 0 && now >= header.expire_ms {
                continue;
            }
            let prefix = stream_group_prefix(self.db_index, &key, header.version);
            for (group_key, _) in self.store.scan_prefix_raw(&prefix) {
                let name =
                    String::from_utf8(group_key[prefix.len()..].to_vec()).unwrap_or_default();
                snapshot.groups += 1;
                snapshot.pending_entries +=
                    self.stream_pending_raw(&key, header.version, &name).len() as u64;
            }
        }
        snapshot
    }
}
