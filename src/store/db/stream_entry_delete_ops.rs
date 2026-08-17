use super::*;

impl Db {
    /// Apply ordered XDEL commands with one metadata read, one entry multi-get, and one write per
    /// pipeline. An ID deleted by an earlier command returns zero in later commands, matching the
    /// observable sequential command semantics.
    pub(crate) async fn stream_delete_batch_async(
        &self,
        commands: &[(&str, Vec<StreamId>)],
    ) -> Vec<Result<usize, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }

        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::with_capacity(commands.len());
        for (key, _) in commands {
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

        let mut candidate_ids = vec![BTreeSet::<StreamId>::new(); keys.len()];
        for (key, ids) in commands {
            let position = key_positions[key];
            if states[position].as_ref().is_ok_and(Option::is_some) {
                candidate_ids[position].extend(ids.iter().copied());
            }
        }
        let mut entry_lookups = Vec::new();
        for (position, ids) in candidate_ids.iter().enumerate() {
            let Ok(Some(meta)) = states[position] else {
                continue;
            };
            entry_lookups.extend(ids.iter().map(|id| {
                (
                    position,
                    *id,
                    stream_entry_key(self.db_index, keys[position], meta.version, *id),
                )
            }));
        }
        let entry_keys = entry_lookups
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect::<Vec<_>>();
        let entry_values = self.store.multi_get_raw_async(&entry_keys).await;
        let mut existing_ids = vec![BTreeSet::<StreamId>::new(); keys.len()];
        for ((position, id, _), value) in entry_lookups.iter().zip(entry_values) {
            if value.is_some() {
                existing_ids[*position].insert(*id);
            }
        }

        let mut deleted_ids = vec![BTreeSet::<StreamId>::new(); keys.len()];
        let mut deleted_per_key = vec![0usize; keys.len()];
        let mut changed_commands = 0u64;
        let mut replies = Vec::with_capacity(commands.len());
        for (key, ids) in commands {
            let position = key_positions[key];
            let reply = match &states[position] {
                Err(error) => Err(Error::msg(error.to_string())),
                Ok(None) => Ok(0),
                Ok(Some(_)) => {
                    let mut seen = BTreeSet::new();
                    let mut deleted = 0usize;
                    for id in ids {
                        if seen.insert(*id) && existing_ids[position].remove(id) {
                            deleted_ids[position].insert(*id);
                            deleted += 1;
                        }
                    }
                    deleted_per_key[position] += deleted;
                    if deleted > 0 {
                        changed_commands += 1;
                    }
                    Ok(deleted)
                }
            };
            replies.push(reply);
        }
        if changed_commands == 0 {
            return replies;
        }

        let mut batch = WriteBatch::new();
        for (position, ids) in deleted_ids.iter().enumerate() {
            let Ok(Some(meta)) = states[position] else {
                continue;
            };
            for id in ids {
                (batch.delete(&stream_entry_key(
                    self.db_index,
                    keys[position],
                    meta.version,
                    *id,
                )))
                .expect("write batch append invariant violated");
            }
            if deleted_per_key[position] > 0 {
                let mut updated = meta;
                updated.length = updated
                    .length
                    .saturating_sub(deleted_per_key[position] as u64);
                (batch.put(&self.mk(keys[position]), &encode_stream_meta(updated)))
                    .expect("write batch append invariant violated");
            }
        }
        self.write_existing_version_batch_if_not_empty_async(&batch)
            .await;
        self.changes.fetch_add(changed_commands, Ordering::Relaxed);
        replies
    }

    pub fn stream_delete(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        for id in ids {
            if !seen_ids.insert(*id) {
                continue;
            }
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            if self.store.get_raw(&entry_key).is_some() {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub async fn stream_delete_async(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        self.stream_delete_async_unlocked(key, ids).await
    }

    async fn stream_delete_async_unlocked(
        &self,
        key: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        let unique_ids = ids
            .iter()
            .copied()
            .filter(|id| seen_ids.insert(*id))
            .collect::<Vec<_>>();
        let entry_keys = unique_ids
            .iter()
            .map(|id| stream_entry_key(self.db_index, key, meta.version, *id))
            .collect::<Vec<_>>();
        let existing = self.store.multi_get_raw_async(&entry_keys).await;
        for (entry_key, old_raw) in entry_keys.into_iter().zip(existing) {
            if old_raw.is_some() {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
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
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stream_set_id_async(&self, key: &str, id: StreamId) -> Result<(), Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let mut meta = self
            .stream_meta_async(key)
            .await?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        meta.last_id = id;
        let mut batch = WriteBatch::new();
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn stream_ack_delete(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<Vec<i64>, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(vec![-1; ids.len()]);
        };
        self.stream_group_state(key, group)?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;

        let mut statuses = Vec::with_capacity(ids.len());
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_entry_ids = std::collections::BTreeSet::new();
        let mut seen_pending_ids = std::collections::BTreeSet::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            let exists = self.store.get_raw(&entry_key).is_some();
            statuses.push(if exists { 1 } else { -1 });
            if exists && seen_entry_ids.insert(*id) {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }

            if seen_pending_ids.insert(*id) {
                let pending_key = stream_pel_key(self.db_index, key, meta.version, group, *id);
                if self.store.get_raw(&pending_key).is_some() {
                    (batch.delete(&pending_key)).expect("write batch append invariant violated");
                }
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(statuses)
    }

    pub async fn stream_ack_delete_async(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<Vec<i64>, Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(vec![-1; ids.len()]);
        };
        self.stream_group_state_async(key, group)
            .await?
            .ok_or_else(|| Error::msg("NOGROUP No such key or consumer group"))?;

        let entry_keys = ids
            .iter()
            .map(|id| stream_entry_key(self.db_index, key, meta.version, *id))
            .collect::<Vec<_>>();
        let pending_keys = ids
            .iter()
            .map(|id| stream_pel_key(self.db_index, key, meta.version, group, *id))
            .collect::<Vec<_>>();
        let mut lookup_keys = entry_keys.clone();
        lookup_keys.extend(pending_keys.iter().cloned());
        let mut lookup_values = self.store.multi_get_raw_async(&lookup_keys).await;
        let pending_values = lookup_values.split_off(entry_keys.len());
        let entry_values = lookup_values;
        let mut statuses = Vec::with_capacity(ids.len());
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_entry_ids = std::collections::BTreeSet::new();
        let mut seen_pending_ids = std::collections::BTreeSet::new();
        for (((id, entry_key), entry_value), (pending_key, pending_value)) in ids
            .iter()
            .zip(entry_keys)
            .zip(entry_values)
            .zip(pending_keys.into_iter().zip(pending_values))
        {
            let exists = entry_value.is_some();
            statuses.push(if exists { 1 } else { -1 });
            if exists && seen_entry_ids.insert(*id) {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }

            if seen_pending_ids.insert(*id) && pending_value.is_some() {
                (batch.delete(&pending_key)).expect("write batch append invariant violated");
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(statuses)
    }

    pub fn stream_delete_with_statuses(
        &self,
        key: &str,
        ids: &[StreamId],
    ) -> Result<Vec<i64>, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(vec![-1; ids.len()]);
        };
        let mut statuses = Vec::with_capacity(ids.len());
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            let exists = self.store.get_raw(&entry_key).is_some();
            statuses.push(if exists { 1 } else { -1 });
            if exists && seen_ids.insert(*id) {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(statuses)
    }

    pub async fn stream_delete_with_statuses_async(
        &self,
        key: &str,
        ids: &[StreamId],
    ) -> Result<Vec<i64>, Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(vec![-1; ids.len()]);
        };
        let entry_keys = ids
            .iter()
            .map(|id| stream_entry_key(self.db_index, key, meta.version, *id))
            .collect::<Vec<_>>();
        let entry_values = self.store.multi_get_raw_async(&entry_keys).await;
        let mut statuses = Vec::with_capacity(ids.len());
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        for ((id, entry_key), value) in ids.iter().zip(entry_keys).zip(entry_values) {
            let exists = value.is_some();
            statuses.push(if exists { 1 } else { -1 });
            if exists && seen_ids.insert(*id) {
                (batch.delete(&entry_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            (batch.put(&self.mk(key), &encode_stream_meta(meta)))
                .expect("write batch append invariant violated");
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(statuses)
    }
}
