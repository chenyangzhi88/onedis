use super::*;

impl Db {
    pub fn set_add(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.set_meta(key)?;
        let version = match meta {
            Some(meta) => meta.version,
            None => self.next_persisted_version(),
        };
        let mut batch = WriteBatch::new();
        let mut added = 0usize;
        let mut seen_in_batch = std::collections::HashSet::new();

        for member in members {
            if !seen_in_batch.insert(member.clone()) {
                continue;
            }
            let member_key = set_member_key(self.db_index, key, version, member);
            if !self.store.contains_key(&member_key) {
                batch.put(&member_key, INDEX_MARKER_VALUE);
                self.set_slot_add_to_batch(
                    &mut batch,
                    key,
                    version,
                    meta.map_or(0, |meta| meta.len).saturating_add(added) as u64,
                    member.as_bytes(),
                );
                added += 1;
            }
        }

        if added > 0 || meta.is_none() {
            let expire_ms = meta.map_or(0, |meta| meta.expire_ms);
            let len = meta.map_or(0, |meta| meta.len).saturating_add(added);
            batch.put(&self.mk(key), &encode_set_meta(expire_ms, version, len));
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(added)
    }

    pub async fn set_add_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let _set_write_guard = self.set_write_lock(key).lock().await;
        self.set_add_async_unlocked(key, members).await
    }

    pub(in crate::store::db) async fn set_add_async_unlocked(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<usize, Error> {
        let meta = self.set_meta_async(key).await?;
        let version = match meta {
            Some(meta) => meta.version,
            None => self.next_persisted_version_async().await,
        };
        let mut batch = WriteBatch::new();
        let mut added = 0usize;
        let mut seen_in_batch = std::collections::HashSet::new();
        let mut member_keys = Vec::new();
        let mut unique_members = Vec::new();

        for member in members {
            if !seen_in_batch.insert(member.clone()) {
                continue;
            }
            let member_key = set_member_key(self.db_index, key, version, member);
            member_keys.push(member_key);
            unique_members.push(member.as_bytes().to_vec());
        }

        let existing = self.store.multi_get_raw_async(&member_keys).await;
        for ((member_key, member), old_raw) in
            member_keys.into_iter().zip(unique_members).zip(existing)
        {
            if old_raw.is_none() {
                batch.put(&member_key, INDEX_MARKER_VALUE);
                self.set_slot_add_to_batch(
                    &mut batch,
                    key,
                    version,
                    meta.map_or(0, |meta| meta.len).saturating_add(added) as u64,
                    &member,
                );
                added += 1;
            }
        }

        if added > 0 || meta.is_none() {
            let expire_ms = meta.map_or(0, |meta| meta.expire_ms);
            let len = meta.map_or(0, |meta| meta.len).saturating_add(added);
            batch.put(&self.mk(key), &encode_set_meta(expire_ms, version, len));
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(added)
    }

    /// 删除 set members，返回实际删除数量。
    pub fn set_remove(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(0);
        };
        let meta = self.ensure_set_slot_index(key, meta);

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut seen_in_batch = std::collections::HashSet::new();
        let unique_members: Vec<&String> = members
            .iter()
            .filter(|member| seen_in_batch.insert((*member).clone()))
            .collect();
        let rebuild_after_remove = unique_members.len() > 1;
        if unique_members.len() == 1 {
            let member = unique_members[0];
            if self.set_slot_remove_to_batch(
                &mut batch,
                key,
                meta.version,
                meta.len,
                member.as_bytes(),
            ) {
                deleted = 1;
            }
        } else {
            for member in unique_members {
                let member_key = set_member_key(self.db_index, key, meta.version, member);
                if self.store.contains_key(&member_key) {
                    batch.delete(&member_key);
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            let len = meta.len.saturating_sub(deleted);
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                );
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        if deleted > 0 && rebuild_after_remove && meta.len > deleted {
            self.rebuild_set_slot_index(
                key,
                SetMeta {
                    len: meta.len.saturating_sub(deleted),
                    ..meta
                },
            );
        }
        Ok(deleted)
    }

    pub async fn set_remove_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let _set_write_guard = self.set_write_lock(key).lock().await;
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(0);
        };
        let meta = self.ensure_set_slot_index_async(key, meta).await;

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut seen_in_batch = std::collections::HashSet::new();
        let unique_members: Vec<&String> = members
            .iter()
            .filter(|member| seen_in_batch.insert((*member).clone()))
            .collect();
        let rebuild_after_remove = unique_members.len() > 1;
        if unique_members.len() == 1 {
            let member = unique_members[0];
            if self
                .set_slot_remove_to_batch_async(
                    &mut batch,
                    key,
                    meta.version,
                    meta.len,
                    member.as_bytes(),
                )
                .await
            {
                deleted = 1;
            }
        } else {
            let member_keys: Vec<Vec<u8>> = unique_members
                .iter()
                .map(|member| set_member_key(self.db_index, key, meta.version, member))
                .collect();
            let existing = self.store.multi_get_raw_async(&member_keys).await;
            for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
                if old_raw.is_some() {
                    batch.delete(&member_key);
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            let len = meta.len.saturating_sub(deleted);
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                );
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        if deleted > 0 && rebuild_after_remove && meta.len > deleted {
            self.rebuild_set_slot_index_async(
                key,
                SetMeta {
                    len: meta.len.saturating_sub(deleted),
                    ..meta
                },
            )
            .await;
        }
        Ok(deleted)
    }

    pub fn set_move(&self, source: &str, destination: &str, member: &str) -> Result<bool, Error> {
        if source == destination {
            return self.set_contains(source, member);
        }
        let Some(source_meta) = self.set_meta(source)? else {
            return Ok(false);
        };
        let destination_meta = self
            .set_meta(destination)?
            .map(|meta| self.ensure_set_slot_index(destination, meta));
        let source_meta = self.ensure_set_slot_index(source, source_meta);
        let mut batch = WriteBatch::new();
        if !self.set_slot_remove_to_batch(
            &mut batch,
            source,
            source_meta.version,
            source_meta.len,
            member.as_bytes(),
        ) {
            return Ok(false);
        }

        let source_len = source_meta.len.saturating_sub(1);
        if source_len == 0 {
            batch.delete(&self.mk(source));
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                source,
                source_meta.version,
                TYPE_SET,
            );
            if source_meta.expire_ms > 0 {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    source_meta.expire_ms,
                    self.db_index,
                    source,
                );
            }
        } else {
            batch.put(
                &self.mk(source),
                &encode_set_meta(source_meta.expire_ms, source_meta.version, source_len),
            );
        }

        let destination_version = destination_meta
            .map(|meta| meta.version)
            .unwrap_or_else(|| self.next_persisted_version());
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self.store.contains_key(&destination_member_key) {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            batch.put(&destination_member_key, INDEX_MARKER_VALUE);
            self.set_slot_add_to_batch(
                &mut batch,
                destination,
                destination_version,
                destination_len as u64,
                member.as_bytes(),
            );
            batch.put(
                &self.mk(destination),
                &encode_set_meta(
                    destination_meta.map_or(0, |meta| meta.expire_ms),
                    destination_version,
                    destination_len.saturating_add(1),
                ),
            );
        }

        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    pub async fn set_move_async(
        &self,
        source: &str,
        destination: &str,
        member: &str,
    ) -> Result<bool, Error> {
        let source_shard = set_write_lock_shard(self.db_index, source);
        let destination_shard = set_write_lock_shard(self.db_index, destination);
        if source_shard == destination_shard {
            let _guard = self.set_write_locks[source_shard].lock().await;
            self.set_move_async_unlocked(source, destination, member)
                .await
        } else if source_shard < destination_shard {
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            let _destination_guard = self.set_write_locks[destination_shard].lock().await;
            self.set_move_async_unlocked(source, destination, member)
                .await
        } else {
            let _destination_guard = self.set_write_locks[destination_shard].lock().await;
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            self.set_move_async_unlocked(source, destination, member)
                .await
        }
    }

    async fn set_move_async_unlocked(
        &self,
        source: &str,
        destination: &str,
        member: &str,
    ) -> Result<bool, Error> {
        if source == destination {
            return self.set_contains_async(source, member).await;
        }
        let Some(source_meta) = self.set_meta_async(source).await? else {
            return Ok(false);
        };
        let destination_meta = match self.set_meta_async(destination).await? {
            Some(meta) => Some(self.ensure_set_slot_index_async(destination, meta).await),
            None => None,
        };
        let source_meta = self.ensure_set_slot_index_async(source, source_meta).await;
        let mut batch = WriteBatch::new();
        if !self
            .set_slot_remove_to_batch_async(
                &mut batch,
                source,
                source_meta.version,
                source_meta.len,
                member.as_bytes(),
            )
            .await
        {
            return Ok(false);
        }

        let source_len = source_meta.len.saturating_sub(1);
        if source_len == 0 {
            batch.delete(&self.mk(source));
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                source,
                source_meta.version,
                TYPE_SET,
            );
            if source_meta.expire_ms > 0 {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    source_meta.expire_ms,
                    self.db_index,
                    source,
                );
            }
        } else {
            batch.put(
                &self.mk(source),
                &encode_set_meta(source_meta.expire_ms, source_meta.version, source_len),
            );
        }

        let destination_version = match destination_meta {
            Some(meta) => meta.version,
            None => self.next_persisted_version_async().await,
        };
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self.store.contains_key_async(&destination_member_key).await {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            batch.put(&destination_member_key, INDEX_MARKER_VALUE);
            self.set_slot_add_to_batch(
                &mut batch,
                destination,
                destination_version,
                destination_len as u64,
                member.as_bytes(),
            );
            batch.put(
                &self.mk(destination),
                &encode_set_meta(
                    destination_meta.map_or(0, |meta| meta.expire_ms),
                    destination_version,
                    destination_len.saturating_add(1),
                ),
            );
        }

        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}
