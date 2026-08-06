use super::*;

impl Db {
    pub fn set_add(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.set_meta(key)?;
        let version = match meta {
            Some(meta) => meta.version,
            None => self.next_version(),
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
        let key_bytes = self.mk(key);
        let raw_meta = self.store.get_raw_async(&key_bytes).await;
        let mut expired_header = None;
        let meta = match raw_meta.as_deref() {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    expired_header = Some(header);
                    None
                } else {
                    if header.type_tag != TYPE_SET {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    Some(
                        decode_set_meta(raw)
                            .ok_or_else(|| Error::msg("Failed to decode set metadata"))?,
                    )
                }
            }
            None => None,
        };
        let existing_version = meta.is_some();
        let version = match meta {
            Some(meta) => meta.version,
            None => self.next_version_async().await,
        };
        let mut batch = WriteBatch::new();
        if let Some(header) = expired_header {
            self.append_expiration_delete_to_batch(&mut batch, key, &key_bytes, header)?;
        }
        let mut added = 0usize;
        let mut seen_in_batch = std::collections::HashSet::with_capacity(members.len());
        let mut unique_members = Vec::with_capacity(members.len());

        for member in members {
            if !seen_in_batch.insert(member.as_str()) {
                continue;
            }
            unique_members.push(member.as_str());
        }

        if existing_version {
            let member_keys = unique_members
                .iter()
                .map(|member| set_member_key(self.db_index, key, version, member))
                .collect::<Vec<_>>();
            let existing = self.store.multi_get_raw_async(&member_keys).await;
            for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
                if old_raw.is_some() {
                    continue;
                }
                batch.put(&member_key, INDEX_MARKER_VALUE);
                added += 1;
            }
        } else {
            for member in unique_members {
                let member_key = set_member_key(self.db_index, key, version, member);
                batch.put(&member_key, INDEX_MARKER_VALUE);
                added += 1;
            }
        }

        if added > 0 || meta.is_none() {
            let expire_ms = meta.map_or(0, |meta| meta.expire_ms);
            let len = meta.map_or(0, |meta| meta.len).saturating_add(added);
            batch.put(&key_bytes, &encode_set_meta(expire_ms, version, len));
        }

        if batch.count() > 0 {
            if existing_version {
                self.write_existing_version_batch_if_not_empty_async(&batch)
                    .await;
            } else {
                self.write_batch_if_not_empty_async(&batch).await;
            }
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(header) = expired_header {
            self.refresh_after_expiration_delete(key, header.type_tag);
        }
        Ok(added)
    }

    /// 删除 set members，返回实际删除数量。
    pub fn set_remove(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(0);
        };

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut seen_in_batch = std::collections::HashSet::with_capacity(members.len());
        let unique_members = members
            .iter()
            .filter(|member| seen_in_batch.insert(member.as_str()))
            .collect::<Vec<_>>();
        let member_keys = unique_members
            .iter()
            .map(|member| set_member_key(self.db_index, key, meta.version, member))
            .collect::<Vec<_>>();
        let existing = self.store.multi_get_raw(&member_keys);
        for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
            if old_raw.is_some() {
                batch.delete(&member_key);
                deleted += 1;
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
        Ok(deleted)
    }

    pub async fn set_remove_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let _set_write_guard = self.set_write_lock(key).lock().await;
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(0);
        };

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut seen_in_batch = std::collections::HashSet::with_capacity(members.len());
        let unique_members = members
            .iter()
            .filter(|member| seen_in_batch.insert(member.as_str()))
            .collect::<Vec<_>>();
        let member_keys = unique_members
            .iter()
            .map(|member| set_member_key(self.db_index, key, meta.version, member))
            .collect::<Vec<_>>();
        let existing = self.store.multi_get_raw_async(&member_keys).await;
        for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
            if old_raw.is_some() {
                batch.delete(&member_key);
                deleted += 1;
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
        Ok(deleted)
    }

    pub fn set_move(&self, source: &str, destination: &str, member: &str) -> Result<bool, Error> {
        if source == destination {
            return self.set_contains(source, member);
        }
        let Some(source_meta) = self.set_meta(source)? else {
            return Ok(false);
        };
        let destination_meta = self.set_meta(destination)?;
        let source_member_key = set_member_key(self.db_index, source, source_meta.version, member);
        if !self.store.contains_key(&source_member_key) {
            return Ok(false);
        }
        let mut batch = WriteBatch::new();
        batch.delete(&source_member_key);

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
            .unwrap_or_else(|| self.next_version());
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self.store.contains_key(&destination_member_key) {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            batch.put(&destination_member_key, INDEX_MARKER_VALUE);
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
        let source_shard = key_write_lock_shard(self.db_index, source);
        let destination_shard = key_write_lock_shard(self.db_index, destination);
        if source_shard == destination_shard {
            let _guard = self.key_write_locks[source_shard].lock().await;
            self.set_move_async_unlocked(source, destination, member)
                .await
        } else if source_shard < destination_shard {
            let _source_guard = self.key_write_locks[source_shard].lock().await;
            let _destination_guard = self.key_write_locks[destination_shard].lock().await;
            self.set_move_async_unlocked(source, destination, member)
                .await
        } else {
            let _destination_guard = self.key_write_locks[destination_shard].lock().await;
            let _source_guard = self.key_write_locks[source_shard].lock().await;
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
        let destination_meta = self.set_meta_async(destination).await?;
        let source_member_key = set_member_key(self.db_index, source, source_meta.version, member);
        if !self.store.contains_key_async(&source_member_key).await {
            return Ok(false);
        }
        let mut batch = WriteBatch::new();
        batch.delete(&source_member_key);

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
            None => self.next_version_async().await,
        };
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self.store.contains_key_async(&destination_member_key).await {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            batch.put(&destination_member_key, INDEX_MARKER_VALUE);
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
