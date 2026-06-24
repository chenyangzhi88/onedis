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
                batch.delete(&self.mk(key));
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
                batch.delete(&self.mk(key));
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
        if !self.set_contains(source, member)? {
            return Ok(false);
        }
        self.set_remove(source, &[member.to_string()])?;
        self.set_add(destination, &[member.to_string()])?;
        Ok(true)
    }

    pub async fn set_move_async(
        &self,
        source: &str,
        destination: &str,
        member: &str,
    ) -> Result<bool, Error> {
        if !self.set_contains_async(source, member).await? {
            return Ok(false);
        }
        self.set_remove_async(source, &[member.to_string()]).await?;
        self.set_add_async(destination, &[member.to_string()])
            .await?;
        Ok(true)
    }
}
