use super::*;

impl Db {
    pub fn set_random_members(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<String>>, Error> {
        let mut members = self.set_members(key)?;
        if members.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = members.len();
        members.rotate_left(seed % len);

        let Some(count) = count else {
            return Ok(Some(vec![members[0].clone()]));
        };
        if count >= 0 {
            members.truncate((count as usize).min(members.len()));
            return Ok(Some(members));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for idx in 0..requested {
            result.push(members[idx % members.len()].clone());
        }
        Ok(Some(result))
    }

    pub async fn set_random_members_async(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<String>>, Error> {
        let mut members = self.set_members_async(key).await?;
        if members.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = members.len();
        members.rotate_left(seed % len);

        let Some(count) = count else {
            return Ok(Some(vec![members[0].clone()]));
        };
        if count >= 0 {
            members.truncate((count as usize).min(members.len()));
            return Ok(Some(members));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for idx in 0..requested {
            result.push(members[idx % members.len()].clone());
        }
        Ok(Some(result))
    }

    /// 弹出 count 个成员。
    pub fn set_pop(&self, key: &str, count: usize) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        let mut meta = self.ensure_set_slot_index(key, meta);

        let target_count = count.min(meta.len);
        if target_count == 1 {
            for _ in 0..2 {
                if meta.len == 0 {
                    return Ok(Vec::new());
                }
                let slot = random_u64() % meta.len as u64;
                let slot_key = set_slot_key(self.db_index, key, meta.version, slot);
                let Some(member) = self.store.get_raw(&slot_key) else {
                    meta = self.rebuild_set_slot_index(key, meta);
                    continue;
                };
                let mut batch = WriteBatch::new();
                if !self.set_slot_remove_to_batch(&mut batch, key, meta.version, meta.len, &member)
                {
                    meta = self.rebuild_set_slot_index(key, meta);
                    continue;
                }
                let member = String::from_utf8(member).map_err(|_| {
                    Error::msg("ERR invalid UTF-8 set member found while popping from set")
                })?;
                let len = meta.len.saturating_sub(1);
                if len == 0 {
                    self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                } else {
                    batch.put(
                        &self.mk(key),
                        &encode_set_meta(meta.expire_ms, meta.version, len),
                    );
                }
                self.write_batch_if_not_empty(&batch);
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(vec![member]);
            }
            return Ok(Vec::new());
        }

        let available = if target_count == meta.len {
            self.set_members_raw(key, meta.version)
        } else {
            self.set_random_seek_members(key, meta.version, target_count)
        };
        let mut batch = WriteBatch::new();
        let mut popped = Vec::new();
        for member in available.into_iter().take(count) {
            let member = String::from_utf8(member).map_err(|_| {
                Error::msg("ERR invalid UTF-8 set member found while popping from set")
            })?;
            batch.delete(&set_member_key(self.db_index, key, meta.version, &member));
            popped.push(member);
        }

        if !popped.is_empty() {
            let len = meta.len.saturating_sub(popped.len());
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
        if !popped.is_empty() && meta.len > popped.len() {
            self.rebuild_set_slot_index(
                key,
                SetMeta {
                    len: meta.len.saturating_sub(popped.len()),
                    ..meta
                },
            );
        }
        Ok(popped)
    }

    pub async fn set_pop_async(&self, key: &str, count: usize) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _set_write_guard = self.set_write_lock(key).lock().await;
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        let target_count = count.min(meta.len);
        if target_count == 1 {
            let mut meta = self.ensure_set_slot_index_async(key, meta).await;
            for _ in 0..2 {
                if meta.len == 0 {
                    return Ok(Vec::new());
                }
                let slot = random_u64() % meta.len as u64;
                let slot_key = set_slot_key(self.db_index, key, meta.version, slot);
                let Some(member) = self.store.get_raw_async(&slot_key).await else {
                    meta = self.rebuild_set_slot_index_async(key, meta).await;
                    continue;
                };
                let mut batch = WriteBatch::new();
                if !self
                    .set_slot_remove_to_batch_async(
                        &mut batch,
                        key,
                        meta.version,
                        meta.len,
                        &member,
                    )
                    .await
                {
                    meta = self.rebuild_set_slot_index_async(key, meta).await;
                    continue;
                }
                let member = String::from_utf8(member).map_err(|_| {
                    Error::msg("ERR invalid UTF-8 set member found while popping from set")
                })?;
                let len = meta.len.saturating_sub(1);
                if len == 0 {
                    self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                } else {
                    batch.put(
                        &self.mk(key),
                        &encode_set_meta(meta.expire_ms, meta.version, len),
                    );
                }
                self.write_batch_if_not_empty_async(&batch).await;
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(vec![member]);
            }
            return Ok(Vec::new());
        }

        let meta = self.ensure_set_slot_index_async(key, meta).await;
        let available = if target_count == meta.len {
            self.set_members_raw_async(key, meta.version).await
        } else {
            self.set_random_seek_members_async(key, meta.version, target_count)
                .await
        };
        let mut batch = WriteBatch::new();
        let mut popped = Vec::new();
        for member in available.into_iter().take(count) {
            let member = String::from_utf8(member).map_err(|_| {
                Error::msg("ERR invalid UTF-8 set member found while popping from set")
            })?;
            batch.delete(&set_member_key(self.db_index, key, meta.version, &member));
            popped.push(member);
        }

        if !popped.is_empty() {
            let len = meta.len.saturating_sub(popped.len());
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
        if !popped.is_empty() && meta.len > popped.len() {
            self.rebuild_set_slot_index_async(
                key,
                SetMeta {
                    len: meta.len.saturating_sub(popped.len()),
                    ..meta
                },
            )
            .await;
        }
        Ok(popped)
    }

    /// 扫描 set members，返回下一个游标和成员。
    pub fn set_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<String>), Error> {
        let mut members = self.set_members(key)?;
        if pattern_str != "*" {
            members.retain(|member| pattern::is_match(member, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, members.len());
        let items = if start_index < members.len() {
            members[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= members.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }

    pub async fn set_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<String>), Error> {
        let mut members = self.set_members_async(key).await?;
        if pattern_str != "*" {
            members.retain(|member| pattern::is_match(member, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, members.len());
        let items = if start_index < members.len() {
            members[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= members.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }
}
