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

        if meta.is_none() {}
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

    pub fn set_intersection_card(&self, keys: &[String], limit: usize) -> Result<usize, Error> {
        let count = self.set_intersection(keys)?.len();
        Ok(if limit == 0 { count } else { count.min(limit) })
    }

    /// 检查 member 是否属于 set。
    pub fn set_contains(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        Ok(self
            .store
            .contains_key(&set_member_key(self.db_index, key, meta.version, member)))
    }

    pub async fn set_contains_async(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        Ok(self
            .store
            .contains_key_async(&set_member_key(self.db_index, key, meta.version, member))
            .await)
    }

    /// 返回 set 成员数量。
    pub fn set_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self.set_meta(key)?.map_or(0, |meta| meta.len))
    }

    /// 返回 set 所有成员。
    pub fn set_members(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw(key, meta.version)
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    pub async fn set_members_async(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw_async(key, meta.version)
            .await
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    async fn set_member_set_async(&self, key: &str) -> Result<Option<HashSet<String>>, Error> {
        match self.set_meta_async(key).await? {
            Some(meta) => Ok(Some(
                self.set_members_raw_async(key, meta.version)
                    .await
                    .into_iter()
                    .filter_map(|member| String::from_utf8(member).ok())
                    .collect(),
            )),
            None => Ok(None),
        }
    }

    /// 计算多个 set 的差集。不存在的 key 视为空 set。
    pub fn set_diff(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        };

        let mut difference = match self.get(first_key) {
            Some(Structure::Set(set)) => set,
            Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
            None => HashSet::new(),
        };

        for key in rest {
            match self.get(key) {
                Some(Structure::Set(set)) => {
                    for member in set {
                        difference.remove(&member);
                    }
                }
                Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
                None => {}
            }
        }

        Ok(difference)
    }

    pub async fn set_diff_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        };

        let mut difference = self
            .set_member_set_async(first_key)
            .await?
            .unwrap_or_default();
        for key in rest {
            if let Some(set) = self.set_member_set_async(key).await? {
                for member in set {
                    difference.remove(&member);
                }
            }
        }

        Ok(difference)
    }

    /// 计算 set 差集并写入目标 key，返回写入成员数量。
    pub fn set_diff_store(&self, destination: &str, keys: &[String]) -> Result<usize, Error> {
        let difference = self.set_diff(keys)?;
        let len = difference.len();
        if len == 0 {
            self.remove(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(difference));
        }
        Ok(len)
    }

    pub async fn set_diff_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let difference = self.set_diff_async(keys).await?;
        let len = difference.len();
        if len == 0 {
            self.remove(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(difference));
        }
        Ok(len)
    }

    pub fn set_intersection(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        };

        let mut intersection = match self.get(first_key) {
            Some(Structure::Set(set)) => set,
            Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
            None => return Ok(HashSet::new()),
        };

        for key in rest {
            match self.get(key) {
                Some(Structure::Set(set)) => {
                    intersection = intersection.intersection(&set).cloned().collect();
                }
                Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
                None => return Ok(HashSet::new()),
            }
        }

        Ok(intersection)
    }

    pub async fn set_intersection_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        };

        let Some(mut intersection) = self.set_member_set_async(first_key).await? else {
            return Ok(HashSet::new());
        };
        for key in rest {
            let Some(set) = self.set_member_set_async(key).await? else {
                return Ok(HashSet::new());
            };
            intersection = intersection.intersection(&set).cloned().collect();
        }

        Ok(intersection)
    }

    pub fn set_intersection_store(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let intersection = self.set_intersection(keys)?;
        let len = intersection.len();
        if len == 0 {
            self.delete_key(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(intersection));
        }
        Ok(len)
    }

    pub async fn set_intersection_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let intersection = self.set_intersection_async(keys).await?;
        let len = intersection.len();
        if len == 0 {
            self.delete_key(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(intersection));
        }
        Ok(len)
    }

    pub async fn set_union_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let mut result = HashSet::new();
        for key in keys {
            if let Some(set) = self.set_member_set_async(key).await? {
                result.extend(set);
            }
        }
        Ok(result)
    }

    pub async fn set_union_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let union = self.set_union_async(keys).await?;
        let len = union.len();
        if len == 0 {
            self.delete_key(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(union));
        }
        Ok(len)
    }

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
                    batch.delete(&self.mk(key));
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
                    batch.delete(&self.mk(key));
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
