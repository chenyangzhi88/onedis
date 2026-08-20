use super::*;

impl Db {
    /// Apply an ordered pipeline of positive relative expiration updates and PERSIST operations
    /// using one observed read and one physical write for all affected keys.
    pub(crate) async fn apply_key_expiration_batch_async(
        &self,
        mutations: &[KeyExpirationBatchMutation<'_>],
    ) -> Vec<Result<i64, Error>> {
        if mutations.is_empty() {
            return Vec::new();
        }
        let mut key_positions = HashMap::<&str, usize>::with_capacity(mutations.len());
        let mut keys = Vec::<&str>::with_capacity(mutations.len());
        for mutation in mutations {
            let key = mutation.key();
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _write_guards = self.lock_write_shards(&shards).await;

        for _ in 0..64 {
            for key in &keys {
                if let Err(error) = self.expire_if_needed_async(key).await {
                    return storage_batch_error(mutations.len(), error);
                }
            }
            let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let observations = match self.store.multi_get_raw_observed_async(&raw_keys).await {
                Ok(observations) => observations,
                Err(error) => return storage_batch_error(mutations.len(), error),
            };
            let mut states = observations
                .iter()
                .map(|observed| observed.value().map(|raw| raw.to_vec()))
                .collect::<Vec<_>>();
            let mut dirty = vec![false; keys.len()];
            let mut replies = Vec::with_capacity(mutations.len());

            for mutation in mutations {
                let position = key_positions[mutation.key()];
                let Some(raw) = states[position].as_mut() else {
                    replies.push(Ok(0));
                    continue;
                };
                let Some(header) = decode_meta_header(raw) else {
                    replies.push(Ok(0));
                    continue;
                };
                match mutation {
                    KeyExpirationBatchMutation::Expire { ttl_ms, .. } => {
                        let expire_ms = now_ms().saturating_add(*ttl_ms);
                        let Some(patched) = patch_meta_expire_ms(raw, expire_ms) else {
                            replies.push(Ok(0));
                            continue;
                        };
                        *raw = patched;
                        dirty[position] = true;
                        replies.push(Ok(1));
                    }
                    KeyExpirationBatchMutation::Persist { .. } => {
                        if header.expire_ms == 0 {
                            replies.push(Ok(0));
                            continue;
                        }
                        let Some(patched) = patch_meta_expire_ms(raw, 0) else {
                            replies.push(Ok(0));
                            continue;
                        };
                        *raw = patched;
                        dirty[position] = true;
                        replies.push(Ok(1));
                    }
                }
            }

            let dirty_positions = dirty
                .iter()
                .enumerate()
                .filter_map(|(position, dirty)| dirty.then_some(position))
                .collect::<Vec<_>>();
            if dirty_positions.is_empty() {
                return replies;
            }
            let mut batch = WriteBatch::new();
            for &position in &dirty_positions {
                let key = keys[position];
                let raw = states[position]
                    .as_deref()
                    .expect("a dirty expiration state must contain metadata");
                let initial_expire_ms = observations[position]
                    .value()
                    .map_or(0, |raw| decode_expire_ms(raw));
                let final_expire_ms = decode_expire_ms(raw);
                (batch.put(&raw_keys[position], raw))
                    .expect("write batch append invariant violated");
                let Some(header) = decode_meta_header(raw) else {
                    return fail_successful_batch_replies(
                        replies,
                        Error::msg("ERR corrupt key expiration metadata"),
                    );
                };
                if let Err(error) = self.append_fulltext_expiration_update_to_batch(
                    &mut batch,
                    key,
                    header.type_tag,
                ) {
                    return fail_successful_batch_replies(replies, error);
                }
                if initial_expire_ms > 0 && initial_expire_ms != final_expire_ms {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        initial_expire_ms,
                        self.db_index,
                        key,
                    );
                }
                if final_expire_ms > 0 {
                    self.ttl_manager
                        .add_to_batch(&mut batch, final_expire_ms, self.db_index, key);
                }
            }
            let conditions = dirty_positions
                .iter()
                .map(|&position| CompareCondition::from_observed(&observations[position]))
                .collect::<Vec<_>>();
            match self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await
            {
                Ok(true) => return replies,
                Ok(false) => continue,
                Err(error) => {
                    let message = error.to_string();
                    return mutations
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }
        mutations
            .iter()
            .map(|_| Err(Error::msg("ERR expiration batch write conflict")))
            .collect()
    }

    /**
     * 设置过期
     *
     * @param key 键名
     * @param ttl 距离现在多少【毫秒】后过期
     */
    pub fn expire(&self, key: String, ttl: u64) -> Result<bool, Error> {
        self.expire_with_condition(key, ttl, ExpireCondition::Always)
    }

    pub async fn expire_async(&self, key: String, ttl: u64) -> Result<bool, Error> {
        self.expire_with_condition_async(key, ttl, ExpireCondition::Always)
            .await
    }

    pub fn expire_with_condition(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        self.expire_if_needed(&key)?;
        let key_bytes = self.mk(&key);
        let expire_ms = now_ms().saturating_add(ttl);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(false);
            };
            let Some(header) = decode_meta_header(raw) else {
                return Ok(false);
            };
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                self.expire_if_needed(&key)?;
                continue;
            }
            if !Self::expire_condition_matches(header.expire_ms, expire_ms, condition) {
                return Ok(false);
            }
            let mut batch = WriteBatch::new();
            if ttl == 0 {
                self.append_expiration_delete_to_batch(&mut batch, &key, &key_bytes, header)?;
            } else {
                let Some(patched) = patch_meta_expire_ms(raw, expire_ms) else {
                    return Ok(false);
                };
                (batch.put(&key_bytes, &patched)).expect("write batch append invariant violated");
                self.append_fulltext_expiration_update_to_batch(&mut batch, &key, header.type_tag)?;
                if header.expire_ms > 0 && header.expire_ms != expire_ms {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        &key,
                    );
                }
                self.ttl_manager
                    .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
            }
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
                Ok(true) => {
                    if ttl == 0 {
                        self.refresh_after_expiration_delete(&key, header.type_tag);
                    }
                    return Ok(true);
                }
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR expiration update conflict"))
    }

    pub async fn expire_with_condition_async(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        let _write_guard = self.set_write_lock(&key).lock().await;
        self.expire_with_condition_async_unlocked(key, ttl, condition)
            .await
    }

    async fn expire_with_condition_async_unlocked(
        &self,
        key: String,
        ttl: u64,
        condition: ExpireCondition,
    ) -> Result<bool, Error> {
        let key_bytes = self.mk(&key);
        let expire_ms = now_ms().saturating_add(ttl);
        for _ in 0..64 {
            self.expire_if_needed_async(&key).await?;
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(false);
            };
            let Some(header) = decode_meta_header(raw) else {
                return Ok(false);
            };
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                self.expire_if_needed_async(&key).await?;
                continue;
            }
            if !Self::expire_condition_matches(header.expire_ms, expire_ms, condition) {
                return Ok(false);
            }
            if ttl == 0 {
                let mut batch = WriteBatch::new();
                self.append_expiration_delete_to_batch(&mut batch, &key, &key_bytes, header)?;
                match self
                    .compare_and_write_batch_if_not_empty_async(
                        &[CompareCondition::from_observed(&observed)],
                        &batch,
                    )
                    .await
                {
                    Ok(true) => {
                        self.refresh_after_expiration_delete(&key, header.type_tag);
                        return Ok(true);
                    }
                    Ok(false) => continue,
                    Err(error) => return Err(error),
                }
            }
            let Some(patched) = patch_meta_expire_ms(raw, expire_ms) else {
                return Ok(false);
            };
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &patched)).expect("write batch append invariant violated");
            self.append_fulltext_expiration_update_to_batch(&mut batch, &key, header.type_tag)?;
            if header.expire_ms > 0 && header.expire_ms != expire_ms {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    &key,
                );
            }
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, &key);
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR expiration update conflict"))
    }

    pub(in crate::store::db) fn expire_condition_matches(
        current_expire_ms: u64,
        expire_ms: u64,
        condition: ExpireCondition,
    ) -> bool {
        match condition {
            ExpireCondition::Always => true,
            ExpireCondition::Nx => current_expire_ms == 0,
            ExpireCondition::Xx => current_expire_ms > 0,
            ExpireCondition::Gt => current_expire_ms > 0 && expire_ms > current_expire_ms,
            ExpireCondition::Lt => current_expire_ms == 0 || expire_ms < current_expire_ms,
            ExpireCondition::XxGt => current_expire_ms > 0 && expire_ms > current_expire_ms,
            ExpireCondition::XxLt => current_expire_ms > 0 && expire_ms < current_expire_ms,
        }
    }

    /**
     * 移除过期时间（PERSIST 命令）
     */
    pub fn persist(&self, key: &str) -> Result<bool, Error> {
        self.expire_if_needed(key)?;
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(false);
            };
            let Some(header) = decode_meta_header(raw) else {
                return Err(Error::msg("ERR corrupt key expiration metadata"));
            };
            let expire_ms = header.expire_ms;
            if expire_ms == 0 {
                return Ok(false);
            }
            if now_ms() >= expire_ms {
                self.expire_if_needed(key)?;
                continue;
            }
            let Some(patched) = patch_meta_expire_ms(raw, 0) else {
                return Ok(false);
            };
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &patched)).expect("write batch append invariant violated");
            self.append_fulltext_expiration_update_to_batch(&mut batch, key, header.type_tag)?;
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            match self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            ) {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR persist conflict"))
    }

    pub async fn persist_async(&self, key: &str) -> Result<bool, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.persist_async_unlocked(key).await
    }

    async fn persist_async_unlocked(&self, key: &str) -> Result<bool, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await?;
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(false);
            };
            let Some(header) = decode_meta_header(raw) else {
                return Err(Error::msg("ERR corrupt key expiration metadata"));
            };
            let expire_ms = header.expire_ms;
            if expire_ms == 0 {
                return Ok(false);
            }
            if now_ms() >= expire_ms {
                self.expire_if_needed_async(key).await?;
                continue;
            }
            let Some(patched) = patch_meta_expire_ms(raw, 0) else {
                return Ok(false);
            };
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &patched)).expect("write batch append invariant violated");
            self.append_fulltext_expiration_update_to_batch(&mut batch, key, header.type_tag)?;
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR persist conflict"))
    }

    /**
     * 删除键值
     *
     * @param key 键名
     * @return 如果删除成功，返回被删除的值
     */
    pub fn remove(&self, key: &str) -> Result<Option<Structure>, Error> {
        self.remove_internal(key, true)
    }

    pub async fn remove_async(&self, key: &str) -> Result<Option<Structure>, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.remove_internal_async(key, true).await
    }

    pub fn delete_key(&self, key: &str) -> Result<bool, Error> {
        self.delete_key_internal(key, true)
    }

    pub async fn delete_key_async(&self, key: &str) -> Result<bool, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.delete_key_internal_async(key, true).await
    }

    pub fn touch(&self, key: &str) -> Result<bool, Error> {
        Ok(self.read_live_raw(key)?.is_some())
    }

    pub async fn touch_async(&self, key: &str) -> Result<bool, Error> {
        Ok(self.read_live_raw_async(key).await?.is_some())
    }
}
