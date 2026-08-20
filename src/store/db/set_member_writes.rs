use super::*;

impl Db {
    fn try_set_add_packed(&self, key: &str, members: &[String]) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            self.expire_if_needed(key)?;
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let (expire_ms, mut packed) = match observed.value() {
                None => (0, PackedSetMembers::new()),
                Some(raw) => {
                    let header = decode_meta_header(raw)
                        .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
                    if header.type_tag != TYPE_SET {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let Some(packed) = decode_packed_set(raw) else {
                        return Ok(None);
                    };
                    (header.expire_ms, packed)
                }
            };
            let mut added = 0usize;
            for member in members {
                added += usize::from(packed.insert(member.clone()));
            }
            if added == 0 {
                return Ok(Some(0));
            }

            let mut batch = WriteBatch::new();
            if let Some(raw) = encode_packed_set(expire_ms, &packed) {
                (batch.put(&key_bytes, &raw)).expect("write batch append invariant violated");
            } else {
                let version = self.next_version();
                (batch.put(
                    &key_bytes,
                    &encode_set_meta(expire_ms, version, packed.len()),
                ))
                .expect("write batch append invariant violated");
                for member in &packed {
                    (batch.put(
                        &set_member_key(self.db_index, key, version, member),
                        INDEX_MARKER_VALUE,
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(added));
            }
        }
        self.promote_packed_set(key)?;
        Ok(None)
    }

    async fn try_set_add_packed_async(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            self.expire_if_needed_async(key).await?;
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let (expire_ms, mut packed) = match observed.value() {
                None => (0, PackedSetMembers::new()),
                Some(raw) => {
                    let header = decode_meta_header(raw)
                        .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
                    if header.type_tag != TYPE_SET {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    let Some(packed) = decode_packed_set(raw) else {
                        return Ok(None);
                    };
                    (header.expire_ms, packed)
                }
            };
            let mut added = 0usize;
            for member in members {
                added += usize::from(packed.insert(member.clone()));
            }
            if added == 0 {
                return Ok(Some(0));
            }

            let mut batch = WriteBatch::new();
            if let Some(raw) = encode_packed_set(expire_ms, &packed) {
                (batch.put(&key_bytes, &raw)).expect("write batch append invariant violated");
            } else {
                let version = self.next_version_async().await;
                (batch.put(
                    &key_bytes,
                    &encode_set_meta(expire_ms, version, packed.len()),
                ))
                .expect("write batch append invariant violated");
                for member in &packed {
                    (batch.put(
                        &set_member_key(self.db_index, key, version, member),
                        INDEX_MARKER_VALUE,
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(added));
            }
        }
        self.promote_packed_set_async(key).await?;
        Ok(None)
    }

    fn try_set_remove_packed(&self, key: &str, members: &[String]) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            self.expire_if_needed(key)?;
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(Some(0));
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
            if header.type_tag != TYPE_SET {
                return Err(Error::msg(WRONG_TYPE_ERROR));
            }
            let Some(mut packed) = decode_packed_set(raw) else {
                return Ok(None);
            };
            let mut removed = 0usize;
            for member in members {
                removed += usize::from(packed.remove(member));
            }
            if removed == 0 {
                return Ok(Some(0));
            }
            let mut batch = WriteBatch::new();
            if packed.is_empty() {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
            } else {
                let encoded = encode_packed_set(header.expire_ms, &packed)
                    .expect("removing members cannot overflow packed set");
                (batch.put(&key_bytes, &encoded)).expect("write batch append invariant violated");
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(removed));
            }
        }
        self.promote_packed_set(key)?;
        Ok(None)
    }

    async fn try_set_remove_packed_async(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            self.expire_if_needed_async(key).await?;
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(Some(0));
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
            if header.type_tag != TYPE_SET {
                return Err(Error::msg(WRONG_TYPE_ERROR));
            }
            let Some(mut packed) = decode_packed_set(raw) else {
                return Ok(None);
            };
            let mut removed = 0usize;
            for member in members {
                removed += usize::from(packed.remove(member));
            }
            if removed == 0 {
                return Ok(Some(0));
            }
            let mut batch = WriteBatch::new();
            if packed.is_empty() {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
            } else {
                let encoded = encode_packed_set(header.expire_ms, &packed)
                    .expect("removing members cannot overflow packed set");
                (batch.put(&key_bytes, &encoded)).expect("write batch append invariant violated");
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(removed));
            }
        }
        self.promote_packed_set_async(key).await?;
        Ok(None)
    }

    /// Apply ordered SADD/SREM commands with one metadata read and one atomic write per batch.
    pub(crate) async fn apply_set_batch_mutations_async(
        &self,
        mutations: &[SetBatchMutation<'_>],
    ) -> Vec<Result<usize, Error>> {
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

        for attempt in 0..64 {
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
            let mut states = Vec::with_capacity(keys.len());
            for (position, key) in keys.iter().enumerate() {
                states.push(
                    SetBatchState::from_raw(
                        observations[position].value().map(AsRef::as_ref),
                        || self.next_version_async(),
                    )
                    .await
                    .map(|state| (*key, state)),
                );
            }

            let mut candidate_members = vec![HashSet::<&str>::new(); keys.len()];
            for mutation in mutations {
                let position = key_positions[mutation.key()];
                if states[position].is_ok() {
                    candidate_members[position].extend(mutation.members().iter().copied());
                }
            }
            let mut member_lookups = Vec::new();
            for (position, candidates) in candidate_members.iter().enumerate() {
                let Ok((key, state)) = &states[position] else {
                    continue;
                };
                if !state.initially_exists || state.packed {
                    continue;
                }
                member_lookups.extend(candidates.iter().map(|member| {
                    (
                        position,
                        (*member).to_string(),
                        set_member_key(self.db_index, key, state.version, member),
                    )
                }));
            }
            let member_keys = member_lookups
                .iter()
                .map(|(_, _, key)| key.clone())
                .collect::<Vec<_>>();
            let member_values = match self.store.multi_get_raw_async(&member_keys).await {
                Ok(values) => values,
                Err(error) => return storage_batch_error(mutations.len(), error),
            };
            for ((position, member, _), value) in member_lookups.into_iter().zip(member_values) {
                if value.is_some()
                    && let Ok((_, state)) = &mut states[position]
                {
                    state.initial_members.insert(member.clone());
                    state.members.insert(member);
                }
            }

            let mut replies = Vec::with_capacity(mutations.len());
            let mut changed_commands = 0u64;
            for mutation in mutations {
                let position = key_positions[mutation.key()];
                let result = match &mut states[position] {
                    Err(error) => Err(Error::msg(error.to_string())),
                    Ok((_, state)) => {
                        let mut seen = HashSet::with_capacity(mutation.members().len());
                        let mut changed = 0usize;
                        for member in mutation.members() {
                            if !seen.insert(*member) {
                                continue;
                            }
                            let did_change = match mutation {
                                SetBatchMutation::Add { .. } => {
                                    state.members.insert((*member).to_string())
                                }
                                SetBatchMutation::Remove { .. } => state.members.remove(*member),
                            };
                            changed += usize::from(did_change);
                        }
                        state.touched |= changed > 0;
                        if changed > 0 {
                            changed_commands += 1;
                        }
                        Ok(changed)
                    }
                };
                replies.push(result);
            }

            let dirty_positions = states
                .iter()
                .enumerate()
                .filter_map(|(position, state)| {
                    state
                        .as_ref()
                        .ok()
                        .is_some_and(|(_, state)| state.touched)
                        .then_some(position)
                })
                .collect::<Vec<_>>();
            if dirty_positions.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            for &position in &dirty_positions {
                let (key, state) = states[position].as_ref().expect("dirty set state is valid");
                if state.packed {
                    if state.members.is_empty() {
                        self.delete_main_key_with_ttl_to_batch(&mut batch, key, state.expire_ms);
                    } else {
                        let packed = state.members.iter().cloned().collect::<PackedSetMembers>();
                        if let Some(raw) = encode_packed_set(state.expire_ms, &packed) {
                            (batch.put(&self.mk(key), &raw))
                                .expect("write batch append invariant violated");
                        } else {
                            (batch.put(
                                &self.mk(key),
                                &encode_set_meta(
                                    state.expire_ms,
                                    state.version,
                                    state.members.len(),
                                ),
                            ))
                            .expect("write batch append invariant violated");
                            for member in &state.members {
                                (batch.put(
                                    &set_member_key(self.db_index, key, state.version, member),
                                    INDEX_MARKER_VALUE,
                                ))
                                .expect("write batch append invariant violated");
                            }
                        }
                    }
                    continue;
                }
                for member in state.members.difference(&state.initial_members) {
                    (batch.put(
                        &set_member_key(self.db_index, key, state.version, member),
                        INDEX_MARKER_VALUE,
                    ))
                    .expect("write batch append invariant violated");
                }
                for member in state.initial_members.difference(&state.members) {
                    (batch.delete(&set_member_key(self.db_index, key, state.version, member)))
                        .expect("write batch append invariant violated");
                }
                let added = state.members.difference(&state.initial_members).count();
                let removed = state.initial_members.difference(&state.members).count();
                let final_len = state
                    .initial_len
                    .saturating_add(added)
                    .saturating_sub(removed);
                if final_len == 0 {
                    self.delete_main_key_with_ttl_to_batch(&mut batch, key, state.expire_ms);
                    if state.initially_exists {
                        delete_sub_keys_to_batch(
                            &mut batch,
                            self.db_index,
                            key,
                            state.version,
                            TYPE_SET,
                        );
                    }
                } else {
                    (batch.put(
                        &self.mk(key),
                        &encode_set_meta(state.expire_ms, state.version, final_len),
                    ))
                    .expect("write batch append invariant violated");
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
                Ok(true) => {
                    self.changes.fetch_add(changed_commands, Ordering::Relaxed);
                    return replies;
                }
                Ok(false) => {
                    if attempt + 1 == SMALL_INLINE_CAS_ATTEMPTS {
                        for &position in &dirty_positions {
                            if let Ok((key, state)) = &states[position]
                                && state.packed
                                && let Err(error) = self.promote_packed_set_async(key).await
                            {
                                let message = error.to_string();
                                return mutations
                                    .iter()
                                    .map(|_| Err(Error::msg(message.clone())))
                                    .collect();
                            }
                        }
                    }
                    continue;
                }
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
            .map(|_| Err(Error::msg("ERR set batch write conflict")))
            .collect()
    }

    pub fn set_add(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        if let Some(added) = self.try_set_add_packed(key, members)? {
            return Ok(added);
        }
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
            if !self.store.contains_key(&member_key)? {
                (batch.put(&member_key, INDEX_MARKER_VALUE))
                    .expect("write batch append invariant violated");
                added += 1;
            }
        }

        if added > 0 || meta.is_none() {
            let expire_ms = meta.map_or(0, |meta| meta.expire_ms);
            let len = meta.map_or(0, |meta| meta.len).saturating_add(added);
            (batch.put(&self.mk(key), &encode_set_meta(expire_ms, version, len)))
                .expect("write batch append invariant violated");
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch)?;
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
        if let Some(added) = self.try_set_add_packed_async(key, members).await? {
            return Ok(added);
        }
        let key_bytes = self.mk(key);
        let raw_meta = self.store.get_raw_async(&key_bytes).await?;
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
            let existing = self.store.multi_get_raw_async(&member_keys).await?;
            for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
                if old_raw.is_some() {
                    continue;
                }
                (batch.put(&member_key, INDEX_MARKER_VALUE))
                    .expect("write batch append invariant violated");
                added += 1;
            }
        } else {
            for member in unique_members {
                let member_key = set_member_key(self.db_index, key, version, member);
                (batch.put(&member_key, INDEX_MARKER_VALUE))
                    .expect("write batch append invariant violated");
                added += 1;
            }
        }

        if added > 0 || meta.is_none() {
            let expire_ms = meta.map_or(0, |meta| meta.expire_ms);
            let len = meta.map_or(0, |meta| meta.len).saturating_add(added);
            (batch.put(&key_bytes, &encode_set_meta(expire_ms, version, len)))
                .expect("write batch append invariant violated");
        }

        if batch.count() > 0 {
            if existing_version {
                self.write_existing_version_batch_if_not_empty_async(&batch)
                    .await?;
            } else {
                self.write_batch_if_not_empty_async(&batch).await?;
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
        if let Some(removed) = self.try_set_remove_packed(key, members)? {
            return Ok(removed);
        }
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
        let existing = self.store.multi_get_raw(&member_keys)?;
        for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
            if old_raw.is_some() {
                (batch.delete(&member_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }

        if deleted > 0 {
            let len = meta.len.saturating_sub(deleted);
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                (batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                ))
                .expect("write batch append invariant violated");
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch)?;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub async fn set_remove_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let _set_write_guard = self.set_write_lock(key).lock().await;
        if let Some(removed) = self.try_set_remove_packed_async(key, members).await? {
            return Ok(removed);
        }
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
        let existing = self.store.multi_get_raw_async(&member_keys).await?;
        for (member_key, old_raw) in member_keys.into_iter().zip(existing) {
            if old_raw.is_some() {
                (batch.delete(&member_key)).expect("write batch append invariant violated");
                deleted += 1;
            }
        }

        if deleted > 0 {
            let len = meta.len.saturating_sub(deleted);
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                (batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                ))
                .expect("write batch append invariant violated");
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await?;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub fn set_move(&self, source: &str, destination: &str, member: &str) -> Result<bool, Error> {
        if source == destination {
            return self.set_contains(source, member);
        }
        self.promote_packed_set(source)?;
        self.promote_packed_set(destination)?;
        let Some(source_meta) = self.set_meta(source)? else {
            return Ok(false);
        };
        let destination_meta = self.set_meta(destination)?;
        let source_member_key = set_member_key(self.db_index, source, source_meta.version, member);
        if !self.store.contains_key(&source_member_key)? {
            return Ok(false);
        }
        let mut batch = WriteBatch::new();
        (batch.delete(&source_member_key)).expect("write batch append invariant violated");

        let source_len = source_meta.len.saturating_sub(1);
        if source_len == 0 {
            (batch.delete(&self.mk(source))).expect("write batch append invariant violated");
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
            (batch.put(
                &self.mk(source),
                &encode_set_meta(source_meta.expire_ms, source_meta.version, source_len),
            ))
            .expect("write batch append invariant violated");
        }

        let destination_version = destination_meta
            .map(|meta| meta.version)
            .unwrap_or_else(|| self.next_version());
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self.store.contains_key(&destination_member_key)? {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            (batch.put(&destination_member_key, INDEX_MARKER_VALUE))
                .expect("write batch append invariant violated");
            (batch.put(
                &self.mk(destination),
                &encode_set_meta(
                    destination_meta.map_or(0, |meta| meta.expire_ms),
                    destination_version,
                    destination_len.saturating_add(1),
                ),
            ))
            .expect("write batch append invariant violated");
        }

        self.write_batch_if_not_empty(&batch)?;
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
        self.promote_packed_set_async(source).await?;
        self.promote_packed_set_async(destination).await?;
        let Some(source_meta) = self.set_meta_async(source).await? else {
            return Ok(false);
        };
        let destination_meta = self.set_meta_async(destination).await?;
        let source_member_key = set_member_key(self.db_index, source, source_meta.version, member);
        if !self.store.contains_key_async(&source_member_key).await? {
            return Ok(false);
        }
        let mut batch = WriteBatch::new();
        (batch.delete(&source_member_key)).expect("write batch append invariant violated");

        let source_len = source_meta.len.saturating_sub(1);
        if source_len == 0 {
            (batch.delete(&self.mk(source))).expect("write batch append invariant violated");
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
            (batch.put(
                &self.mk(source),
                &encode_set_meta(source_meta.expire_ms, source_meta.version, source_len),
            ))
            .expect("write batch append invariant violated");
        }

        let destination_version = match destination_meta {
            Some(meta) => meta.version,
            None => self.next_version_async().await,
        };
        let destination_member_key =
            set_member_key(self.db_index, destination, destination_version, member);
        if !self
            .store
            .contains_key_async(&destination_member_key)
            .await?
        {
            let destination_len = destination_meta.map_or(0, |meta| meta.len);
            (batch.put(&destination_member_key, INDEX_MARKER_VALUE))
                .expect("write batch append invariant violated");
            (batch.put(
                &self.mk(destination),
                &encode_set_meta(
                    destination_meta.map_or(0, |meta| meta.expire_ms),
                    destination_version,
                    destination_len.saturating_add(1),
                ),
            ))
            .expect("write batch append invariant violated");
        }

        self.write_batch_if_not_empty_async(&batch).await?;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

struct SetBatchState {
    version: u64,
    expire_ms: u64,
    initial_len: usize,
    initially_exists: bool,
    packed: bool,
    initial_members: HashSet<String>,
    members: HashSet<String>,
    touched: bool,
}

impl SetBatchState {
    async fn from_raw<F, Fut>(raw: Option<&[u8]>, next_version: F) -> Result<Self, Error>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = u64>,
    {
        let (version, expire_ms, initial_len, initially_exists, packed, initial_members) = match raw
        {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
                if header.type_tag != TYPE_SET {
                    return Err(Error::msg(WRONG_TYPE_ERROR));
                }
                let meta = decode_set_meta(raw)
                    .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
                if meta.packed {
                    let members = decode_packed_set(raw)
                        .ok_or_else(|| Error::msg("Failed to decode packed set"))?
                        .into_iter()
                        .collect::<HashSet<_>>();
                    (
                        next_version().await,
                        meta.expire_ms,
                        meta.len,
                        true,
                        true,
                        members,
                    )
                } else {
                    (
                        meta.version,
                        meta.expire_ms,
                        meta.len,
                        true,
                        false,
                        HashSet::new(),
                    )
                }
            }
            None => (next_version().await, 0, 0, false, true, HashSet::new()),
        };
        let members = initial_members.clone();
        Ok(Self {
            version,
            expire_ms,
            initial_len,
            initially_exists,
            packed,
            initial_members,
            members,
            touched: false,
        })
    }
}
