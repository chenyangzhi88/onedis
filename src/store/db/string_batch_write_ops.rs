use super::*;

impl Db {
    /// Apply an already ordered pipeline of independent single-key string mutations with one
    /// storage write. Replies are still produced command-by-command, and mutations of the same
    /// key observe all earlier commands in the pipeline.
    pub(crate) async fn apply_string_batch_mutations_async(
        &self,
        mutations: &[StringBatchMutation<'_>],
    ) -> Vec<Result<StringBatchReply, Error>> {
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
                self.expire_if_needed_async(key).await;
            }
            let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
            let mut states = observations
                .iter()
                .map(|observed| StringBatchState::from_raw(observed.value().map(AsRef::as_ref)))
                .collect::<Vec<_>>();
            let mut replies = Vec::with_capacity(mutations.len());
            let mut changed_commands = 0u64;

            for mutation in mutations {
                let position = key_positions[mutation.key()];
                let state = &mut states[position];
                let result = apply_string_batch_mutation(state, mutation);
                if result.as_ref().is_ok_and(|(_, changed)| *changed) {
                    changed_commands += 1;
                }
                replies.push(result.map(|(reply, _)| reply));
            }

            let dirty_positions = states
                .iter()
                .enumerate()
                .filter_map(|(position, state)| state.dirty.then_some(position))
                .collect::<Vec<_>>();
            if dirty_positions.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            for &position in &dirty_positions {
                let key = keys[position];
                let state = &states[position];
                self.prepare_string_overwrite_to_batch(
                    &mut batch,
                    key,
                    observations[position].value().map(AsRef::as_ref),
                );
                if let Some(value) = state.string_value() {
                    self.write_string_to_batch_with_deferred_old_raw(
                        &mut batch,
                        key,
                        value,
                        state.expire_ms,
                        observations[position].value().map(AsRef::as_ref),
                    );
                } else {
                    let old_expire_ms = observations[position]
                        .value()
                        .and_then(|raw| decode_meta_header(raw))
                        .map(|header| header.expire_ms)
                        .unwrap_or(0);
                    self.delete_main_key_with_ttl_to_batch(&mut batch, key, old_expire_ms);
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
            .map(|_| Err(Error::msg("ERR string batch write conflict")))
            .collect()
    }

    pub async fn insert_string_bytes_refs_async(&self, key_vals: &[(&str, &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            key_vals.iter().map(|(key, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.prepare_string_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub async fn insert_string_bytes_refs_without_watch_publish_async(
        &self,
        key_vals: &[(&str, &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            key_vals.iter().map(|(key, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.prepare_string_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_without_watch_publish_async(&batch)
            .await;
    }

    pub async fn insert_string_byte_keys_async(&self, key_vals: &[(&[u8], &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, key_vals.iter().map(|(key, _)| *key));
        let _write_guards = self.lock_write_shards(&shards).await;
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| main_key_bytes(self.db_index, key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.prepare_string_byte_key_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
            self.write_string_byte_key_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_owned_if_not_empty_async(batch)
            .await;
    }

    pub async fn insert_string_byte_keys_without_watch_publish_async(
        &self,
        key_vals: &[(&[u8], &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, key_vals.iter().map(|(key, _)| *key));
        let _write_guards = self.lock_write_shards(&shards).await;
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| main_key_bytes(self.db_index, key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.prepare_string_byte_key_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
            self.write_string_byte_key_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub fn insert_strings(&self, key_vals: Vec<(String, String)>) {
        self.insert_string_bytes_many(
            key_vals
                .into_iter()
                .map(|(key, value)| (key, value.into_bytes()))
                .collect(),
        );
    }

    pub fn insert_string_bytes_many(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_old_raw(&mut batch, &key, &value, 0, None);
        }
        self.write_plain_string_batch_if_not_empty(&batch);
    }

    pub async fn insert_string_bytes_many_async(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            key_vals.iter().map(|(key, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.into_iter().zip(old_values) {
            self.prepare_string_overwrite_to_batch(&mut batch, &key, old_raw.as_deref());
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                &key,
                &value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub fn insert_string_bytes_many_nx(&self, key_vals: Vec<(String, Vec<u8>)>) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        for (key, _) in &key_vals {
            self.expire_if_needed(key);
            if self.store.contains_key(&self.mk(key)) {
                return false;
            }
        }

        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch(&mut batch, &key, &value, 0);
        }
        self.write_batch_if_not_empty(&batch);
        true
    }

    pub async fn insert_string_bytes_many_nx_async(
        &self,
        key_vals: Vec<(String, Vec<u8>)>,
    ) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            key_vals.iter().map(|(key, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        for (key, _) in &key_vals {
            self.expire_if_needed_async(key).await;
        }
        let mut observations = Vec::with_capacity(key_vals.len());
        for (key, _) in &key_vals {
            let observed = self.store.get_raw_observed_async(&self.mk(key)).await;
            if observed.exists() {
                return false;
            }
            observations.push(observed);
        }

        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_deferred_old_raw(&mut batch, &key, &value, 0, None);
        }
        let conditions = observations
            .iter()
            .map(CompareCondition::from_observed)
            .collect::<Vec<_>>();
        match self
            .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
            .await
        {
            Ok(true) => {
                self.changes
                    .fetch_add(observations.len() as u64, Ordering::Relaxed);
                true
            }
            Ok(false) => false,
            Err(error) => {
                log::error!("failed to apply MSETNX batch: {error}");
                false
            }
        }
    }

    pub fn set_string_bytes_many(
        &self,
        key_vals: Vec<(String, Vec<u8>)>,
        expiration: SetExpiration,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let key_vals = deduplicate_string_items(key_vals);
        if key_vals.is_empty() {
            return Ok(false);
        }
        for (key, _) in &key_vals {
            self.expire_if_needed(key);
        }
        let old_values = key_vals
            .iter()
            .map(|(key, _)| self.store.get_raw(&self.mk(key)))
            .collect::<Vec<_>>();
        if !batch_condition_matches(condition, &old_values) {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(&old_values) {
            let old_header = old_raw.as_deref().and_then(decode_meta_header);
            let expire_ms = expiration_ms(expiration, old_header.map(|header| header.expire_ms));
            self.prepare_string_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
            if expire_ms > 0 && now_ms() >= expire_ms {
                (batch.delete(&self.mk(key))).expect("write batch append invariant violated");
                if let Some(header) = old_header
                    && header.expire_ms > 0
                {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        key,
                    );
                }
            } else {
                self.write_string_to_batch_with_deferred_old_raw(
                    &mut batch,
                    key,
                    value,
                    expire_ms,
                    old_raw.as_deref(),
                );
            }
        }
        self.write_batch_if_not_empty(&batch);
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        Ok(true)
    }

    pub async fn set_string_bytes_many_async(
        &self,
        key_vals: Vec<(String, Vec<u8>)>,
        expiration: SetExpiration,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let key_vals = deduplicate_string_items(key_vals);
        if key_vals.is_empty() {
            return Ok(false);
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            key_vals.iter().map(|(key, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        for _ in 0..64 {
            for (key, _) in &key_vals {
                self.expire_if_needed_async(key).await;
            }
            let mut observations = Vec::with_capacity(key_vals.len());
            for (key, _) in &key_vals {
                observations.push(self.store.get_raw_observed_async(&self.mk(key)).await);
            }
            let old_values = observations
                .iter()
                .map(|observed| observed.value().map(|value| value.to_vec()))
                .collect::<Vec<_>>();
            if !batch_condition_matches(condition, &old_values) {
                return Ok(false);
            }

            let mut batch = WriteBatch::new();
            for ((key, value), old_raw) in key_vals.iter().zip(&old_values) {
                let old_header = old_raw.as_deref().and_then(decode_meta_header);
                let expire_ms =
                    expiration_ms(expiration, old_header.map(|header| header.expire_ms));
                self.prepare_string_overwrite_to_batch(&mut batch, key, old_raw.as_deref());
                if expire_ms > 0 && now_ms() >= expire_ms {
                    (batch.delete(&self.mk(key))).expect("write batch append invariant violated");
                    if let Some(header) = old_header
                        && header.expire_ms > 0
                    {
                        self.ttl_manager.remove_known_to_batch(
                            &mut batch,
                            header.expire_ms,
                            self.db_index,
                            key,
                        );
                    }
                } else {
                    self.write_string_to_batch_with_deferred_old_raw(
                        &mut batch,
                        key,
                        value,
                        expire_ms,
                        old_raw.as_deref(),
                    );
                }
            }
            let conditions = observations
                .iter()
                .map(CompareCondition::from_observed)
                .collect::<Vec<_>>();
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes
                    .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
                return Ok(true);
            }
        }
        Err(Error::msg("ERR string batch write conflict"))
    }
}

enum StringBatchValue {
    Missing,
    String(Vec<u8>),
    Other,
}

struct StringBatchState {
    value: StringBatchValue,
    expire_ms: u64,
    dirty: bool,
}

impl StringBatchState {
    fn from_raw(raw: Option<&[u8]>) -> Self {
        let (value, expire_ms) = match raw {
            None => (StringBatchValue::Missing, 0),
            Some(raw) => match decode_meta_header(raw) {
                Some(header) if header.type_tag == TYPE_STRING => (
                    StringBatchValue::String(decode_string_bytes(raw).unwrap_or_default()),
                    header.expire_ms,
                ),
                Some(header) => (StringBatchValue::Other, header.expire_ms),
                None => (StringBatchValue::Other, 0),
            },
        };
        Self {
            value,
            expire_ms,
            dirty: false,
        }
    }

    fn string_value(&self) -> Option<&[u8]> {
        match &self.value {
            StringBatchValue::String(value) => Some(value),
            StringBatchValue::Missing | StringBatchValue::Other => None,
        }
    }
}

fn apply_string_batch_mutation(
    state: &mut StringBatchState,
    mutation: &StringBatchMutation<'_>,
) -> Result<(StringBatchReply, bool), Error> {
    use crate::frame::MAX_BULK_STRING_BYTES;

    match mutation {
        StringBatchMutation::Append { value, .. } => {
            let current_len = match &state.value {
                StringBatchValue::Missing => 0,
                StringBatchValue::String(bytes) => bytes.len(),
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            let required_len = current_len
                .checked_add(value.len())
                .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
            let bytes = match &mut state.value {
                StringBatchValue::Missing => {
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve(value.len())
                        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                    state.value = StringBatchValue::String(bytes);
                    let StringBatchValue::String(bytes) = &mut state.value else {
                        unreachable!();
                    };
                    bytes
                }
                StringBatchValue::String(bytes) => bytes,
                StringBatchValue::Other => unreachable!(),
            };
            bytes
                .try_reserve(value.len())
                .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
            bytes.extend_from_slice(value);
            state.dirty = true;
            Ok((StringBatchReply::Integer(required_len as i64), true))
        }
        StringBatchMutation::GetSet { value, .. } => {
            let old = match &state.value {
                StringBatchValue::Missing => None,
                StringBatchValue::String(bytes) => Some(bytes.clone()),
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            state.value = StringBatchValue::String(value.to_vec());
            state.expire_ms = 0;
            state.dirty = true;
            Ok((StringBatchReply::Bulk(old), true))
        }
        StringBatchMutation::GetDel { .. } => {
            let old = match &state.value {
                StringBatchValue::Missing => return Ok((StringBatchReply::Bulk(None), false)),
                StringBatchValue::String(bytes) => bytes.clone(),
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            state.value = StringBatchValue::Missing;
            state.expire_ms = 0;
            state.dirty = true;
            Ok((StringBatchReply::Bulk(Some(old)), true))
        }
        StringBatchMutation::SetNx { value, .. } => {
            if !matches!(state.value, StringBatchValue::Missing) {
                return Ok((StringBatchReply::Integer(0), false));
            }
            state.value = StringBatchValue::String(value.to_vec());
            state.expire_ms = 0;
            state.dirty = true;
            Ok((StringBatchReply::Integer(1), true))
        }
        StringBatchMutation::SetBit { offset, bit, .. } => {
            let byte_index = offset / 8;
            let bytes = match &mut state.value {
                StringBatchValue::Missing => {
                    let mut bytes = Vec::new();
                    string_bitmap_ops::resize_bitmap(&mut bytes, byte_index.saturating_add(1))?;
                    state.value = StringBatchValue::String(bytes);
                    let StringBatchValue::String(bytes) = &mut state.value else {
                        unreachable!();
                    };
                    bytes
                }
                StringBatchValue::String(bytes) => bytes,
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            if bytes.len() <= byte_index {
                string_bitmap_ops::resize_bitmap(bytes, byte_index.saturating_add(1))?;
            }
            let mask = 1u8 << (7 - (offset % 8));
            let old = u8::from(bytes[byte_index] & mask != 0);
            if *bit == 1 {
                bytes[byte_index] |= mask;
            } else {
                bytes[byte_index] &= !mask;
            }
            state.dirty = true;
            Ok((StringBatchReply::Integer(i64::from(old)), true))
        }
        StringBatchMutation::SetRange { offset, value, .. } => {
            let current_len = match &state.value {
                StringBatchValue::Missing => 0,
                StringBatchValue::String(bytes) => bytes.len(),
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            if value.is_empty() {
                return Ok((StringBatchReply::Integer(current_len as i64), false));
            }
            let required_len = offset
                .checked_add(value.len())
                .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
            let bytes = match &mut state.value {
                StringBatchValue::Missing => {
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(required_len)
                        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                    state.value = StringBatchValue::String(bytes);
                    let StringBatchValue::String(bytes) = &mut state.value else {
                        unreachable!();
                    };
                    bytes
                }
                StringBatchValue::String(bytes) => bytes,
                StringBatchValue::Other => return Err(Error::msg(WRONG_TYPE_ERROR)),
            };
            if required_len > bytes.len() {
                bytes
                    .try_reserve_exact(required_len - bytes.len())
                    .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                bytes.resize(required_len, 0);
            }
            bytes[*offset..required_len].copy_from_slice(value);
            state.dirty = true;
            Ok((StringBatchReply::Integer(bytes.len() as i64), true))
        }
        StringBatchMutation::Psetex { ttl_ms, value, .. } => {
            state.value = StringBatchValue::String(value.to_vec());
            state.expire_ms = now_ms().saturating_add(*ttl_ms);
            state.dirty = true;
            Ok((StringBatchReply::Ok, true))
        }
    }
}

fn deduplicate_string_items(key_vals: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    let mut positions: HashMap<String, usize> = HashMap::with_capacity(key_vals.len());
    let mut unique: Vec<(String, Vec<u8>)> = Vec::with_capacity(key_vals.len());
    for (key, value) in key_vals {
        if let Some(&position) = positions.get(&key) {
            unique[position].1 = value;
        } else {
            positions.insert(key.clone(), unique.len());
            unique.push((key, value));
        }
    }
    unique
}

fn batch_condition_matches(condition: SetCondition, old_values: &[Option<Vec<u8>>]) -> bool {
    match condition {
        SetCondition::Always => true,
        SetCondition::Nx => old_values.iter().all(Option::is_none),
        SetCondition::Xx => old_values.iter().all(Option::is_some),
    }
}

fn expiration_ms(expiration: SetExpiration, old_expiration: Option<u64>) -> u64 {
    match expiration {
        SetExpiration::Clear => 0,
        SetExpiration::KeepTtl => old_expiration.unwrap_or(0),
        SetExpiration::At(expire_ms) => expire_ms,
    }
}
