use super::*;

pub type ZsetEntry = (String, f64);
pub type ZsetMultiPopResult = Option<(String, Vec<ZsetEntry>)>;

impl Db {
    fn try_zset_pop_packed(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Option<Vec<ZsetEntry>>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed(key);
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(Some(Vec::new()));
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
            if header.type_tag != TYPE_SORTED_SET {
                return Err(Error::msg(WRONG_TYPE_ERROR));
            }
            let Some(mut packed) = decode_packed_zset(raw) else {
                return Ok(None);
            };
            let mut ranked = packed
                .iter()
                .map(|(member, score)| (member.clone(), *score))
                .collect::<Vec<_>>();
            ranked.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            if !min {
                ranked.reverse();
            }
            let selected = ranked.into_iter().take(count).collect::<Vec<_>>();
            if selected.is_empty() {
                return Ok(Some(Vec::new()));
            }
            for (member, _) in &selected {
                packed.remove(member);
            }
            let mut batch = WriteBatch::new();
            if packed.is_empty() {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
            } else {
                batch.put(
                    &key_bytes,
                    &encode_packed_zset(header.expire_ms, &packed)
                        .expect("removing entries cannot overflow packed sorted set"),
                )?;
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(selected));
            }
        }
        Err(Error::msg("ERR sorted set pop write conflict"))
    }

    async fn try_zset_pop_packed_async(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Option<Vec<ZsetEntry>>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return Ok(Some(Vec::new()));
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
            if header.type_tag != TYPE_SORTED_SET {
                return Err(Error::msg(WRONG_TYPE_ERROR));
            }
            let Some(mut packed) = decode_packed_zset(raw) else {
                return Ok(None);
            };
            let mut ranked = packed
                .iter()
                .map(|(member, score)| (member.clone(), *score))
                .collect::<Vec<_>>();
            ranked.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            if !min {
                ranked.reverse();
            }
            let selected = ranked.into_iter().take(count).collect::<Vec<_>>();
            if selected.is_empty() {
                return Ok(Some(Vec::new()));
            }
            for (member, _) in &selected {
                packed.remove(member);
            }
            let mut batch = WriteBatch::new();
            if packed.is_empty() {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, header.expire_ms);
            } else {
                batch.put(
                    &key_bytes,
                    &encode_packed_zset(header.expire_ms, &packed)
                        .expect("removing entries cannot overflow packed sorted set"),
                )?;
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(selected));
            }
        }
        Err(Error::msg("ERR sorted set pop write conflict"))
    }

    /// Apply ordered ZPOPMIN/ZPOPMAX commands with at most one bounded scan from each end/key.
    pub(crate) async fn zset_pop_batch_async(
        &self,
        commands: &[(&str, bool, usize)],
    ) -> Vec<Result<Vec<ZsetEntry>, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }

        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::with_capacity(commands.len());
        let mut requested_min = Vec::<usize>::new();
        let mut requested_max = Vec::<usize>::new();
        for (key, min, count) in commands {
            let position = *key_positions.entry(key).or_insert_with(|| {
                let position = keys.len();
                keys.push(*key);
                requested_min.push(0);
                requested_max.push(0);
                position
            });
            if *min {
                requested_min[position] = requested_min[position].saturating_add(*count);
            } else {
                requested_max[position] = requested_max[position].saturating_add(*count);
            }
        }
        let shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _write_guards = self.lock_write_shards(&shards).await;

        for key in &keys {
            if let Err(error) = self.promote_packed_zset_async(key).await {
                let message = error.to_string();
                return commands
                    .iter()
                    .map(|_| Err(Error::msg(message.clone())))
                    .collect();
            }
        }

        for _ in 0..64 {
            for key in &keys {
                self.expire_if_needed_async(key).await;
            }
            let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
            let mut states = observations
                .iter()
                .map(|observed| ZsetPopBatchState::from_raw(observed.value().map(AsRef::as_ref)))
                .collect::<Vec<_>>();

            for position in 0..keys.len() {
                let Ok(state) = &mut states[position] else {
                    continue;
                };
                let Some(version) = state.version else {
                    continue;
                };
                let prefix = zset_rank_prefix(self.db_index, keys[position], version);
                let upper = prefix_exclusive_upper_bound(&prefix);
                let min_limit = requested_min[position].saturating_add(1);
                let max_limit = requested_max[position].saturating_add(1);
                if requested_min[position] > 0 {
                    for (rank_key, _) in self
                        .store
                        .scan_range_raw_limited_async(&prefix, upper.clone(), min_limit)
                        .await
                    {
                        state.insert_ranked(self, keys[position], version, rank_key);
                    }
                }
                if requested_max[position] > 0 {
                    for (rank_key, _) in self
                        .store
                        .scan_range_raw_limited_reverse_async(&prefix, upper, max_limit)
                        .await
                    {
                        state.insert_ranked(self, keys[position], version, rank_key);
                    }
                }
            }

            let mut replies = Vec::with_capacity(commands.len());
            let mut removed = Vec::<(usize, Vec<u8>, String)>::new();
            for (key, min, count) in commands {
                let position = key_positions[key];
                let result = match &mut states[position] {
                    Err(error) => Err(Error::msg(error.to_string())),
                    Ok(state) => {
                        let mut entries = Vec::with_capacity(*count);
                        for _ in 0..*count {
                            let next = if *min {
                                state.candidates.pop_first()
                            } else {
                                state.candidates.pop_last()
                            };
                            let Some((rank_key, (member, score))) = next else {
                                break;
                            };
                            removed.push((position, rank_key, member.clone()));
                            entries.push((member, score));
                        }
                        Ok(entries)
                    }
                };
                replies.push(result);
            }
            if removed.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            let mut dirty_positions = BTreeSet::new();
            for (position, rank_key, member) in removed {
                let version = states[position]
                    .as_ref()
                    .ok()
                    .and_then(|state| state.version)
                    .expect("removed zset entry has a version");
                (batch.delete(&rank_key)).expect("write batch append invariant violated");
                (batch.delete(&zset_member_key(
                    self.db_index,
                    keys[position],
                    version,
                    &member,
                )))
                .expect("write batch append invariant violated");
                dirty_positions.insert(position);
            }
            for &position in &dirty_positions {
                let state = states[position]
                    .as_ref()
                    .expect("dirty zset pop state is valid");
                if state.candidates.is_empty() {
                    self.delete_main_key_with_ttl_to_batch(
                        &mut batch,
                        keys[position],
                        state.expire_ms,
                    );
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
                    let changed = replies
                        .iter()
                        .filter(|reply| reply.as_ref().is_ok_and(|entries| !entries.is_empty()))
                        .count() as u64;
                    self.changes.fetch_add(changed, Ordering::Relaxed);
                    return replies;
                }
                Ok(false) => continue,
                Err(error) => {
                    let message = error.to_string();
                    return commands
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }

        commands
            .iter()
            .map(|_| Err(Error::msg("ERR sorted set pop batch write conflict")))
            .collect()
    }

    pub fn zset_pop(&self, key: &str, min: bool, count: usize) -> Result<Vec<ZsetEntry>, Error> {
        self.zset_pop_unlocked(key, min, count)
    }

    pub async fn zset_pop_async(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<ZsetEntry>, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.zset_pop_async_unlocked(key, min, count).await
    }

    async fn zset_pop_async_unlocked(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<ZsetEntry>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if let Some(entries) = self.try_zset_pop_packed_async(key, min, count).await? {
            return Ok(entries);
        }
        let Some((expire_ms, version)) = self.zset_expire_ms_async(key).await? else {
            return Ok(Vec::new());
        };
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let scan_limit = count.saturating_add(1);
        let raw_entries = if min {
            self.store
                .scan_range_raw_limited_async(&prefix, upper, scan_limit)
                .await
        } else {
            self.store
                .scan_range_raw_limited_reverse_async(&prefix, upper, scan_limit)
                .await
        };
        let (entries, batch) =
            self.prepare_ranked_zset_removal(key, expire_ms, version, raw_entries, count)?;
        if !entries.is_empty() {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(entries)
    }

    fn zset_pop_unlocked(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<ZsetEntry>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if let Some(entries) = self.try_zset_pop_packed(key, min, count)? {
            return Ok(entries);
        }
        let Some((expire_ms, version)) = self.zset_expire_ms(key)? else {
            return Ok(Vec::new());
        };
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let scan_limit = count.saturating_add(1);
        let raw_entries = if min {
            self.store
                .scan_range_raw_limited(&prefix, upper, scan_limit)
        } else {
            self.store
                .scan_range_raw_limited_reverse(&prefix, upper, scan_limit)
        };
        let (entries, batch) =
            self.prepare_ranked_zset_removal(key, expire_ms, version, raw_entries, count)?;
        if !entries.is_empty() {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(entries)
    }

    fn prepare_ranked_zset_removal(
        &self,
        key: &str,
        expire_ms: u64,
        version: u64,
        raw_entries: Vec<(Vec<u8>, Vec<u8>)>,
        count: usize,
    ) -> Result<(Vec<ZsetEntry>, WriteBatch), Error> {
        let has_more = raw_entries.len() > count;
        let mut entries = Vec::with_capacity(raw_entries.len().min(count));
        let mut batch = WriteBatch::new();
        for (rank_key, _) in raw_entries.into_iter().take(count) {
            let Some(score) = self.decode_rank_score(key, version, &rank_key) else {
                continue;
            };
            let Some(member) = self.decode_rank_member(key, version, &rank_key) else {
                continue;
            };
            (batch.delete(&rank_key)).expect("write batch append invariant violated");
            (batch.delete(&zset_member_key(self.db_index, key, version, &member)))
                .expect("write batch append invariant violated");
            entries.push((member, score));
        }
        if entries.is_empty() {
            return Ok((entries, batch));
        }
        if !has_more {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
        }
        Ok((entries, batch))
    }

    pub fn zset_multi_pop(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<ZsetMultiPopResult, Error> {
        for key in keys {
            let entries = self.zset_pop(key, min, count)?;
            if !entries.is_empty() {
                return Ok(Some((key.clone(), entries)));
            }
        }
        Ok(None)
    }

    pub async fn zset_multi_pop_async(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<ZsetMultiPopResult, Error> {
        let mut shards = keys
            .iter()
            .map(|key| key_write_lock_shard(self.db_index, key))
            .collect::<Vec<_>>();
        shards.sort_unstable();
        shards.dedup();
        let mut guards = Vec::with_capacity(shards.len());
        for shard in shards {
            guards.push(self.key_write_locks[shard].lock().await);
        }

        for key in keys {
            let entries = self.zset_pop_async_unlocked(key, min, count).await?;
            if !entries.is_empty() {
                return Ok(Some((key.clone(), entries)));
            }
        }
        Ok(None)
    }
}

struct ZsetPopBatchState {
    version: Option<u64>,
    expire_ms: u64,
    candidates: BTreeMap<Vec<u8>, (String, f64)>,
}

impl ZsetPopBatchState {
    fn from_raw(raw: Option<&[u8]>) -> Result<Self, Error> {
        let Some(raw) = raw else {
            return Ok(Self {
                version: None,
                expire_ms: 0,
                candidates: BTreeMap::new(),
            });
        };
        let header = decode_meta_header(raw)
            .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
        if header.type_tag != TYPE_SORTED_SET {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Ok(Self {
            version: Some(header.version),
            expire_ms: header.expire_ms,
            candidates: BTreeMap::new(),
        })
    }

    fn insert_ranked(&mut self, db: &Db, key: &str, version: u64, rank_key: Vec<u8>) {
        let Some(member) = db.decode_rank_member(key, version, &rank_key) else {
            return;
        };
        let Some(score) = db.decode_rank_score(key, version, &rank_key) else {
            return;
        };
        self.candidates.insert(rank_key, (member, score));
    }
}
