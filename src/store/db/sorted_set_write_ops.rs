use super::*;

// A small bounded retry window lets field-local writers finish without taking the structural
// barrier. Persistent hot-member contention still falls back to the exclusive path, so retries
// cannot turn into an unbounded storm.
const ZSET_ADD_SHARED_CAS_ATTEMPTS: usize = 3;
const ZSET_ADD_MERGE_BATCH_MAX: usize = 1_024;
const ZSET_INCREMENT_MERGE_BATCH_MAX: usize = 256;

enum ZsetAddAttempt {
    Applied(ZsetAddOutcome),
    Conflict,
}

enum ZsetIncrementBatchKeyState {
    Valid {
        version: u64,
        initially_exists: bool,
        expired_at: Option<u64>,
    },
    Error(String),
}

enum ZsetAddBatchKeyState {
    Valid {
        version: u64,
        initially_exists: bool,
        expired_at: Option<u64>,
    },
    Error(String),
}

impl Db {
    /// Apply plain ZADD commands in pipeline order with one read per distinct member and one
    /// conditional commit. Duplicate members inside one command retain the last supplied score,
    /// matching the ordinary ZADD path.
    pub(crate) async fn zset_add_batch_async(
        &self,
        commands: &[(&str, Vec<(f64, &str)>)],
    ) -> Vec<Result<usize, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }

        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::new();
        for (key, _) in commands {
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let key_shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _structural_guards = self.lock_read_shards(&key_shards).await;

        let mut member_shards = commands
            .iter()
            .flat_map(|(key, members)| {
                members
                    .iter()
                    .map(move |(_, member)| hash_field_write_lock_shard(self.db_index, key, member))
            })
            .collect::<Vec<_>>();
        member_shards.sort_unstable();
        member_shards.dedup();
        let _member_guards = self.lock_hash_field_write_shards(&member_shards).await;

        for _ in 0..64 {
            let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let meta_observations = self.store.multi_get_raw_observed_async(&meta_keys).await;
            let now = now_ms();
            let mut states = Vec::with_capacity(keys.len());
            for observed in &meta_observations {
                let state = match observed.value() {
                    None => ZsetAddBatchKeyState::Valid {
                        version: self.next_version_async().await,
                        initially_exists: false,
                        expired_at: None,
                    },
                    Some(raw) => match decode_meta_header(raw) {
                        None => ZsetAddBatchKeyState::Error(
                            "Failed to decode sorted set metadata".to_string(),
                        ),
                        Some(header) if header.expire_ms > 0 && now >= header.expire_ms => {
                            ZsetAddBatchKeyState::Valid {
                                version: self.next_version_async().await,
                                initially_exists: false,
                                expired_at: Some(header.expire_ms),
                            }
                        }
                        Some(header) if header.type_tag != TYPE_SORTED_SET => {
                            ZsetAddBatchKeyState::Error(WRONG_TYPE_ERROR.to_string())
                        }
                        Some(header) => ZsetAddBatchKeyState::Valid {
                            version: header.version,
                            initially_exists: true,
                            expired_at: None,
                        },
                    },
                };
                states.push(state);
            }

            let mut pair_positions = HashMap::<(usize, &str), usize>::new();
            let mut pairs = Vec::new();
            for (key, members) in commands {
                let key_position = key_positions[key];
                let ZsetAddBatchKeyState::Valid { version, .. } = states[key_position] else {
                    continue;
                };
                for (_, member) in members {
                    if !pair_positions.contains_key(&(key_position, *member)) {
                        pair_positions.insert((key_position, *member), pairs.len());
                        pairs.push((
                            key_position,
                            *member,
                            zset_member_key(self.db_index, key, version, member),
                        ));
                    }
                }
            }
            let member_keys = pairs
                .iter()
                .map(|(_, _, key)| key.clone())
                .collect::<Vec<_>>();
            let member_observations = self.store.multi_get_raw_observed_async(&member_keys).await;
            let initial_scores = member_observations
                .iter()
                .map(|observed| {
                    observed
                        .value()
                        .map(Bytes::as_ref)
                        .and_then(decode_zset_score)
                })
                .collect::<Vec<_>>();
            let mut scores = initial_scores.clone();
            let mut touched_pairs = vec![false; pairs.len()];
            let mut replies = Vec::with_capacity(commands.len());
            let mut changed_commands = 0u64;

            for (key, members) in commands {
                let key_position = key_positions[key];
                if let ZsetAddBatchKeyState::Error(message) = &states[key_position] {
                    replies.push(Err(Error::msg(message.clone())));
                    continue;
                }
                let mut seen_members = HashSet::with_capacity(members.len());
                let mut added = 0usize;
                let mut changed = false;
                for (score, member) in members.iter().rev() {
                    if !seen_members.insert(*member) {
                        continue;
                    }
                    let pair_position = pair_positions[&(key_position, *member)];
                    if scores[pair_position].is_none() {
                        added += 1;
                    }
                    if scores[pair_position] != Some(*score) {
                        scores[pair_position] = Some(*score);
                        touched_pairs[pair_position] = true;
                        changed = true;
                    }
                }
                changed_commands += u64::from(changed);
                replies.push(Ok(added));
            }

            let dirty_pairs = touched_pairs
                .iter()
                .enumerate()
                .filter_map(|(position, touched)| touched.then_some(position))
                .collect::<Vec<_>>();
            if dirty_pairs.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            let mut conditions = Vec::with_capacity(dirty_pairs.len() + keys.len());
            let mut dirty_keys = HashSet::new();
            for pair_position in dirty_pairs {
                let (key_position, member, member_key) = &pairs[pair_position];
                let key = keys[*key_position];
                let ZsetAddBatchKeyState::Valid { version, .. } = states[*key_position] else {
                    unreachable!("dirty member belongs to a valid sorted set")
                };
                if let Some(old_score) = initial_scores[pair_position] {
                    batch.delete(&zset_rank_key(
                        self.db_index,
                        key,
                        version,
                        old_score,
                        member,
                    ));
                }
                let score = scores[pair_position].expect("dirty ZADD member has a score");
                batch.put(member_key, &score.to_be_bytes());
                batch.put(
                    &zset_rank_key(self.db_index, key, version, score, member),
                    INDEX_MARKER_VALUE,
                );
                conditions.push(member_observations[pair_position].condition());
                dirty_keys.insert(*key_position);
            }
            for key_position in dirty_keys {
                let ZsetAddBatchKeyState::Valid {
                    version,
                    initially_exists,
                    expired_at,
                } = states[key_position]
                else {
                    unreachable!("dirty key is valid")
                };
                if !initially_exists {
                    let key = keys[key_position];
                    if let Some(expire_ms) = expired_at {
                        self.ttl_manager.remove_known_to_batch(
                            &mut batch,
                            expire_ms,
                            self.db_index,
                            key,
                        );
                    }
                    batch.put(&meta_keys[key_position], &encode_zset_meta(0, version));
                    conditions.push(meta_observations[key_position].condition());
                }
            }

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
                    return commands
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }

        commands
            .iter()
            .map(|_| Err(Error::msg("ERR sorted set add batch write conflict")))
            .collect()
    }

    /// Merge plain one-member ZADDs for one hot member across connections. A single no-op stays
    /// on the read-only path; score changes share one ordered batch and one storage commit.
    pub(crate) async fn zset_add_many_merged_async(
        &self,
        commands: &[(&str, Vec<(f64, &str)>)],
    ) -> Vec<Result<usize, Error>> {
        let Some((first_key, first_members)) = commands.first() else {
            return Vec::new();
        };
        let Some((_, first_member)) = first_members.first() else {
            return self.zset_add_batch_async(commands).await;
        };
        if self.store.is_transactional()
            || commands.iter().any(|(key, members)| {
                *key != *first_key || members.len() != 1 || members[0].1 != *first_member
            })
        {
            if let [(key, members)] = commands {
                let members = members
                    .iter()
                    .map(|(score, member)| (*score, (*member).to_string()))
                    .collect::<Vec<_>>();
                return vec![self.zset_add_async(key, &members).await];
            }
            return self.zset_add_batch_async(commands).await;
        }

        if commands
            .iter()
            .all(|(_, members)| members[0].0 == first_members[0].0)
        {
            match self.zset_score_async(first_key, first_member).await {
                Ok(Some(current)) if current == first_members[0].0 => {
                    return commands.iter().map(|_| Ok(0)).collect();
                }
                Ok(_) => {}
                Err(error) => {
                    let message = error.to_string();
                    return commands
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }

        let mut receivers = Vec::with_capacity(commands.len());
        for (_, members) in commands {
            receivers.push(self.enqueue_zset_add(first_key, first_member, members[0].0));
        }
        let mut replies = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            replies.push(
                receiver
                    .await
                    .unwrap_or_else(|_| Err(Error::msg("ERR sorted set add merger stopped"))),
            );
        }
        replies
    }

    fn enqueue_zset_add(
        &self,
        key: &str,
        member: &str,
        score: f64,
    ) -> tokio::sync::oneshot::Receiver<Result<usize, Error>> {
        let queue_key = (
            self.db_index,
            key.as_bytes().to_vec(),
            member.as_bytes().to_vec(),
        );
        let queue = self
            .counter_cache
            .zset_add_queues
            .entry(queue_key)
            .or_insert_with(|| Arc::new(ZsetAddMergeQueue::default()))
            .clone();
        let (reply, result) = tokio::sync::oneshot::channel();
        queue
            .pending
            .lock()
            .expect("zset add merge queue mutex poisoned")
            .push_back(ZsetAddMergeRequest { score, reply });
        if queue
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.spawn_zset_add_merge_worker(key.to_string(), member.to_string(), queue.clone());
        }
        drop(queue);
        result
    }

    fn spawn_zset_add_merge_worker(
        &self,
        key: String,
        member: String,
        queue: Arc<ZsetAddMergeQueue>,
    ) {
        let db = self.shared_task_view();
        let queue_key = (
            self.db_index,
            key.as_bytes().to_vec(),
            member.as_bytes().to_vec(),
        );
        tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                let requests = {
                    let mut pending = queue
                        .pending
                        .lock()
                        .expect("zset add merge queue mutex poisoned");
                    let count = pending.len().min(ZSET_ADD_MERGE_BATCH_MAX);
                    pending.drain(..count).collect::<Vec<_>>()
                };
                if !requests.is_empty() {
                    let commands = requests
                        .iter()
                        .map(|request| (key.as_str(), vec![(request.score, member.as_str())]))
                        .collect::<Vec<_>>();
                    let replies = db.zset_add_batch_async(&commands).await;
                    for (request, reply) in requests.into_iter().zip(replies) {
                        let _ = request.reply.send(reply);
                    }
                }

                let pending = queue
                    .pending
                    .lock()
                    .expect("zset add merge queue mutex poisoned");
                if pending.is_empty() {
                    queue.running.store(false, Ordering::Release);
                    db.counter_cache
                        .zset_add_queues
                        .remove_if(&queue_key, |_, existing| {
                            Arc::ptr_eq(existing, &queue) && Arc::strong_count(existing) == 2
                        });
                    break;
                }
                drop(pending);
            }
        });
    }

    /// Apply an ordered ZINCRBY pipeline with one observed read per distinct member and one CAS
    /// write. Commands for the same member see prior commands in pipeline order.
    pub(crate) async fn zset_increment_batch_async(
        &self,
        commands: &[(&str, f64, &str)],
    ) -> Vec<Result<f64, Error>> {
        if commands.is_empty() {
            return Vec::new();
        }

        let mut key_positions = HashMap::<&str, usize>::with_capacity(commands.len());
        let mut keys = Vec::<&str>::new();
        for (key, _, _) in commands {
            if !key_positions.contains_key(key) {
                key_positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        let key_shards =
            unique_key_write_lock_shards(self.db_index, keys.iter().map(|key| key.as_bytes()));
        let _structural_guards = self.lock_read_shards(&key_shards).await;

        let mut member_shards = commands
            .iter()
            .map(|(key, _, member)| hash_field_write_lock_shard(self.db_index, key, member))
            .collect::<Vec<_>>();
        member_shards.sort_unstable();
        member_shards.dedup();
        let _member_guards = self.lock_hash_field_write_shards(&member_shards).await;

        for _ in 0..64 {
            let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
            let meta_observations = self.store.multi_get_raw_observed_async(&meta_keys).await;
            let now = now_ms();
            let mut states = Vec::with_capacity(keys.len());
            for observed in &meta_observations {
                let state = match observed.value() {
                    None => ZsetIncrementBatchKeyState::Valid {
                        version: self.next_version_async().await,
                        initially_exists: false,
                        expired_at: None,
                    },
                    Some(raw) => match decode_meta_header(raw) {
                        None => ZsetIncrementBatchKeyState::Error(
                            "Failed to decode sorted set metadata".to_string(),
                        ),
                        Some(header) if header.expire_ms > 0 && now >= header.expire_ms => {
                            ZsetIncrementBatchKeyState::Valid {
                                version: self.next_version_async().await,
                                initially_exists: false,
                                expired_at: Some(header.expire_ms),
                            }
                        }
                        Some(header) if header.type_tag != TYPE_SORTED_SET => {
                            ZsetIncrementBatchKeyState::Error(WRONG_TYPE_ERROR.to_string())
                        }
                        Some(header) => ZsetIncrementBatchKeyState::Valid {
                            version: header.version,
                            initially_exists: true,
                            expired_at: None,
                        },
                    },
                };
                states.push(state);
            }

            let mut pair_positions = HashMap::<(usize, &str), usize>::new();
            let mut pairs = Vec::new();
            for (key, _, member) in commands {
                let key_position = key_positions[key];
                let ZsetIncrementBatchKeyState::Valid { version, .. } = states[key_position] else {
                    continue;
                };
                if !pair_positions.contains_key(&(key_position, *member)) {
                    pair_positions.insert((key_position, *member), pairs.len());
                    pairs.push((
                        key_position,
                        *member,
                        zset_member_key(self.db_index, key, version, member),
                    ));
                }
            }
            let member_keys = pairs
                .iter()
                .map(|(_, _, key)| key.clone())
                .collect::<Vec<_>>();
            let member_observations = self.store.multi_get_raw_observed_async(&member_keys).await;
            let initial_scores = member_observations
                .iter()
                .map(|observed| {
                    observed
                        .value()
                        .map(Bytes::as_ref)
                        .and_then(decode_zset_score)
                })
                .collect::<Vec<_>>();
            let mut scores = initial_scores.clone();
            let mut touched_pairs = vec![false; initial_scores.len()];
            let mut replies = Vec::with_capacity(commands.len());
            let mut changed_commands = 0u64;

            for (key, increment, member) in commands {
                let key_position = key_positions[key];
                if let ZsetIncrementBatchKeyState::Error(message) = &states[key_position] {
                    replies.push(Err(Error::msg(message.clone())));
                    continue;
                }
                let pair_position = pair_positions[&(key_position, *member)];
                let previous = scores[pair_position];
                let next = previous.unwrap_or(0.0) + increment;
                if next.is_nan() {
                    replies.push(Err(Error::msg("ERR resulting score is not a number (NaN)")));
                    continue;
                }
                if previous != Some(next) {
                    changed_commands += 1;
                    touched_pairs[pair_position] = true;
                }
                scores[pair_position] = Some(next);
                replies.push(Ok(next));
            }

            let dirty_pairs = touched_pairs
                .iter()
                .enumerate()
                .filter_map(|(position, touched)| touched.then_some(position))
                .collect::<Vec<_>>();
            if dirty_pairs.is_empty() {
                return replies;
            }

            let mut batch = WriteBatch::new();
            let mut conditions = Vec::with_capacity(dirty_pairs.len() + keys.len());
            let mut dirty_keys = HashSet::new();
            for pair_position in dirty_pairs {
                let (key_position, member, member_key) = &pairs[pair_position];
                let key = keys[*key_position];
                let ZsetIncrementBatchKeyState::Valid { version, .. } = states[*key_position]
                else {
                    unreachable!("dirty member belongs to a valid sorted set")
                };
                if let Some(old_score) = initial_scores[pair_position] {
                    batch.delete(&zset_rank_key(
                        self.db_index,
                        key,
                        version,
                        old_score,
                        member,
                    ));
                }
                let score = scores[pair_position].expect("successful increment has a score");
                batch.put(member_key, &score.to_be_bytes());
                batch.put(
                    &zset_rank_key(self.db_index, key, version, score, member),
                    INDEX_MARKER_VALUE,
                );
                conditions.push(member_observations[pair_position].condition());
                dirty_keys.insert(*key_position);
            }
            for key_position in dirty_keys {
                let ZsetIncrementBatchKeyState::Valid {
                    version,
                    initially_exists,
                    expired_at,
                } = states[key_position]
                else {
                    unreachable!("dirty key is valid")
                };
                if !initially_exists {
                    let key = keys[key_position];
                    if let Some(expire_ms) = expired_at {
                        self.ttl_manager.remove_known_to_batch(
                            &mut batch,
                            expire_ms,
                            self.db_index,
                            key,
                        );
                    }
                    batch.put(&meta_keys[key_position], &encode_zset_meta(0, version));
                    conditions.push(meta_observations[key_position].condition());
                }
            }

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
                    return commands
                        .iter()
                        .map(|_| Err(Error::msg(message.clone())))
                        .collect();
                }
            }
        }

        commands
            .iter()
            .map(|_| Err(Error::msg("ERR sorted set increment batch write conflict")))
            .collect()
    }

    pub fn zset_add(&self, key: &str, members: &[(f64, String)]) -> Result<usize, Error> {
        let exists = self.zset_expire_ms(key)?;
        let version = match exists {
            Some((_, v)) => v,
            None => self.next_version(),
        };
        let mut batch = WriteBatch::new();
        let mut added = 0usize;
        let mut seen_members = std::collections::HashSet::new();

        if exists.is_none() {
            batch.put(&self.mk(key), &encode_zset_meta(0, version));
        }

        for (score, member) in members.iter().rev() {
            if !seen_members.insert(member.clone()) {
                continue;
            }
            let member_key = zset_member_key(self.db_index, key, version, member);
            let previous_score = self
                .store
                .get_raw(&member_key)
                .and_then(|value| decode_zset_score(&value));

            if previous_score.is_none() {
                added += 1;
            }
            if let Some(old_score) = previous_score {
                batch.delete(&zset_rank_key(
                    self.db_index,
                    key,
                    version,
                    old_score,
                    member,
                ));
            }
            batch.put(&member_key, &score.to_be_bytes());
            batch.put(
                &zset_rank_key(self.db_index, key, version, *score, member),
                INDEX_MARKER_VALUE,
            );
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(added)
    }

    pub async fn zset_add_async(
        &self,
        key: &str,
        members: &[(f64, String)],
    ) -> Result<usize, Error> {
        Ok(self
            .zset_add_with_options_async(key, members, ZsetAddOptions::default())
            .await?
            .added)
    }

    pub fn zset_add_with_options(
        &self,
        key: &str,
        members: &[(f64, String)],
        options: ZsetAddOptions,
    ) -> Result<ZsetAddOutcome, Error> {
        let exists = self.zset_expire_ms(key)?;
        let version = match exists {
            Some((_, version)) => version,
            None => self.next_version(),
        };
        let mut batch = WriteBatch::new();
        let mut outcome = ZsetAddOutcome::default();
        let mut seen_members = HashSet::new();

        for (input_score, member) in members.iter().rev() {
            if !seen_members.insert(member) {
                continue;
            }
            let member_key = zset_member_key(self.db_index, key, version, member);
            let previous_score = self
                .store
                .get_raw(&member_key)
                .and_then(|value| decode_zset_score(&value));
            let score = if options.increment {
                let next = previous_score.unwrap_or(0.0) + input_score;
                if next.is_nan() {
                    return Err(Error::msg("ERR resulting score is not a number (NaN)"));
                }
                next
            } else {
                *input_score
            };
            if !zset_add_condition_matches(previous_score, score, options) {
                continue;
            }

            outcome.applied = true;
            outcome.score = options.increment.then_some(score);
            if previous_score.is_none() {
                outcome.added += 1;
            }
            if previous_score != Some(score) {
                outcome.changed += 1;
                if let Some(old_score) = previous_score {
                    batch.delete(&zset_rank_key(
                        self.db_index,
                        key,
                        version,
                        old_score,
                        member,
                    ));
                }
                batch.put(&member_key, &score.to_be_bytes());
                batch.put(
                    &zset_rank_key(self.db_index, key, version, score, member),
                    INDEX_MARKER_VALUE,
                );
            }
        }

        if outcome.changed > 0 {
            if exists.is_none() {
                batch.put(&self.mk(key), &encode_zset_meta(0, version));
            }
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(outcome)
    }

    pub async fn zset_add_with_options_async(
        &self,
        key: &str,
        members: &[(f64, String)],
        options: ZsetAddOptions,
    ) -> Result<ZsetAddOutcome, Error> {
        for _ in 0..ZSET_ADD_SHARED_CAS_ATTEMPTS {
            let structural_guard = self.set_write_lock(key).read().await;
            match self
                .zset_add_with_options_async_attempt(key, members, options)
                .await?
            {
                ZsetAddAttempt::Applied(outcome) => return Ok(outcome),
                ZsetAddAttempt::Conflict => {}
            }
            drop(structural_guard);
            tokio::task::yield_now().await;
        }

        let _write_guard = self.set_write_lock(key).lock().await;
        loop {
            match self
                .zset_add_with_options_async_attempt(key, members, options)
                .await?
            {
                ZsetAddAttempt::Applied(outcome) => return Ok(outcome),
                ZsetAddAttempt::Conflict => tokio::task::yield_now().await,
            }
        }
    }

    async fn zset_add_with_options_async_attempt(
        &self,
        key: &str,
        members: &[(f64, String)],
        options: ZsetAddOptions,
    ) -> Result<ZsetAddAttempt, Error> {
        let key_bytes = self.mk(key);
        let raw_meta = self.store.get_raw_async(&key_bytes).await;
        let mut expired_at = None;
        let exists = match raw_meta.as_deref() {
            Some(raw) => {
                let header = decode_meta_header(raw)
                    .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
                if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                    expired_at = Some(header.expire_ms);
                    None
                } else {
                    if header.type_tag != TYPE_SORTED_SET {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    Some((header.expire_ms, header.version))
                }
            }
            None => None,
        };
        // Existing structures are protected against structural replacement by the shared key
        // barrier, so their metadata is not part of the member-local CAS. Creation/replacement
        // still observes the main key: if another shared creator won between the plain read and
        // this observation, retry against its version instead of overwriting it.
        let observed_meta = if exists.is_none() {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            if observed.value().map(Bytes::as_ref) != raw_meta.as_deref() {
                return Ok(ZsetAddAttempt::Conflict);
            }
            Some(observed)
        } else {
            None
        };
        let version = match exists {
            Some((_, version)) => version,
            None => self.next_version_async().await,
        };

        let mut seen_members = HashSet::with_capacity(members.len());
        let unique_members = members
            .iter()
            .rev()
            .filter(|(_, member)| seen_members.insert(member.as_str()))
            .collect::<Vec<_>>();
        let member_keys = unique_members
            .iter()
            .map(|(_, member)| zset_member_key(self.db_index, key, version, member))
            .collect::<Vec<_>>();
        let previous_observed = if exists.is_some() {
            self.store
                .multi_get_raw_observed_async(&member_keys)
                .await
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>()
        } else {
            std::iter::repeat_with(|| None)
                .take(member_keys.len())
                .collect::<Vec<_>>()
        };

        let mut batch = WriteBatch::new();
        let mut conditions = Vec::new();
        let mut outcome = ZsetAddOutcome::default();
        for (((input_score, member), member_key), observed_member) in unique_members
            .into_iter()
            .zip(&member_keys)
            .zip(previous_observed)
        {
            let old_raw = observed_member
                .as_ref()
                .and_then(|observed| observed.value());
            let previous_score = old_raw.map(Bytes::as_ref).and_then(decode_zset_score);
            let score = if options.increment {
                let next = previous_score.unwrap_or(0.0) + input_score;
                if next.is_nan() {
                    return Err(Error::msg("ERR resulting score is not a number (NaN)"));
                }
                next
            } else {
                *input_score
            };
            if !zset_add_condition_matches(previous_score, score, options) {
                continue;
            }

            outcome.applied = true;
            outcome.score = options.increment.then_some(score);
            if previous_score.is_none() {
                outcome.added += 1;
            }
            if previous_score == Some(score) {
                continue;
            }

            outcome.changed += 1;
            if let Some(observed_member) = observed_member {
                conditions.push(observed_member.condition());
            }
            if let Some(old_score) = previous_score {
                batch.delete(&zset_rank_key(
                    self.db_index,
                    key,
                    version,
                    old_score,
                    member,
                ));
            }
            batch.put(member_key, &score.to_be_bytes());
            batch.put(
                &zset_rank_key(self.db_index, key, version, score, member),
                INDEX_MARKER_VALUE,
            );
        }

        if outcome.changed == 0 {
            return Ok(ZsetAddAttempt::Applied(outcome));
        }
        if let Some(expire_ms) = expired_at {
            self.ttl_manager
                .remove_known_to_batch(&mut batch, expire_ms, self.db_index, key);
        }
        if exists.is_none() {
            batch.put(&key_bytes, &encode_zset_meta(0, version));
            conditions.push(
                observed_meta
                    .expect("missing or expired metadata was observed")
                    .condition(),
            );
        }
        if !self
            .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
            .await?
        {
            return Ok(ZsetAddAttempt::Conflict);
        }
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(ZsetAddAttempt::Applied(outcome))
    }

    pub(in crate::store::db) async fn zset_add_async_unlocked(
        &self,
        key: &str,
        members: &[(f64, String)],
    ) -> Result<usize, Error> {
        let exists = self.zset_expire_ms_async(key).await?;
        let version = match exists {
            Some((_, v)) => v,
            None => self.next_version_async().await,
        };
        let mut batch = WriteBatch::new();
        let mut added = 0usize;
        let mut seen_members = std::collections::HashSet::new();

        if exists.is_none() {
            batch.put(&self.mk(key), &encode_zset_meta(0, version));
        }

        for (score, member) in members.iter().rev() {
            if !seen_members.insert(member.clone()) {
                continue;
            }
            let member_key = zset_member_key(self.db_index, key, version, member);
            let previous_score = self
                .store
                .get_raw_async(&member_key)
                .await
                .and_then(|value| decode_zset_score(&value));

            if previous_score.is_none() {
                added += 1;
            }
            if let Some(old_score) = previous_score {
                batch.delete(&zset_rank_key(
                    self.db_index,
                    key,
                    version,
                    old_score,
                    member,
                ));
            }
            batch.put(&member_key, &score.to_be_bytes());
            batch.put(
                &zset_rank_key(self.db_index, key, version, *score, member),
                INDEX_MARKER_VALUE,
            );
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(added)
    }

    /// 删除 zset members，返回实际删除数量。
    pub fn zset_remove(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((expire_ms, version)) = meta else {
            return Ok(0);
        };

        let mut batch = WriteBatch::new();
        let mut removed = 0usize;
        let mut seen_members = std::collections::HashSet::new();
        for member in members {
            if !seen_members.insert(member) {
                continue;
            }
            let member_key = zset_member_key(self.db_index, key, version, member);
            let Some(score) = self
                .store
                .get_raw(&member_key)
                .and_then(|value| decode_zset_score(&value))
            else {
                continue;
            };

            batch.delete(&member_key);
            batch.delete(&zset_rank_key(self.db_index, key, version, score, member));
            removed += 1;
        }

        if removed > 0 {
            let prefix = zset_member_prefix(self.db_index, key, version);
            let existing_count = self
                .store
                .scan_range_raw_limited(
                    &prefix,
                    prefix_exclusive_upper_bound(&prefix),
                    removed.saturating_add(1),
                )
                .len();
            if existing_count == removed {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub async fn zset_remove_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        self.zset_remove_async_unlocked(key, members).await
    }

    pub(in crate::store::db) async fn zset_remove_async_unlocked(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((expire_ms, version)) = meta else {
            return Ok(0);
        };

        let mut batch = WriteBatch::new();
        let mut removed = 0usize;
        let mut seen_members = std::collections::HashSet::new();
        let unique_members = members
            .iter()
            .filter(|member| seen_members.insert(member.as_str()))
            .collect::<Vec<_>>();
        let member_keys = unique_members
            .iter()
            .map(|member| zset_member_key(self.db_index, key, version, member))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&member_keys).await;
        for ((member, member_key), old_value) in
            unique_members.into_iter().zip(member_keys).zip(old_values)
        {
            let Some(score) = old_value.as_deref().and_then(decode_zset_score) else {
                continue;
            };

            batch.delete(&member_key);
            batch.delete(&zset_rank_key(self.db_index, key, version, score, member));
            removed += 1;
        }

        if removed > 0 {
            let prefix = zset_member_prefix(self.db_index, key, version);
            let existing_count = self
                .store
                .scan_range_raw_limited_async(
                    &prefix,
                    prefix_exclusive_upper_bound(&prefix),
                    removed.saturating_add(1),
                )
                .await
                .len();
            if existing_count == removed {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    /// 按浮点增量更新 sorted set member，返回更新后的 score。
    pub fn zset_increment_by(&self, key: &str, member: &str, increment: f64) -> Result<f64, Error> {
        if increment.is_nan() {
            return Err(Error::msg("ERR value is not a valid float"));
        }
        let current = self.zset_score(key, member)?.unwrap_or(0.0);
        let next = current + increment;
        if next.is_nan() {
            return Err(Error::msg("ERR resulting score is not a number (NaN)"));
        }
        self.zset_add(key, &[(next, member.to_string())])?;
        Ok(next)
    }

    pub async fn zset_increment_by_async(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> Result<f64, Error> {
        if increment.is_nan() {
            return Err(Error::msg("ERR value is not a valid float"));
        }
        if self.store.is_transactional() {
            return self
                .zset_increment_by_strict_async(key, member, increment)
                .await;
        }

        self.enqueue_zset_increment(key, member, increment)
            .await
            .map_err(|_| Error::msg("ERR sorted set increment merger stopped"))?
    }

    /// Let same-member commands from one pipeline join the same cross-connection commit. Mixed
    /// pipelines retain the existing distinct-member batch path.
    pub(crate) async fn zset_increment_many_merged_async(
        &self,
        commands: &[(&str, f64, &str)],
    ) -> Vec<Result<f64, Error>> {
        let Some((first_key, _, first_member)) = commands.first().copied() else {
            return Vec::new();
        };
        if self.store.is_transactional()
            || commands.iter().any(|(key, increment, member)| {
                *key != first_key || *member != first_member || increment.is_nan()
            })
        {
            return self.zset_increment_batch_async(commands).await;
        }

        let mut receivers = Vec::with_capacity(commands.len());
        for (_, increment, _) in commands {
            receivers.push(self.enqueue_zset_increment(first_key, first_member, *increment));
        }
        let mut replies = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            replies.push(
                receiver
                    .await
                    .unwrap_or_else(|_| Err(Error::msg("ERR sorted set increment merger stopped"))),
            );
        }
        replies
    }

    fn enqueue_zset_increment(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> tokio::sync::oneshot::Receiver<Result<f64, Error>> {
        let queue_key = (
            self.db_index,
            key.as_bytes().to_vec(),
            member.as_bytes().to_vec(),
        );
        let queue = self
            .counter_cache
            .zset_increment_queues
            .entry(queue_key)
            .or_insert_with(|| Arc::new(ZsetIncrementMergeQueue::default()))
            .clone();
        let (reply, result) = tokio::sync::oneshot::channel();
        queue
            .pending
            .lock()
            .expect("zset increment merge queue mutex poisoned")
            .push_back(ZsetIncrementMergeRequest { increment, reply });
        if queue
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.spawn_zset_increment_merge_worker(
                key.to_string(),
                member.to_string(),
                queue.clone(),
            );
        }
        drop(queue);
        result
    }

    async fn zset_increment_by_strict_async(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> Result<f64, Error> {
        let outcome = self
            .zset_add_with_options_async(
                key,
                &[(increment, member.to_string())],
                ZsetAddOptions {
                    increment: true,
                    ..ZsetAddOptions::default()
                },
            )
            .await?;
        outcome
            .score
            .ok_or_else(|| Error::msg("ERR sorted set increment was not applied"))
    }

    fn spawn_zset_increment_merge_worker(
        &self,
        key: String,
        member: String,
        queue: Arc<ZsetIncrementMergeQueue>,
    ) {
        let db = self.shared_task_view();
        let queue_key = (
            self.db_index,
            key.as_bytes().to_vec(),
            member.as_bytes().to_vec(),
        );
        tokio::spawn(async move {
            loop {
                // Give other already-runnable clients one scheduler turn to join this commit.
                tokio::task::yield_now().await;
                let requests = {
                    let mut pending = queue
                        .pending
                        .lock()
                        .expect("zset increment merge queue mutex poisoned");
                    let count = pending.len().min(ZSET_INCREMENT_MERGE_BATCH_MAX);
                    pending.drain(..count).collect::<Vec<_>>()
                };
                if !requests.is_empty() {
                    let commands = requests
                        .iter()
                        .map(|request| (key.as_str(), request.increment, member.as_str()))
                        .collect::<Vec<_>>();
                    let replies = db.zset_increment_batch_async(&commands).await;
                    for (request, reply) in requests.into_iter().zip(replies) {
                        let _ = request.reply.send(reply);
                    }
                }

                let pending = queue
                    .pending
                    .lock()
                    .expect("zset increment merge queue mutex poisoned");
                if pending.is_empty() {
                    // Producers push while holding this mutex and only then inspect `running`, so
                    // publishing the idle state under the same mutex cannot strand a request.
                    queue.running.store(false, Ordering::Release);
                    db.counter_cache
                        .zset_increment_queues
                        .remove_if(&queue_key, |_, existing| {
                            Arc::ptr_eq(existing, &queue) && Arc::strong_count(existing) == 2
                        });
                    break;
                }
                drop(pending);
            }
        });
    }
}

fn zset_add_condition_matches(
    previous_score: Option<f64>,
    score: f64,
    options: ZsetAddOptions,
) -> bool {
    match previous_score {
        Some(previous) => {
            !options.nx && (!options.gt || score > previous) && (!options.lt || score < previous)
        }
        None => !options.xx,
    }
}
