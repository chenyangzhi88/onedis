use super::*;

impl Db {
    pub fn zset_add(&self, key: &str, members: &[(f64, String)]) -> Result<usize, Error> {
        let exists = self.zset_expire_ms(key)?;
        let version = match exists {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
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
        let _write_guard = self.set_write_lock(key).lock().await;
        self.zset_add_async_unlocked(key, members).await
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
            None => self.next_persisted_version(),
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
        let _write_guard = self.set_write_lock(key).lock().await;
        let exists = self.zset_expire_ms_async(key).await?;
        let version = match exists {
            Some((_, version)) => version,
            None => self.next_persisted_version_async().await,
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
                .get_raw_async(&member_key)
                .await
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
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(outcome)
    }

    pub(in crate::store::db) async fn zset_add_async_unlocked(
        &self,
        key: &str,
        members: &[(f64, String)],
    ) -> Result<usize, Error> {
        let exists = self.zset_expire_ms_async(key).await?;
        let version = match exists {
            Some((_, v)) => v,
            None => self.next_persisted_version_async().await,
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

        let existing_count = self.zset_members_raw(key, version).len();
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

        if removed > 0 && existing_count == removed {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
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

        let existing_count = self.zset_members_raw_async(key, version).await.len();
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
                .get_raw_async(&member_key)
                .await
                .and_then(|value| decode_zset_score(&value))
            else {
                continue;
            };

            batch.delete(&member_key);
            batch.delete(&zset_rank_key(self.db_index, key, version, score, member));
            removed += 1;
        }

        if removed > 0 && existing_count == removed {
            self.delete_main_key_with_ttl_to_batch(&mut batch, key, expire_ms);
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
        let _write_guard = self.set_write_lock(key).lock().await;
        let current = self.zset_score_async(key, member).await?.unwrap_or(0.0);
        let next = current + increment;
        if next.is_nan() {
            return Err(Error::msg("ERR resulting score is not a number (NaN)"));
        }
        self.zset_add_async_unlocked(key, &[(next, member.to_string())])
            .await?;
        Ok(next)
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
