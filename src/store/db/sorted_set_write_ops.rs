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

        for (score, member) in members {
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

        for (score, member) in members {
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
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        let existing_count = self.zset_members_raw(key, version).len();
        let mut batch = WriteBatch::new();
        let mut removed = 0usize;
        for member in members {
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
            batch.delete(&self.mk(key));
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub async fn zset_remove_async(&self, key: &str, members: &[String]) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        let existing_count = self.zset_members_raw_async(key, version).await.len();
        let mut batch = WriteBatch::new();
        let mut removed = 0usize;
        for member in members {
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
            batch.delete(&self.mk(key));
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    /// 返回 member 的 score。

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
        let current = self.zset_score_async(key, member).await?.unwrap_or(0.0);
        let next = current + increment;
        if next.is_nan() {
            return Err(Error::msg("ERR resulting score is not a number (NaN)"));
        }
        self.zset_add_async(key, &[(next, member.to_string())])
            .await?;
        Ok(next)
    }
}
