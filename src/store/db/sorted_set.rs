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
    pub fn zset_score(&self, key: &str, member: &str) -> Result<Option<f64>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        Ok(self
            .store
            .get_raw(&zset_member_key(self.db_index, key, version, member))
            .and_then(|value| decode_zset_score(&value)))
    }

    pub async fn zset_score_async(&self, key: &str, member: &str) -> Result<Option<f64>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        Ok(self
            .store
            .get_raw_async(&zset_member_key(self.db_index, key, version, member))
            .await
            .and_then(|value| decode_zset_score(&value)))
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
        let current = self.zset_score_async(key, member).await?.unwrap_or(0.0);
        let next = current + increment;
        if next.is_nan() {
            return Err(Error::msg("ERR resulting score is not a number (NaN)"));
        }
        self.zset_add_async(key, &[(next, member.to_string())])
            .await?;
        Ok(next)
    }

    /// 返回 zset 基数。
    pub fn zset_card(&self, key: &str) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.zset_members_raw(key, version).len())
    }

    pub async fn zset_card_async(&self, key: &str) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.zset_members_raw_async(key, version).await.len())
    }

    /// 返回 member 的 rank，按 score 升序、member 字典序。
    pub fn zset_rank(&self, key: &str, member: &str) -> Result<Option<usize>, Error> {
        let Some(score) = self.zset_score(key, member)? else {
            return Ok(None);
        };

        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        let rank_key = zset_rank_key(self.db_index, key, version, score, member);
        for (index, (candidate_key, _)) in self
            .zset_rank_entries_raw(key, version)
            .into_iter()
            .enumerate()
        {
            if candidate_key == rank_key {
                return Ok(Some(index));
            }
        }

        Ok(None)
    }

    pub async fn zset_rank_async(&self, key: &str, member: &str) -> Result<Option<usize>, Error> {
        let Some(score) = self.zset_score_async(key, member).await? else {
            return Ok(None);
        };

        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        let rank_key = zset_rank_key(self.db_index, key, version, score, member);
        for (index, (candidate_key, _)) in self
            .zset_rank_entries_raw_async(key, version)
            .await
            .into_iter()
            .enumerate()
        {
            if candidate_key == rank_key {
                return Ok(Some(index));
            }
        }

        Ok(None)
    }

    pub fn zset_rev_rank(&self, key: &str, member: &str) -> Result<Option<usize>, Error> {
        let Some(rank) = self.zset_rank(key, member)? else {
            return Ok(None);
        };
        let len = self.zset_card(key)?;
        Ok(Some(len.saturating_sub(rank + 1)))
    }

    pub async fn zset_rev_rank_async(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<usize>, Error> {
        let Some(rank) = self.zset_rank_async(key, member).await? else {
            return Ok(None);
        };
        let len = self.zset_card_async(key).await?;
        Ok(Some(len.saturating_sub(rank + 1)))
    }

    /// 统计 score 落在区间内的成员数量。
    pub fn zset_count(&self, key: &str, min: f64, max: f64) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self
            .zset_rank_entries_raw(key, version)
            .into_iter()
            .filter_map(|(rank_key, _)| self.decode_rank_score(key, version, &rank_key))
            .filter(|score| *score >= min && *score <= max)
            .count())
    }

    pub async fn zset_count_async(&self, key: &str, min: f64, max: f64) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self
            .zset_rank_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(rank_key, _)| self.decode_rank_score(key, version, &rank_key))
            .filter(|score| *score >= min && *score <= max)
            .count())
    }

    /// 按 rank 范围返回成员和分数。
    pub fn zset_range(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        reverse: bool,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        let mut entries = self.zset_ranked_members(key, version);
        if reverse {
            entries.reverse();
        }

        let len = entries.len() as i64;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        normalized_start = normalized_start.max(0);
        normalized_stop = normalized_stop.min(len - 1);

        if normalized_start > normalized_stop || normalized_start >= len || normalized_stop < 0 {
            return Ok(Vec::new());
        }

        Ok(entries[normalized_start as usize..=normalized_stop as usize].to_vec())
    }

    pub async fn zset_range_async(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        reverse: bool,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        let mut entries = self.zset_ranked_members_async(key, version).await;
        if reverse {
            entries.reverse();
        }

        let len = entries.len() as i64;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        normalized_start = normalized_start.max(0);
        normalized_stop = normalized_stop.min(len - 1);

        if normalized_start > normalized_stop || normalized_start >= len || normalized_stop < 0 {
            return Ok(Vec::new());
        }

        Ok(entries[normalized_start as usize..=normalized_stop as usize].to_vec())
    }

    /// 按 score 区间返回成员和分数。
    pub fn zset_range_by_score(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .zset_ranked_members(key, version)
            .into_iter()
            .filter(|(_, score)| *score >= min && *score <= max)
            .collect())
    }

    pub async fn zset_range_by_score_async(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .zset_ranked_members_async(key, version)
            .await
            .into_iter()
            .filter(|(_, score)| *score >= min && *score <= max)
            .collect())
    }

    pub(crate) fn zset_range_by_lex(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .read_zset_members(key, version)
            .into_iter()
            .filter(|(member, _)| {
                crate::cmds::sorted_set::zrange::lex_member_in_range(member, min, max)
            })
            .collect())
    }

    pub(crate) async fn zset_range_by_lex_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .zset_members_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(member, value)| {
                match (String::from_utf8(member), decode_zset_score(&value)) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .filter(|(member, _)| {
                crate::cmds::sorted_set::zrange::lex_member_in_range(member, min, max)
            })
            .collect())
    }

    pub fn zset_remove_range_by_rank(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range(key, start, stop, false)?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove(key, &members)
    }

    pub async fn zset_remove_range_by_rank_async(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_async(key, start, stop, false)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove_async(key, &members).await
    }

    pub fn zset_remove_range_by_score(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_by_score(key, min, max)?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove(key, &members)
    }

    pub async fn zset_remove_range_by_score_async(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_by_score_async(key, min, max)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove_async(key, &members).await
    }

    pub fn zset_store_entries(
        &self,
        destination: &str,
        entries: Vec<(String, f64)>,
    ) -> Result<usize, Error> {
        let len = entries.len();
        if len == 0 {
            self.delete_key(destination);
            return Ok(0);
        }
        let set = entries.into_iter().collect::<BTreeMap<_, _>>();
        self.insert(destination.to_string(), Structure::SortedSet(set));
        Ok(len)
    }

    pub async fn zset_store_entries_async(
        &self,
        destination: &str,
        entries: Vec<(String, f64)>,
    ) -> Result<usize, Error> {
        let len = entries.len();
        if len == 0 {
            self.delete_key_async(destination).await;
            return Ok(0);
        }
        self.delete_key_async(destination).await;
        let members = entries
            .into_iter()
            .map(|(member, score)| (score, member))
            .collect::<Vec<_>>();
        self.zset_add_async(destination, &members).await?;
        Ok(len)
    }

    pub fn zset_all_entries(&self, key: &str) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };
        Ok(self.zset_ranked_members(key, version))
    }

    pub async fn zset_all_entries_async(&self, key: &str) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };
        Ok(self.zset_ranked_members_async(key, version).await)
    }

    pub fn zset_random_members(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        let mut entries = self.zset_all_entries(key)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            return Ok(Some(vec![entries[0].clone()]));
        };
        if count >= 0 {
            entries.truncate((count as usize).min(entries.len()));
            return Ok(Some(entries));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for idx in 0..requested {
            result.push(entries[idx % entries.len()].clone());
        }
        Ok(Some(result))
    }

    pub async fn zset_random_members_async(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        let mut entries = self.zset_all_entries_async(key).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            return Ok(Some(vec![entries[0].clone()]));
        };
        if count >= 0 {
            entries.truncate((count as usize).min(entries.len()));
            return Ok(Some(entries));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for idx in 0..requested {
            result.push(entries[idx % entries.len()].clone());
        }
        Ok(Some(result))
    }

    pub fn zset_rev_range_by_score(
        &self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_range_by_score(key, min, max)?;
        entries.reverse();
        Ok(entries)
    }

    pub async fn zset_rev_range_by_score_async(
        &self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_range_by_score_async(key, min, max).await?;
        entries.reverse();
        Ok(entries)
    }

    pub fn zset_diff(&self, keys: &[String]) -> Result<Vec<(String, f64)>, Error> {
        let Some(first) = keys.first() else {
            return Ok(Vec::new());
        };
        let mut result = self
            .zset_all_entries(first)?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for key in &keys[1..] {
            for (member, _) in self.zset_all_entries(key)? {
                result.remove(&member);
            }
        }
        Ok(result.into_iter().collect())
    }

    pub async fn zset_diff_async(&self, keys: &[String]) -> Result<Vec<(String, f64)>, Error> {
        let Some(first) = keys.first() else {
            return Ok(Vec::new());
        };
        let mut result = self
            .zset_all_entries_async(first)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for key in &keys[1..] {
            for (member, _) in self.zset_all_entries_async(key).await? {
                result.remove(&member);
            }
        }
        Ok(result.into_iter().collect())
    }

    pub fn zset_union_or_inter(
        &self,
        keys: &[String],
        weights: &[f64],
        aggregate: ZsetAggregate,
        intersection: bool,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut scores: BTreeMap<String, f64> = BTreeMap::new();
        let mut seen_counts: BTreeMap<String, usize> = BTreeMap::new();
        for (index, key) in keys.iter().enumerate() {
            let weight = weights.get(index).copied().unwrap_or(1.0);
            let entries = self.zset_all_entries(key)?;
            let mut seen_in_key = HashSet::new();
            for (member, score) in entries {
                let weighted = score * weight;
                scores
                    .entry(member.clone())
                    .and_modify(|current| {
                        *current = match aggregate {
                            ZsetAggregate::Sum => *current + weighted,
                            ZsetAggregate::Min => current.min(weighted),
                            ZsetAggregate::Max => current.max(weighted),
                        }
                    })
                    .or_insert(weighted);
                if seen_in_key.insert(member.clone()) {
                    *seen_counts.entry(member).or_default() += 1;
                }
            }
        }
        if intersection {
            let required = keys.len();
            scores.retain(|member, _| seen_counts.get(member).copied().unwrap_or(0) == required);
        }
        let mut entries = scores.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(member_a, score_a), (member_b, score_b)| {
            score_a
                .partial_cmp(score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| member_a.cmp(member_b))
        });
        Ok(entries)
    }

    pub async fn zset_union_or_inter_async(
        &self,
        keys: &[String],
        weights: &[f64],
        aggregate: ZsetAggregate,
        intersection: bool,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut scores: BTreeMap<String, f64> = BTreeMap::new();
        let mut seen_counts: BTreeMap<String, usize> = BTreeMap::new();
        for (index, key) in keys.iter().enumerate() {
            let weight = weights.get(index).copied().unwrap_or(1.0);
            let entries = self.zset_all_entries_async(key).await?;
            let mut seen_in_key = HashSet::new();
            for (member, score) in entries {
                let weighted = score * weight;
                scores
                    .entry(member.clone())
                    .and_modify(|current| {
                        *current = match aggregate {
                            ZsetAggregate::Sum => *current + weighted,
                            ZsetAggregate::Min => current.min(weighted),
                            ZsetAggregate::Max => current.max(weighted),
                        }
                    })
                    .or_insert(weighted);
                if seen_in_key.insert(member.clone()) {
                    *seen_counts.entry(member).or_default() += 1;
                }
            }
        }
        if intersection {
            let required = keys.len();
            scores.retain(|member, _| seen_counts.get(member).copied().unwrap_or(0) == required);
        }
        let mut entries = scores.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(member_a, score_a), (member_b, score_b)| {
            score_a
                .partial_cmp(score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| member_a.cmp(member_b))
        });
        Ok(entries)
    }

    pub fn zset_pop(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_all_entries(key)?;
        if !min {
            entries.reverse();
        }
        entries.truncate(count);
        let members = entries
            .iter()
            .map(|(member, _)| member.clone())
            .collect::<Vec<_>>();
        self.zset_remove(key, &members)?;
        Ok(entries)
    }

    pub async fn zset_pop_async(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_all_entries_async(key).await?;
        if !min {
            entries.reverse();
        }
        entries.truncate(count);
        let members = entries
            .iter()
            .map(|(member, _)| member.clone())
            .collect::<Vec<_>>();
        self.zset_remove_async(key, &members).await?;
        Ok(entries)
    }

    pub fn zset_multi_pop(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(String, f64)>)>, Error> {
        for key in keys {
            if self.zset_card(key)? == 0 {
                continue;
            }
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
    ) -> Result<Option<(String, Vec<(String, f64)>)>, Error> {
        for key in keys {
            if self.zset_card_async(key).await? == 0 {
                continue;
            }
            let entries = self.zset_pop_async(key, min, count).await?;
            if !entries.is_empty() {
                return Ok(Some((key.clone(), entries)));
            }
        }
        Ok(None)
    }

    pub(crate) fn zset_lex_count(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        Ok(self.zset_range_by_lex(key, min, max)?.len())
    }

    pub(crate) async fn zset_lex_count_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        Ok(self.zset_range_by_lex_async(key, min, max).await?.len())
    }

    pub(crate) fn zset_remove_range_by_lex(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let members = self
            .zset_range_by_lex(key, min, max)?
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>();
        self.zset_remove(key, &members)
    }

    pub(crate) async fn zset_remove_range_by_lex_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let members = self
            .zset_range_by_lex_async(key, min, max)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>();
        self.zset_remove_async(key, &members).await
    }

    /// 分页扫描 zset members，返回下一个游标和成员/分数。
    pub fn zset_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok((0, Vec::new()));
        };

        let mut entries = self.zset_ranked_members(key, version);
        if pattern_str != "*" {
            entries.retain(|(member, _)| pattern::is_match(member, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
        let items = if start_index < entries.len() {
            entries[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= entries.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }

    pub async fn zset_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok((0, Vec::new()));
        };

        let mut entries = self.zset_ranked_members_async(key, version).await;
        if pattern_str != "*" {
            entries.retain(|(member, _)| pattern::is_match(member, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
        let items = if start_index < entries.len() {
            entries[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= entries.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }


}
