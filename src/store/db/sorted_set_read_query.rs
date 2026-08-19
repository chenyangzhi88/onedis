use super::*;

impl Db {
    pub(crate) async fn zset_card_batch_async(
        &self,
        command_keys: &[&str],
    ) -> Vec<Result<usize, Error>> {
        let mut key_positions = HashMap::with_capacity(command_keys.len());
        let mut keys = Vec::new();
        for key in command_keys {
            if !key_positions.contains_key(key) {
                key_positions.insert(*key, keys.len());
                keys.push(*key);
            }
        }
        let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let metas = self.store.multi_get_raw_async(&meta_keys).await;
        let now = now_ms();
        let mut lengths = Vec::with_capacity(keys.len());
        for (key, raw) in keys.iter().zip(metas) {
            let result = match raw {
                None => Ok(0),
                Some(raw) => match decode_meta_header(&raw) {
                    None => Err("Failed to decode sorted set metadata".to_string()),
                    Some(header) if header.expire_ms > 0 && now >= header.expire_ms => Ok(0),
                    Some(header) if header.type_tag != TYPE_SORTED_SET => {
                        Err(WRONG_TYPE_ERROR.to_string())
                    }
                    Some(header) => {
                        if let Some(entries) = decode_packed_zset(&raw) {
                            lengths.push(Ok(entries.len()));
                            continue;
                        }
                        let logical_key = key.as_bytes().to_vec();
                        self.counter_cache
                            .zset_ever_populated
                            .store(true, Ordering::Release);
                        let key_epoch = self
                            .counter_cache
                            .zset_key_epoch(self.db_index, &logical_key);
                        let db_epoch = self.counter_cache.zset_db_epoch(self.db_index);
                        let cache_key = (self.db_index, logical_key);
                        if let Some(cached) = self.counter_cache.zset_lengths.get(&cache_key)
                            && cached.version == header.version
                            && cached.key_epoch == key_epoch
                            && cached.db_epoch == db_epoch
                        {
                            Ok(cached.len)
                        } else {
                            let prefix = zset_member_prefix(self.db_index, key, header.version);
                            let len = self
                                .store
                                .count_range_raw_keys_async(
                                    &prefix,
                                    prefix_exclusive_upper_bound(&prefix),
                                )
                                .await;
                            if self
                                .counter_cache
                                .zset_key_epoch(self.db_index, key.as_bytes())
                                == key_epoch
                                && self.counter_cache.zset_db_epoch(self.db_index) == db_epoch
                            {
                                self.counter_cache.evict_zset_if_full();
                                self.counter_cache.zset_lengths.insert(
                                    cache_key,
                                    ZsetLenCacheEntry {
                                        len,
                                        version: header.version,
                                        key_epoch,
                                        db_epoch,
                                    },
                                );
                            }
                            Ok(len)
                        }
                    }
                },
            };
            lengths.push(result);
        }
        command_keys
            .iter()
            .map(|key| match &lengths[key_positions[key]] {
                Ok(len) => Ok(*len),
                Err(message) => Err(Error::msg(message.clone())),
            })
            .collect()
    }

    pub fn zset_score(&self, key: &str, member: &str) -> Result<Option<f64>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        if version == 0 {
            return Ok(self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_zset)
                .and_then(|entries| entries.get(member).copied()));
        }

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

        if version == 0 {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .and_then(|entries| entries.get(member).copied()));
        }

        Ok(self
            .store
            .get_raw_async(&zset_member_key(self.db_index, key, version, member))
            .await
            .and_then(|value| decode_zset_score(&value)))
    }

    /// Read several member scores with one metadata lookup and one storage multi-get.
    pub async fn zset_multi_score_async(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<Vec<Option<f64>>, Error> {
        let Some((_, version)) = self.zset_expire_ms_async(key).await? else {
            return Ok(vec![None; members.len()]);
        };
        if version == 0 {
            let entries = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .ok_or_else(|| Error::msg("Failed to decode packed sorted set"))?;
            return Ok(members
                .iter()
                .map(|member| entries.get(member).copied())
                .collect());
        }
        let member_keys = members
            .iter()
            .map(|member| zset_member_key(self.db_index, key, version, member))
            .collect::<Vec<_>>();
        Ok(self
            .store
            .multi_get_raw_async(&member_keys)
            .await
            .into_iter()
            .map(|value| value.and_then(|value| decode_zset_score(&value)))
            .collect())
    }

    /// Batch several ZMSCORE commands across a client pipeline with two storage reads:
    /// one for all metadata and one for all requested members.
    pub(crate) async fn zset_multi_score_batch_async(
        &self,
        commands: &[(&str, Vec<String>)],
    ) -> Vec<Result<Vec<Option<f64>>, Error>> {
        let meta_keys = commands
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let metas = self.store.multi_get_raw_async(&meta_keys).await;
        let now = now_ms();
        let mut member_keys = Vec::new();
        let mut plans = Vec::with_capacity(commands.len());
        for ((key, members), raw) in commands.iter().zip(metas) {
            let Some(raw) = raw else {
                plans.push(ZsetMultiScorePlan::Missing(members.len()));
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                plans.push(ZsetMultiScorePlan::Error(
                    "Failed to decode sorted set metadata".to_string(),
                ));
                continue;
            };
            if header.expire_ms > 0 && now >= header.expire_ms {
                plans.push(ZsetMultiScorePlan::Missing(members.len()));
                continue;
            }
            if header.type_tag != TYPE_SORTED_SET {
                plans.push(ZsetMultiScorePlan::Error(WRONG_TYPE_ERROR.to_string()));
                continue;
            }
            if let Some(entries) = decode_packed_zset(&raw) {
                plans.push(ZsetMultiScorePlan::Packed(
                    members
                        .iter()
                        .map(|member| entries.get(member).copied())
                        .collect(),
                ));
                continue;
            }
            let lookup = member_keys.len();
            member_keys.extend(
                members
                    .iter()
                    .map(|member| zset_member_key(self.db_index, key, header.version, member)),
            );
            plans.push(ZsetMultiScorePlan::Members {
                lookup,
                count: members.len(),
            });
        }
        let values = self.store.multi_get_raw_async(&member_keys).await;
        plans
            .into_iter()
            .map(|plan| match plan {
                ZsetMultiScorePlan::Missing(count) => Ok(vec![None; count]),
                ZsetMultiScorePlan::Error(message) => Err(Error::msg(message)),
                ZsetMultiScorePlan::Packed(values) => Ok(values),
                ZsetMultiScorePlan::Members { lookup, count } => Ok(values
                    [lookup..lookup.saturating_add(count)]
                    .iter()
                    .map(|value| value.as_deref().and_then(decode_zset_score))
                    .collect()),
            })
            .collect()
    }

    /// 返回 zset 基数。
    pub fn zset_card(&self, key: &str) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        if version == 0 {
            return Ok(self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_zset)
                .map_or(0, |entries| entries.len()));
        }

        if self.store.is_transactional() {
            let prefix = zset_member_prefix(self.db_index, key, version);
            return Ok(self
                .store
                .count_range_raw_keys(&prefix, prefix_exclusive_upper_bound(&prefix)));
        }
        let logical_key = key.as_bytes().to_vec();
        self.counter_cache
            .zset_ever_populated
            .store(true, Ordering::Release);
        let key_epoch = self
            .counter_cache
            .zset_key_epoch(self.db_index, &logical_key);
        let db_epoch = self.counter_cache.zset_db_epoch(self.db_index);
        let cache_key = (self.db_index, logical_key);
        if let Some(cached) = self.counter_cache.zset_lengths.get(&cache_key)
            && cached.version == version
            && cached.key_epoch == key_epoch
            && cached.db_epoch == db_epoch
        {
            return Ok(cached.len);
        }
        let prefix = zset_member_prefix(self.db_index, key, version);
        let len = self
            .store
            .count_range_raw_keys(&prefix, prefix_exclusive_upper_bound(&prefix));
        if self
            .counter_cache
            .zset_key_epoch(self.db_index, key.as_bytes())
            == key_epoch
            && self.counter_cache.zset_db_epoch(self.db_index) == db_epoch
        {
            self.counter_cache.evict_zset_if_full();
            self.counter_cache.zset_lengths.insert(
                cache_key,
                ZsetLenCacheEntry {
                    len,
                    version,
                    key_epoch,
                    db_epoch,
                },
            );
        }
        Ok(len)
    }

    pub async fn zset_card_async(&self, key: &str) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        if version == 0 {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .map_or(0, |entries| entries.len()));
        }

        if self.store.is_transactional() {
            let prefix = zset_member_prefix(self.db_index, key, version);
            return Ok(self
                .store
                .count_range_raw_keys_async(&prefix, prefix_exclusive_upper_bound(&prefix))
                .await);
        }
        let logical_key = key.as_bytes().to_vec();
        self.counter_cache
            .zset_ever_populated
            .store(true, Ordering::Release);
        let key_epoch = self
            .counter_cache
            .zset_key_epoch(self.db_index, &logical_key);
        let db_epoch = self.counter_cache.zset_db_epoch(self.db_index);
        let cache_key = (self.db_index, logical_key);
        if let Some(cached) = self.counter_cache.zset_lengths.get(&cache_key)
            && cached.version == version
            && cached.key_epoch == key_epoch
            && cached.db_epoch == db_epoch
        {
            return Ok(cached.len);
        }
        let prefix = zset_member_prefix(self.db_index, key, version);
        let len = self
            .store
            .count_range_raw_keys_async(&prefix, prefix_exclusive_upper_bound(&prefix))
            .await;
        if self
            .counter_cache
            .zset_key_epoch(self.db_index, key.as_bytes())
            == key_epoch
            && self.counter_cache.zset_db_epoch(self.db_index) == db_epoch
        {
            self.counter_cache.evict_zset_if_full();
            self.counter_cache.zset_lengths.insert(
                cache_key,
                ZsetLenCacheEntry {
                    len,
                    version,
                    key_epoch,
                    db_epoch,
                },
            );
        }
        Ok(len)
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

        if version == 0 {
            return Ok(self
                .zset_ranked_members(key, version)
                .iter()
                .position(|(candidate, _)| candidate == member));
        }

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let rank_key = zset_rank_key(self.db_index, key, version, score, member);
        Ok(Some(
            self.store.count_range_raw_keys(&prefix, Some(rank_key)),
        ))
    }

    pub async fn zset_rank_async(&self, key: &str, member: &str) -> Result<Option<usize>, Error> {
        let Some(score) = self.zset_score_async(key, member).await? else {
            return Ok(None);
        };

        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(None);
        };

        if version == 0 {
            return Ok(self
                .zset_ranked_members_async(key, version)
                .await
                .iter()
                .position(|(candidate, _)| candidate == member));
        }

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let rank_key = zset_rank_key(self.db_index, key, version, score, member);
        Ok(Some(
            self.store
                .count_range_raw_keys_async(&prefix, Some(rank_key))
                .await,
        ))
    }

    pub fn zset_rev_rank(&self, key: &str, member: &str) -> Result<Option<usize>, Error> {
        let Some(score) = self.zset_score(key, member)? else {
            return Ok(None);
        };
        let Some((_, version)) = self.zset_expire_ms(key)? else {
            return Ok(None);
        };
        if version == 0 {
            return Ok(self
                .zset_ranked_members(key, version)
                .iter()
                .rev()
                .position(|(candidate, _)| candidate == member));
        }
        let mut lower = zset_rank_key(self.db_index, key, version, score, member);
        lower.push(0);
        let prefix = zset_rank_prefix(self.db_index, key, version);
        Ok(Some(self.store.count_range_raw_keys(
            &lower,
            prefix_exclusive_upper_bound(&prefix),
        )))
    }

    pub async fn zset_rev_rank_async(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<usize>, Error> {
        let Some(score) = self.zset_score_async(key, member).await? else {
            return Ok(None);
        };
        let Some((_, version)) = self.zset_expire_ms_async(key).await? else {
            return Ok(None);
        };
        if version == 0 {
            return Ok(self
                .zset_ranked_members_async(key, version)
                .await
                .iter()
                .rev()
                .position(|(candidate, _)| candidate == member));
        }
        let mut lower = zset_rank_key(self.db_index, key, version, score, member);
        lower.push(0);
        let prefix = zset_rank_prefix(self.db_index, key, version);
        Ok(Some(
            self.store
                .count_range_raw_keys_async(&lower, prefix_exclusive_upper_bound(&prefix))
                .await,
        ))
    }

    /// 统计 score 落在区间内的成员数量。
    pub fn zset_count(&self, key: &str, min: f64, max: f64) -> Result<usize, Error> {
        self.zset_count_bounded(key, min, true, max, true)
    }

    pub async fn zset_count_async(&self, key: &str, min: f64, max: f64) -> Result<usize, Error> {
        self.zset_count_bounded_async(key, min, true, max, true)
            .await
    }

    pub(crate) fn zset_count_bounded(
        &self,
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };
        if version == 0 {
            return Ok(self
                .zset_ranked_members(key, version)
                .into_iter()
                .filter(|(_, score)| {
                    (*score > min || min_inclusive && *score == min)
                        && (*score < max || max_inclusive && *score == max)
                })
                .count());
        }
        let Some((lower, upper)) = zset_score_scan_bounds(
            self.db_index,
            key,
            version,
            min,
            min_inclusive,
            max,
            max_inclusive,
        ) else {
            return Ok(0);
        };
        Ok(self.store.count_range_raw_keys(&lower, upper))
    }

    pub(crate) async fn zset_count_bounded_async(
        &self,
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };
        if version == 0 {
            return Ok(self
                .zset_ranked_members_async(key, version)
                .await
                .into_iter()
                .filter(|(_, score)| {
                    (*score > min || min_inclusive && *score == min)
                        && (*score < max || max_inclusive && *score == max)
                })
                .count());
        }
        let Some((lower, upper)) = zset_score_scan_bounds(
            self.db_index,
            key,
            version,
            min,
            min_inclusive,
            max,
            max_inclusive,
        ) else {
            return Ok(0);
        };
        Ok(self.store.count_range_raw_keys_async(&lower, upper).await)
    }

    pub fn zset_intersection_card(&self, keys: &[String], limit: usize) -> Result<usize, Error> {
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zintercard' command",
            ));
        }
        let mut sets = Vec::with_capacity(keys.len());
        for key in keys {
            let entries = self.zset_all_entries(key)?;
            if entries.is_empty() && self.zset_expire_ms(key)?.is_none() {
                return Ok(0);
            }
            sets.push(
                entries
                    .into_iter()
                    .map(|(member, _)| member)
                    .collect::<HashSet<_>>(),
            );
        }
        let Some((smallest_index, smallest)) =
            sets.iter().enumerate().min_by_key(|(_, set)| set.len())
        else {
            return Ok(0);
        };
        let mut count = 0usize;
        for member in smallest {
            if sets
                .iter()
                .enumerate()
                .all(|(index, set)| index == smallest_index || set.contains(member))
            {
                count = count.saturating_add(1);
                if limit > 0 && count >= limit {
                    break;
                }
            }
        }
        Ok(count)
    }

    pub async fn zset_intersection_card_async(
        &self,
        keys: &[String],
        limit: usize,
    ) -> Result<usize, Error> {
        let keys = keys.to_vec();
        self.run_blocking_store_task(move |db| db.zset_intersection_card(&keys, limit))
            .await
    }
}

enum ZsetMultiScorePlan {
    Missing(usize),
    Error(String),
    Packed(Vec<Option<f64>>),
    Members { lookup: usize, count: usize },
}
