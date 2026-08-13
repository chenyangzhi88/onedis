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

    /// Read several member scores with one metadata lookup and one storage multi-get.
    pub async fn zset_multi_score_async(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<Vec<Option<f64>>, Error> {
        let Some((_, version)) = self.zset_expire_ms_async(key).await? else {
            return Ok(vec![None; members.len()]);
        };
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
        let mut versions = Vec::with_capacity(keys.len());
        for key in keys {
            versions.push(self.zset_expire_ms(key)?.map(|(_, version)| version));
        }
        let Some(versions) = versions.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(0);
        };

        let mut smallest = 0usize;
        let mut smallest_len = usize::MAX;
        for (idx, (key, version)) in keys.iter().zip(&versions).enumerate() {
            let prefix = zset_member_prefix(self.db_index, key, *version);
            let len = self.store.scan_range_raw_visit(
                &prefix,
                prefix_exclusive_upper_bound(&prefix),
                usize::MAX,
                |_, _| true,
            );
            if len < smallest_len {
                smallest = idx;
                smallest_len = len;
            }
        }
        if smallest_len == 0 {
            return Ok(0);
        }

        let prefix = zset_member_prefix(self.db_index, &keys[smallest], versions[smallest]);
        let mut count = 0usize;
        self.store.scan_range_raw_visit(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            usize::MAX,
            |member_key, _| {
                let Some(member) = member_key.strip_prefix(prefix.as_slice()) else {
                    return true;
                };
                let Ok(member) = std::str::from_utf8(member) else {
                    return true;
                };
                if keys.iter().enumerate().all(|(idx, key)| {
                    idx == smallest
                        || self.store.contains_key(&zset_member_key(
                            self.db_index,
                            key,
                            versions[idx],
                            member,
                        ))
                }) {
                    count = count.saturating_add(1);
                }
                limit == 0 || count < limit
            },
        );
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
    Members { lookup: usize, count: usize },
}
