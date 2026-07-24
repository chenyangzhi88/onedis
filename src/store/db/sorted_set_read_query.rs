use super::*;

impl Db {
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
