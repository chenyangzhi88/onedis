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

}
