impl Db {
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
}
