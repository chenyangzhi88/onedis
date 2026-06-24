impl Db {
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

    pub(crate) fn zset_filter_entries_limited<F>(
        &self,
        key: &str,
        limit: usize,
        mut accept: F,
    ) -> Result<Vec<(String, f64)>, Error>
    where
        F: FnMut(&str, f64) -> bool,
    {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut entries = Vec::new();
        self.store
            .scan_range_raw_visit(&prefix, upper, usize::MAX, |rank_key, _| {
                let Some(score) = self.decode_rank_score(key, version, rank_key) else {
                    return true;
                };
                let Some(member) = self.decode_rank_member(key, version, rank_key) else {
                    return true;
                };
                if accept(&member, score) {
                    entries.push((member, score));
                }
                entries.len() < limit
            });
        Ok(entries)
    }

    pub(crate) async fn zset_filter_entries_limited_async<F>(
        &self,
        key: &str,
        limit: usize,
        mut accept: F,
    ) -> Result<Vec<(String, f64)>, Error>
    where
        F: FnMut(&str, f64) -> bool + Send,
    {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut entries = Vec::new();
        self.store
            .scan_range_raw_visit_async(&prefix, upper, usize::MAX, |rank_key, _| {
                let Some(score) = self.decode_rank_score(key, version, rank_key) else {
                    return true;
                };
                let Some(member) = self.decode_rank_member(key, version, rank_key) else {
                    return true;
                };
                if accept(&member, score) {
                    entries.push((member, score));
                }
                entries.len() < limit
            })
            .await;
        Ok(entries)
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
}
