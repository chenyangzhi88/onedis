use super::*;

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

        if let Some(entries) =
            self.zset_nonnegative_rank_range_limited(key, version, start, stop, reverse)?
        {
            return Ok(entries);
        }

        let mut entries = self.zset_ranked_members(key, version)?;
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

        if let Some(entries) = self
            .zset_nonnegative_rank_range_limited_async(key, version, start, stop, reverse)
            .await?
        {
            return Ok(entries);
        }

        let mut entries = self.zset_ranked_members_async(key, version).await?;
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

    fn zset_nonnegative_rank_range_limited(
        &self,
        key: &str,
        version: u64,
        start: i64,
        stop: i64,
        reverse: bool,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        if version == 0 || start < 0 || stop < 0 {
            return Ok(None);
        }
        if start > stop {
            return Ok(Some(Vec::new()));
        }
        let Some(stop) = stop.checked_add(1) else {
            return Ok(None);
        };
        let Ok(limit) = usize::try_from(stop) else {
            return Ok(None);
        };
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let raw_entries = if reverse {
            self.store
                .scan_range_raw_limited_reverse(&prefix, upper, limit)?
        } else {
            self.store.scan_range_raw_limited(&prefix, upper, limit)?
        };
        Ok(Some(
            raw_entries
                .into_iter()
                .skip(start as usize)
                .filter_map(|(rank_key, _)| {
                    Some((
                        self.decode_rank_member(key, version, &rank_key)?,
                        self.decode_rank_score(key, version, &rank_key)?,
                    ))
                })
                .collect(),
        ))
    }

    async fn zset_nonnegative_rank_range_limited_async(
        &self,
        key: &str,
        version: u64,
        start: i64,
        stop: i64,
        reverse: bool,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        if version == 0 || start < 0 || stop < 0 {
            return Ok(None);
        }
        if start > stop {
            return Ok(Some(Vec::new()));
        }
        let Some(stop) = stop.checked_add(1) else {
            return Ok(None);
        };
        let Ok(limit) = usize::try_from(stop) else {
            return Ok(None);
        };
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let raw_entries = if reverse {
            self.store
                .scan_range_raw_limited_reverse_async(&prefix, upper, limit)
                .await?
        } else {
            self.store
                .scan_range_raw_limited_async(&prefix, upper, limit)
                .await?
        };
        Ok(Some(
            raw_entries
                .into_iter()
                .skip(start as usize)
                .filter_map(|(rank_key, _)| {
                    Some((
                        self.decode_rank_member(key, version, &rank_key)?,
                        self.decode_rank_score(key, version, &rank_key)?,
                    ))
                })
                .collect(),
        ))
    }

    /// 按 score 区间返回成员和分数。
    pub fn zset_range_by_score(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_score_window(ZsetScoreWindow {
            key,
            min,
            min_inclusive: true,
            max,
            max_inclusive: true,
            reverse: false,
            limit: None,
        })
    }

    pub(crate) fn zset_range_by_score_window(
        &self,
        window: ZsetScoreWindow<'_>,
    ) -> Result<Vec<(String, f64)>, Error> {
        let ZsetScoreWindow {
            key,
            min,
            min_inclusive,
            max,
            max_inclusive,
            reverse,
            limit,
        } = window;
        let Some((offset, scan_limit)) = zset_score_scan_window(limit) else {
            return Ok(Vec::new());
        };
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        if version == 0 {
            let mut entries = self
                .zset_ranked_members(key, version)?
                .into_iter()
                .filter(|(_, score)| {
                    (*score > min || min_inclusive && *score == min)
                        && (*score < max || max_inclusive && *score == max)
                })
                .collect::<Vec<_>>();
            if reverse {
                entries.reverse();
            }
            return Ok(entries
                .into_iter()
                .skip(offset)
                .take(scan_limit.saturating_sub(offset))
                .collect());
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
            return Ok(Vec::new());
        };
        let raw_entries = if reverse {
            self.store
                .scan_range_raw_limited_reverse(&lower, upper, scan_limit)?
        } else {
            self.store
                .scan_range_raw_limited(&lower, upper, scan_limit)?
        };
        Ok(raw_entries
            .into_iter()
            .skip(offset)
            .filter_map(|(rank_key, _)| {
                Some((
                    self.decode_rank_member(key, version, &rank_key)?,
                    self.decode_rank_score(key, version, &rank_key)?,
                ))
            })
            .collect())
    }

    pub async fn zset_range_by_score_async(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_score_window_async(ZsetScoreWindow {
            key,
            min,
            min_inclusive: true,
            max,
            max_inclusive: true,
            reverse: false,
            limit: None,
        })
        .await
    }

    pub(crate) async fn zset_range_by_score_window_async(
        &self,
        window: ZsetScoreWindow<'_>,
    ) -> Result<Vec<(String, f64)>, Error> {
        let ZsetScoreWindow {
            key,
            min,
            min_inclusive,
            max,
            max_inclusive,
            reverse,
            limit,
        } = window;
        let Some((offset, scan_limit)) = zset_score_scan_window(limit) else {
            return Ok(Vec::new());
        };
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        if version == 0 {
            let mut entries = self
                .zset_ranked_members_async(key, version)
                .await?
                .into_iter()
                .filter(|(_, score)| {
                    (*score > min || min_inclusive && *score == min)
                        && (*score < max || max_inclusive && *score == max)
                })
                .collect::<Vec<_>>();
            if reverse {
                entries.reverse();
            }
            return Ok(entries
                .into_iter()
                .skip(offset)
                .take(scan_limit.saturating_sub(offset))
                .collect());
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
            return Ok(Vec::new());
        };
        let raw_entries = if reverse {
            self.store
                .scan_range_raw_limited_reverse_async(&lower, upper, scan_limit)
                .await?
        } else {
            self.store
                .scan_range_raw_limited_async(&lower, upper, scan_limit)
                .await?
        };
        Ok(raw_entries
            .into_iter()
            .skip(offset)
            .filter_map(|(rank_key, _)| {
                Some((
                    self.decode_rank_member(key, version, &rank_key)?,
                    self.decode_rank_score(key, version, &rank_key)?,
                ))
            })
            .collect())
    }

    pub fn zset_rev_range_by_score(
        &self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_score_window(ZsetScoreWindow {
            key,
            min,
            min_inclusive: true,
            max,
            max_inclusive: true,
            reverse: true,
            limit: None,
        })
    }

    pub async fn zset_rev_range_by_score_async(
        &self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_score_window_async(ZsetScoreWindow {
            key,
            min,
            min_inclusive: true,
            max,
            max_inclusive: true,
            reverse: true,
            limit: None,
        })
        .await
    }
}

fn zset_score_scan_window(limit: Option<(i64, i64)>) -> Option<(usize, usize)> {
    let Some((offset, count)) = limit else {
        return Some((0, usize::MAX));
    };
    if offset < 0 || count == 0 {
        return None;
    }
    let offset = usize::try_from(offset).ok()?;
    let scan_limit = if count < 0 {
        usize::MAX
    } else {
        offset.saturating_add(usize::try_from(count).ok()?)
    };
    Some((offset, scan_limit))
}
