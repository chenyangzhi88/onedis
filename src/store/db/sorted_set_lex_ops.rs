use super::*;

impl Db {
    pub(crate) fn zset_range_by_lex(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_lex_window(key, min, max, false, None)
    }

    pub(crate) fn zset_range_by_lex_window(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<(String, f64)>, Error> {
        let Some((offset, scan_limit)) = zset_range_scan_window(limit) else {
            return Ok(Vec::new());
        };
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        if version == 0 {
            let mut entries = self
                .zset_members_raw(key, version)?
                .into_iter()
                .filter_map(|(member, score)| {
                    let member = String::from_utf8(member).ok()?;
                    if !zset_member_in_lex_range(&member, min, max) {
                        return None;
                    }
                    Some((member, decode_zset_score(&score)?))
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

        let Some((prefix, lower, upper)) =
            zset_lex_scan_bounds(self.db_index, key, version, min, max)
        else {
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
            .filter_map(|(member_key, value)| {
                let member = member_key.strip_prefix(prefix.as_slice())?;
                match (
                    String::from_utf8(member.to_vec()),
                    decode_zset_score(&value),
                ) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .collect())
    }

    pub(crate) async fn zset_range_by_lex_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        self.zset_range_by_lex_window_async(key, min, max, false, None)
            .await
    }

    pub(crate) async fn zset_range_by_lex_window_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<(String, f64)>, Error> {
        let Some((offset, scan_limit)) = zset_range_scan_window(limit) else {
            return Ok(Vec::new());
        };
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        if version == 0 {
            let mut entries = self
                .zset_members_raw_async(key, version)
                .await?
                .into_iter()
                .filter_map(|(member, score)| {
                    let member = String::from_utf8(member).ok()?;
                    if !zset_member_in_lex_range(&member, min, max) {
                        return None;
                    }
                    Some((member, decode_zset_score(&score)?))
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

        let Some((prefix, lower, upper)) =
            zset_lex_scan_bounds(self.db_index, key, version, min, max)
        else {
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
            .filter_map(|(member_key, value)| {
                let member = member_key.strip_prefix(prefix.as_slice())?;
                match (
                    String::from_utf8(member.to_vec()),
                    decode_zset_score(&value),
                ) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .collect())
    }

    pub(crate) fn zset_lex_count(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };
        if version == 0 {
            return Ok(self
                .zset_members_raw(key, version)?
                .into_iter()
                .filter_map(|(member, _)| String::from_utf8(member).ok())
                .filter(|member| zset_member_in_lex_range(member, min, max))
                .count());
        }
        let Some((_, lower, upper)) = zset_lex_scan_bounds(self.db_index, key, version, min, max)
        else {
            return Ok(0);
        };
        Ok(self.store.count_range_raw_keys(&lower, upper)?)
    }

    pub(crate) async fn zset_lex_count_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };
        if version == 0 {
            return Ok(self
                .zset_members_raw_async(key, version)
                .await?
                .into_iter()
                .filter_map(|(member, _)| String::from_utf8(member).ok())
                .filter(|member| zset_member_in_lex_range(member, min, max))
                .count());
        }
        let Some((_, lower, upper)) = zset_lex_scan_bounds(self.db_index, key, version, min, max)
        else {
            return Ok(0);
        };
        Ok(self.store.count_range_raw_keys_async(&lower, upper).await?)
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
        let _write_guard = self.set_write_lock(key).lock().await;
        let members = self
            .zset_range_by_lex_async(key, min, max)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>();
        self.zset_remove_async_unlocked(key, &members).await
    }
}

fn zset_member_in_lex_range(
    member: &str,
    min: &crate::cmds::sorted_set::zrange::LexBound,
    max: &crate::cmds::sorted_set::zrange::LexBound,
) -> bool {
    use crate::cmds::sorted_set::zrange::LexBound;

    let above_min = match min {
        LexBound::NegInfinity => true,
        LexBound::PosInfinity => false,
        LexBound::Value { value, inclusive } => {
            member > value.as_str() || *inclusive && member == value
        }
    };
    let below_max = match max {
        LexBound::PosInfinity => true,
        LexBound::NegInfinity => false,
        LexBound::Value { value, inclusive } => {
            member < value.as_str() || *inclusive && member == value
        }
    };
    above_min && below_max
}

fn zset_range_scan_window(limit: Option<(i64, i64)>) -> Option<(usize, usize)> {
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

fn zset_lex_scan_bounds(
    db_index: u16,
    key: &str,
    version: u64,
    min: &crate::cmds::sorted_set::zrange::LexBound,
    max: &crate::cmds::sorted_set::zrange::LexBound,
) -> Option<ZsetLexScanBounds> {
    use crate::cmds::sorted_set::zrange::LexBound;

    let prefix = zset_member_prefix(db_index, key, version);
    let lower = match min {
        LexBound::NegInfinity => prefix.clone(),
        LexBound::PosInfinity => return None,
        LexBound::Value { value, inclusive } => {
            let mut bound = zset_member_key(db_index, key, version, value);
            if !inclusive {
                bound.push(0);
            }
            bound
        }
    };
    let upper = match max {
        LexBound::PosInfinity => prefix_exclusive_upper_bound(&prefix),
        LexBound::NegInfinity => return None,
        LexBound::Value { value, inclusive } => {
            let mut bound = zset_member_key(db_index, key, version, value);
            if *inclusive {
                bound.push(0);
            }
            Some(bound)
        }
    };
    if upper
        .as_ref()
        .is_some_and(|upper| lower.as_slice() >= upper.as_slice())
    {
        return None;
    }
    Some((prefix, lower, upper))
}
