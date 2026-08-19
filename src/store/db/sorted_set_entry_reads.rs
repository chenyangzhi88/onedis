use super::set_random_pop_scan::random_member_ordinals;
use super::*;

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
        let mut entries = Vec::new();
        self.zset_visit_entries(key, |member, score| {
            if accept(&member, score) {
                entries.push((member, score));
            }
            entries.len() < limit
        })?;
        Ok(entries)
    }

    pub(crate) fn zset_visit_entries<F>(&self, key: &str, mut visit: F) -> Result<(), Error>
    where
        F: FnMut(String, f64) -> bool,
    {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(());
        };

        if version == 0 {
            for (member, score) in self.zset_ranked_members(key, version) {
                if !visit(member, score) {
                    break;
                }
            }
            return Ok(());
        }

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        self.store
            .scan_range_raw_visit(&prefix, upper, usize::MAX, |rank_key, _| {
                let Some(score) = self.decode_rank_score(key, version, rank_key) else {
                    return true;
                };
                let Some(member) = self.decode_rank_member(key, version, rank_key) else {
                    return true;
                };
                visit(member, score)
            });
        Ok(())
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
        let mut entries = Vec::new();
        self.zset_visit_entries_async(key, |member, score| {
            if accept(&member, score) {
                entries.push((member, score));
            }
            entries.len() < limit
        })
        .await?;
        Ok(entries)
    }

    pub(crate) async fn zset_visit_entries_async<F>(
        &self,
        key: &str,
        mut visit: F,
    ) -> Result<(), Error>
    where
        F: FnMut(String, f64) -> bool + Send,
    {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(());
        };

        if version == 0 {
            for (member, score) in self.zset_ranked_members_async(key, version).await {
                if !visit(member, score) {
                    break;
                }
            }
            return Ok(());
        }

        let prefix = zset_rank_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        self.store
            .scan_range_raw_visit_async(&prefix, upper, usize::MAX, |rank_key, _| {
                let Some(score) = self.decode_rank_score(key, version, rank_key) else {
                    return true;
                };
                let Some(member) = self.decode_rank_member(key, version, rank_key) else {
                    return true;
                };
                visit(member, score)
            })
            .await;
        Ok(())
    }

    pub fn zset_random_members(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        let Some((_, version)) = self.zset_expire_ms(key)? else {
            return Ok(None);
        };
        let len = self.zset_card(key)?;
        if len == 0 {
            return Ok(None);
        }
        let (picks, unique) = random_member_ordinals(len, count);
        self.zset_entries_at_ordinals(key, version, &picks, &unique)
            .map(Some)
    }

    pub async fn zset_random_members_async(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<(String, f64)>>, Error> {
        let Some((_, version)) = self.zset_expire_ms_async(key).await? else {
            return Ok(None);
        };
        let len = self.zset_card_async(key).await?;
        if len == 0 {
            return Ok(None);
        }
        let (picks, unique) = random_member_ordinals(len, count);
        self.zset_entries_at_ordinals_async(key, version, &picks, &unique)
            .await
            .map(Some)
    }

    fn zset_entries_at_ordinals(
        &self,
        key: &str,
        version: u64,
        picks: &[usize],
        unique: &[usize],
    ) -> Result<Vec<(String, f64)>, Error> {
        if version == 0 {
            let entries = self.zset_ranked_members(key, version);
            let by_ordinal = unique
                .iter()
                .filter_map(|ordinal| {
                    entries
                        .get(*ordinal)
                        .cloned()
                        .map(|entry| (*ordinal, entry))
                })
                .collect::<HashMap<_, _>>();
            return Ok(picks
                .iter()
                .map(|ordinal| {
                    by_ordinal
                        .get(ordinal)
                        .expect("selected packed sorted set ordinal is present")
                        .clone()
                })
                .collect());
        }
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let rank_keys = self.store.scan_range_raw_keys_at_ordinals(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            unique,
        );
        self.decode_random_zset_selection(key, version, picks, unique, rank_keys)
    }

    async fn zset_entries_at_ordinals_async(
        &self,
        key: &str,
        version: u64,
        picks: &[usize],
        unique: &[usize],
    ) -> Result<Vec<(String, f64)>, Error> {
        if version == 0 {
            let entries = self.zset_ranked_members_async(key, version).await;
            let by_ordinal = unique
                .iter()
                .filter_map(|ordinal| {
                    entries
                        .get(*ordinal)
                        .cloned()
                        .map(|entry| (*ordinal, entry))
                })
                .collect::<HashMap<_, _>>();
            return Ok(picks
                .iter()
                .map(|ordinal| {
                    by_ordinal
                        .get(ordinal)
                        .expect("selected packed sorted set ordinal is present")
                        .clone()
                })
                .collect());
        }
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let rank_keys = self
            .store
            .scan_range_raw_keys_at_ordinals_async(
                &prefix,
                prefix_exclusive_upper_bound(&prefix),
                unique,
            )
            .await;
        self.decode_random_zset_selection(key, version, picks, unique, rank_keys)
    }

    fn decode_random_zset_selection(
        &self,
        key: &str,
        version: u64,
        picks: &[usize],
        unique: &[usize],
        rank_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(String, f64)>, Error> {
        if rank_keys.len() != unique.len() {
            return Err(Error::msg(
                "ERR sorted set changed while selecting random members",
            ));
        }
        let mut entries = HashMap::with_capacity(unique.len());
        for (ordinal, rank_key) in unique.iter().copied().zip(rank_keys) {
            let member = self
                .decode_rank_member(key, version, &rank_key)
                .ok_or_else(|| Error::msg("ERR invalid sorted set rank key"))?;
            let score = self
                .decode_rank_score(key, version, &rank_key)
                .ok_or_else(|| Error::msg("ERR invalid sorted set rank key"))?;
            entries.insert(ordinal, (member, score));
        }
        Ok(picks
            .iter()
            .map(|ordinal| {
                entries
                    .get(ordinal)
                    .expect("selected sorted set ordinal is present")
                    .clone()
            })
            .collect())
    }
}
