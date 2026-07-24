use super::*;

impl Db {
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
        let _write_guard = self.set_write_lock(destination).lock().await;
        let len = entries.len();
        if len == 0 {
            self.delete_key_internal_async(destination, true).await;
            return Ok(0);
        }
        self.delete_key_internal_async(destination, true).await;
        let members = entries
            .into_iter()
            .map(|(member, score)| (score, member))
            .collect::<Vec<_>>();
        self.zset_add_async_unlocked(destination, &members).await?;
        Ok(len)
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
}
