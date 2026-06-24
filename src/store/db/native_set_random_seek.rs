impl Db {
    fn set_random_seek_members(&self, key: &str, version: u64, count: usize) -> Vec<Vec<u8>> {
        if count == 0 {
            return Vec::new();
        }

        let prefix = set_member_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut members = Vec::with_capacity(count);
        let mut seen = HashSet::with_capacity(count);
        let attempts = count.saturating_mul(2).max(1);

        for _ in 0..attempts {
            if members.len() >= count {
                break;
            }
            let mut lower = prefix.clone();
            lower.extend_from_slice(&random_u64().to_be_bytes());

            let mut hit = self.store.scan_range_raw_limited(&lower, upper.clone(), 1);
            if hit.is_empty() {
                hit = self.store.scan_range_raw_limited(&prefix, upper.clone(), 1);
            }
            if let Some((member_key, _)) = hit.into_iter().next()
                && let Some(member) = member_key.strip_prefix(prefix.as_slice())
            {
                let member = member.to_vec();
                if seen.insert(member.clone()) {
                    members.push(member);
                }
            }
        }

        if members.len() < count {
            for (member_key, _) in
                self.store
                    .scan_range_raw_limited(&prefix, upper, count.saturating_mul(2))
            {
                if let Some(member) = member_key.strip_prefix(prefix.as_slice()) {
                    let member = member.to_vec();
                    if seen.insert(member.clone()) {
                        members.push(member);
                        if members.len() >= count {
                            break;
                        }
                    }
                }
            }
        }

        members
    }

    async fn set_random_seek_members_async(
        &self,
        key: &str,
        version: u64,
        count: usize,
    ) -> Vec<Vec<u8>> {
        if count == 0 {
            return Vec::new();
        }

        let prefix = set_member_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut members = Vec::with_capacity(count);
        let mut seen = HashSet::with_capacity(count);
        let attempts = count.saturating_mul(2).max(1);

        for _ in 0..attempts {
            if members.len() >= count {
                break;
            }
            let mut lower = prefix.clone();
            lower.extend_from_slice(&random_u64().to_be_bytes());

            let mut hit = self
                .store
                .scan_range_raw_limited_async(&lower, upper.clone(), 1)
                .await;
            if hit.is_empty() {
                hit = self
                    .store
                    .scan_range_raw_limited_async(&prefix, upper.clone(), 1)
                    .await;
            }
            if let Some((member_key, _)) = hit.into_iter().next()
                && let Some(member) = member_key.strip_prefix(prefix.as_slice())
            {
                let member = member.to_vec();
                if seen.insert(member.clone()) {
                    members.push(member);
                }
            }
        }

        if members.len() < count {
            for (member_key, _) in self
                .store
                .scan_range_raw_limited_async(&prefix, upper, count.saturating_mul(2))
                .await
            {
                if let Some(member) = member_key.strip_prefix(prefix.as_slice()) {
                    let member = member.to_vec();
                    if seen.insert(member.clone()) {
                        members.push(member);
                        if members.len() >= count {
                            break;
                        }
                    }
                }
            }
        }

        members
    }
}
