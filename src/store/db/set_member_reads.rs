use super::*;

impl Db {
    /// 检查 member 是否属于 set。
    pub fn set_contains(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        Ok(self
            .store
            .contains_key(&set_member_key(self.db_index, key, meta.version, member)))
    }

    pub async fn set_contains_async(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        Ok(self
            .store
            .contains_key_async(&set_member_key(self.db_index, key, meta.version, member))
            .await)
    }

    /// 返回 set 成员数量。
    pub fn set_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self.set_meta(key)?.map_or(0, |meta| meta.len))
    }

    pub async fn set_len_async(&self, key: &str) -> Result<usize, Error> {
        Ok(self.set_meta_async(key).await?.map_or(0, |meta| meta.len))
    }

    /// 返回 set 所有成员。
    pub fn set_members(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw(key, meta.version)
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    /// Returns set members without allowing a single response to materialize an
    /// unbounded amount of data first.
    pub fn set_members_bounded(
        &self,
        key: &str,
        max_members: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        if meta.len > max_members {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }

        let prefix = set_member_prefix(self.db_index, key, meta.version);
        let mut members = Vec::with_capacity(meta.len.min(max_members));
        let mut encoded_bytes = 32usize;
        let mut exceeded = false;
        self.store.scan_range_raw_visit(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            max_members.saturating_add(1),
            |member_key, _| {
                let Some(raw_member) = member_key.strip_prefix(prefix.as_slice()) else {
                    return true;
                };
                let Ok(member) = String::from_utf8(raw_member.to_vec()) else {
                    return true;
                };
                let Some(next_bytes) = encoded_bytes.checked_add(member.len().saturating_add(32))
                else {
                    exceeded = true;
                    return false;
                };
                if members.len() >= max_members || next_bytes > max_encoded_bytes {
                    exceeded = true;
                    return false;
                }
                encoded_bytes = next_bytes;
                members.push(member);
                true
            },
        );

        if exceeded {
            Err(Error::msg("ERR response exceeds configured limit"))
        } else {
            Ok(members)
        }
    }

    pub async fn set_members_async(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw_async(key, meta.version)
            .await
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    pub async fn set_members_bounded_async(
        &self,
        key: &str,
        max_members: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<String>, Error> {
        let key = key.to_owned();
        self.run_blocking_store_task(move |db| {
            db.set_members_bounded(&key, max_members, max_encoded_bytes)
        })
        .await
    }

    pub(in crate::store::db) async fn set_member_set_async(
        &self,
        key: &str,
    ) -> Result<Option<HashSet<String>>, Error> {
        match self.set_meta_async(key).await? {
            Some(meta) => Ok(Some(
                self.set_members_raw_async(key, meta.version)
                    .await
                    .into_iter()
                    .filter_map(|member| String::from_utf8(member).ok())
                    .collect(),
            )),
            None => Ok(None),
        }
    }
}
