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
