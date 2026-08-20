use super::*;

impl Db {
    pub(in crate::store::db) fn set_members_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if version == 0 {
            return Ok(self
                .store
                .get_raw(&self.mk(key))?
                .as_deref()
                .and_then(decode_packed_set)
                .map(|members| {
                    members
                        .into_iter()
                        .map(String::into_bytes)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default());
        }
        let prefix = set_member_prefix(self.db_index, key, version);
        Ok(self
            .store
            .scan_prefix_raw(&prefix)?
            .into_iter()
            .filter_map(|(member_key, _)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| member.to_vec())
            })
            .collect())
    }

    pub(in crate::store::db) async fn set_members_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if version == 0 {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await?
                .as_deref()
                .and_then(decode_packed_set)
                .map(|members| {
                    members
                        .into_iter()
                        .map(String::into_bytes)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default());
        }
        let prefix = set_member_prefix(self.db_index, key, version);
        Ok(self
            .store
            .scan_prefix_raw_async(&prefix)
            .await?
            .into_iter()
            .filter_map(|(member_key, _)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| member.to_vec())
            })
            .collect())
    }
}
