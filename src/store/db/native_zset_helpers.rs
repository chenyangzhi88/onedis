use super::*;

impl Db {
    pub(in crate::store::db) fn zset_expire_ms(
        &self,
        key: &str,
    ) -> Result<Option<(u64, u64)>, Error> {
        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode sorted set metadata"));
        };

        if header.type_tag != TYPE_SORTED_SET {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Ok(Some((header.expire_ms, header.version)))
    }

    pub(in crate::store::db) async fn zset_expire_ms_async(
        &self,
        key: &str,
    ) -> Result<Option<(u64, u64)>, Error> {
        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode sorted set metadata"));
        };

        if header.type_tag != TYPE_SORTED_SET {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Ok(Some((header.expire_ms, header.version)))
    }

    pub(in crate::store::db) fn zset_members_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = zset_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(member_key, value)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| (member.to_vec(), value))
            })
            .collect()
    }

    pub(in crate::store::db) async fn zset_members_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = zset_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(member_key, value)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| (member.to_vec(), value))
            })
            .collect()
    }

    pub(in crate::store::db) fn zset_rank_entries_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan_prefix_raw(&zset_rank_prefix(self.db_index, key, version))
    }

    pub(in crate::store::db) async fn zset_rank_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan_prefix_raw_async(&zset_rank_prefix(self.db_index, key, version))
            .await
    }

    pub(in crate::store::db) fn zset_ranked_members(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(String, f64)> {
        self.zset_rank_entries_raw(key, version)
            .into_iter()
            .filter_map(|(rank_key, _)| {
                let score = self.decode_rank_score(key, version, &rank_key)?;
                let member = self.decode_rank_member(key, version, &rank_key)?;
                Some((member, score))
            })
            .collect()
    }

    pub(in crate::store::db) async fn zset_ranked_members_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(String, f64)> {
        self.zset_rank_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(rank_key, _)| {
                let score = self.decode_rank_score(key, version, &rank_key)?;
                let member = self.decode_rank_member(key, version, &rank_key)?;
                Some((member, score))
            })
            .collect()
    }
}
