use super::*;

impl Db {
    pub(in crate::store::db) fn promote_packed_zset(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(entries) = decode_packed_zset(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
            let version = self.next_version();
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encode_zset_meta(header.expire_ms, version))?;
            for (member, score) in entries {
                batch.put(
                    &zset_member_key(self.db_index, key, version, &member),
                    &score.to_be_bytes(),
                )?;
                batch.put(
                    &zset_rank_key(self.db_index, key, version, score, &member),
                    INDEX_MARKER_VALUE,
                )?;
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                return Ok(());
            }
        }
        Err(Error::msg("ERR sorted set layout promotion conflict"))
    }

    pub(in crate::store::db) async fn promote_packed_zset_async(
        &self,
        key: &str,
    ) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(entries) = decode_packed_zset(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode sorted set metadata"))?;
            let version = self.next_version_async().await;
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encode_zset_meta(header.expire_ms, version))?;
            for (member, score) in entries {
                batch.put(
                    &zset_member_key(self.db_index, key, version, &member),
                    &score.to_be_bytes(),
                )?;
                batch.put(
                    &zset_rank_key(self.db_index, key, version, score, &member),
                    INDEX_MARKER_VALUE,
                )?;
            }
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::msg("ERR sorted set layout promotion conflict"))
    }

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
        if version == 0 {
            return self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_zset)
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|(member, score)| (member.into_bytes(), score.to_be_bytes().to_vec()))
                        .collect()
                })
                .unwrap_or_default();
        }
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
        if version == 0 {
            return self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|(member, score)| (member.into_bytes(), score.to_be_bytes().to_vec()))
                        .collect()
                })
                .unwrap_or_default();
        }
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
        if version == 0 {
            let mut entries = self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_zset)
                .map(|entries| entries.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            return entries
                .into_iter()
                .map(|(member, score)| {
                    (
                        zset_rank_key(self.db_index, key, version, score, &member),
                        INDEX_MARKER_VALUE.to_vec(),
                    )
                })
                .collect();
        }
        self.store
            .scan_prefix_raw(&zset_rank_prefix(self.db_index, key, version))
    }

    pub(in crate::store::db) async fn zset_rank_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if version == 0 {
            let mut entries = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .map(|entries| entries.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            return entries
                .into_iter()
                .map(|(member, score)| {
                    (
                        zset_rank_key(self.db_index, key, version, score, &member),
                        INDEX_MARKER_VALUE.to_vec(),
                    )
                })
                .collect();
        }
        self.store
            .scan_prefix_raw_async(&zset_rank_prefix(self.db_index, key, version))
            .await
    }

    pub(in crate::store::db) fn zset_ranked_members(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(String, f64)> {
        if version == 0 {
            let mut entries = self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_zset)
                .map(PackedZsetEntries::into_iter)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            return entries;
        }
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
        if version == 0 {
            let mut entries = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_zset)
                .map(PackedZsetEntries::into_iter)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| left_member.cmp(right_member))
            });
            return entries;
        }
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
