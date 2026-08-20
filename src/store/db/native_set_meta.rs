use super::*;

impl Db {
    pub(in crate::store::db) fn promote_packed_set(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(members) = decode_packed_set(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
            let version = self.next_version();
            let mut batch = WriteBatch::new();
            batch.put(
                &key_bytes,
                &encode_set_meta(header.expire_ms, version, members.len()),
            )?;
            for member in members {
                batch.put(
                    &set_member_key(self.db_index, key, version, &member),
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
        Err(Error::msg("ERR set layout promotion conflict"))
    }

    pub(in crate::store::db) async fn promote_packed_set_async(
        &self,
        key: &str,
    ) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let Some(members) = decode_packed_set(raw) else {
                return Ok(());
            };
            let header = decode_meta_header(raw)
                .ok_or_else(|| Error::msg("Failed to decode set metadata"))?;
            let version = self.next_version_async().await;
            let mut batch = WriteBatch::new();
            batch.put(
                &key_bytes,
                &encode_set_meta(header.expire_ms, version, members.len()),
            )?;
            for member in members {
                batch.put(
                    &set_member_key(self.db_index, key, version, &member),
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
        Err(Error::msg("ERR set layout promotion conflict"))
    }

    pub(in crate::store::db) fn set_meta(&self, key: &str) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed(key)?;

        let Some(raw) = self.store.get_raw(&self.mk(key))? else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }

    pub(in crate::store::db) async fn set_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed_async(key).await?;

        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await? else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }
}
