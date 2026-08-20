use super::*;

impl Db {
    pub(in crate::store::db) async fn hash_meta_async(
        &self,
        key: &str,
    ) -> Result<Option<HashMeta>, Error> {
        self.expire_if_needed_async(key).await?;
        self.store
            .get_raw_async(&self.mk(key))
            .await?
            .map(|raw| decode_hash_meta_checked(&raw))
            .transpose()
    }

    pub(in crate::store::db) fn hash_expire_ms(
        &self,
        key: &str,
    ) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed(key)?;

        let Some(raw) = self.store.get_raw(&key_bytes)? else {
            return Ok(None);
        };

        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    pub(in crate::store::db) async fn hash_expire_ms_async(
        &self,
        key: &str,
    ) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed_async(key).await?;

        let Some(raw) = self.store.get_raw_async(&key_bytes).await? else {
            return Ok(None);
        };
        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    pub(in crate::store::db) fn hash_entries_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<RawKeyValue>, Error> {
        if version == 0 {
            return Ok(self
                .store
                .get_raw(&self.mk(key))?
                .and_then(|raw| decode_packed_hash(&raw))
                .map(|fields| {
                    fields
                        .into_iter()
                        .map(|(field, value)| (field.into_bytes(), value))
                        .collect()
                })
                .unwrap_or_default());
        }
        let prefix = hash_field_prefix(self.db_index, key, version);
        Ok(self
            .store
            .scan_prefix_raw(&prefix)?
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect())
    }

    pub(in crate::store::db) fn hash_live_entries_raw(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<RawKeyValue>, Error> {
        let mut live = Vec::new();
        for (field, value) in self.hash_entries_raw(key, version)? {
            let field_text = String::from_utf8_lossy(&field);
            if self.hash_field_is_live(key, version, &field_text)? {
                live.push((field, value));
            }
        }
        Ok(live)
    }

    pub(in crate::store::db) async fn hash_live_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<RawKeyValue>, Error> {
        let mut entries = Vec::new();
        for (field, value) in self.hash_entries_raw_async(key, version).await? {
            let field_text = String::from_utf8_lossy(&field);
            if self
                .hash_field_is_live_async(key, version, &field_text)
                .await?
            {
                entries.push((field, value));
            }
        }
        Ok(entries)
    }

    pub(in crate::store::db) async fn hash_live_entries_for_meta_async(
        &self,
        key: &str,
        meta: HashMeta,
    ) -> Result<Vec<RawKeyValue>, Error> {
        let entries = self.hash_entries_raw_async(key, meta.version).await?;
        if !meta.may_have_field_ttl {
            return Ok(entries);
        }
        let mut live = Vec::with_capacity(entries.len());
        for (field, value) in entries {
            let field_text = String::from_utf8_lossy(&field);
            if self
                .hash_field_is_live_async(key, meta.version, &field_text)
                .await?
            {
                live.push((field, value));
            }
        }
        Ok(live)
    }

    pub(in crate::store::db) fn hash_field_is_live(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> Result<bool, Error> {
        if version == 0 {
            return Ok(true);
        }
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let field_key = hash_field_key(self.db_index, key, version, field);
        for _ in 0..64 {
            let observed_expire = self.store.get_raw_observed(&expire_key)?;
            let Some(raw) = observed_expire.value() else {
                return Ok(true);
            };
            let Some(expire_ms) = decode_u64_be(raw) else {
                return Ok(true);
            };
            if expire_ms == 0 || now_ms() < expire_ms {
                return Ok(true);
            }

            let observed_field = self.store.get_raw_observed(&field_key)?;
            let mut batch = WriteBatch::new();
            (batch.delete(&field_key)).expect("write batch append invariant violated");
            (batch.delete(&expire_key)).expect("write batch append invariant violated");
            match self.compare_and_write_batch_if_not_empty(
                &[
                    CompareCondition::from_observed(&observed_expire),
                    CompareCondition::from_observed(&observed_field),
                ],
                &batch,
            ) {
                Ok(true) => return Ok(false),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR hash field expiration conflict"))
    }

    pub(in crate::store::db) async fn hash_field_is_live_async(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> Result<bool, Error> {
        if version == 0 {
            return Ok(true);
        }
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let field_key = hash_field_key(self.db_index, key, version, field);
        for _ in 0..64 {
            let observed_expire = self.store.get_raw_observed_async(&expire_key).await?;
            let Some(raw) = observed_expire.value() else {
                return Ok(true);
            };
            let Some(expire_ms) = decode_u64_be(raw) else {
                return Ok(true);
            };
            if expire_ms == 0 || now_ms() < expire_ms {
                return Ok(true);
            }

            let observed_field = self.store.get_raw_observed_async(&field_key).await?;
            let mut batch = WriteBatch::new();
            (batch.delete(&field_key)).expect("write batch append invariant violated");
            (batch.delete(&expire_key)).expect("write batch append invariant violated");
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[
                        CompareCondition::from_observed(&observed_expire),
                        CompareCondition::from_observed(&observed_field),
                    ],
                    &batch,
                )
                .await
            {
                Ok(true) => return Ok(false),
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::msg("ERR hash field expiration conflict"))
    }

    pub(in crate::store::db) fn hash_live_field_value(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        if version == 0 {
            return Ok(self
                .store
                .get_raw(&self.mk(key))?
                .and_then(|raw| decode_packed_hash(&raw))
                .and_then(|fields| fields.get(field).cloned()));
        }
        if !self.hash_field_is_live(key, version, field)? {
            return Ok(None);
        }
        Ok(self
            .store
            .get_raw(&hash_field_key(self.db_index, key, version, field))?)
    }

    pub(in crate::store::db) async fn hash_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Result<Vec<RawKeyValue>, Error> {
        if version == 0 {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await?
                .and_then(|raw| decode_packed_hash(&raw))
                .map(|fields| {
                    fields
                        .into_iter()
                        .map(|(field, value)| (field.into_bytes(), value))
                        .collect()
                })
                .unwrap_or_default());
        }
        let prefix = hash_field_prefix(self.db_index, key, version);
        Ok(self
            .store
            .scan_prefix_raw_async(&prefix)
            .await?
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect())
    }

    /// Promotes an inline small hash to the versioned field layout. The main-record CAS makes
    /// promotion safe without requiring callers to hold a structural lock.
    pub(in crate::store::db) fn promote_packed_hash(&self, key: &str) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        loop {
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let meta = decode_hash_meta_checked(raw)?;
            if !meta.packed {
                return Ok(());
            }
            let fields = decode_packed_hash(raw)
                .ok_or_else(|| Error::msg("Failed to decode packed hash"))?;
            let version = self.next_version();
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &encode_hash_meta(meta.expire_ms, version)))
                .expect("write batch append invariant violated");
            for (field, value) in fields {
                (batch.put(&hash_field_key(self.db_index, key, version, &field), &value))
                    .expect("write batch append invariant violated");
            }
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                return Ok(());
            }
        }
    }

    pub(in crate::store::db) async fn promote_packed_hash_async(
        &self,
        key: &str,
    ) -> Result<(), Error> {
        let key_bytes = self.mk(key);
        loop {
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(());
            };
            let meta = decode_hash_meta_checked(raw)?;
            if !meta.packed {
                return Ok(());
            }
            let fields = decode_packed_hash(raw)
                .ok_or_else(|| Error::msg("Failed to decode packed hash"))?;
            let version = self.next_version_async().await;
            let mut batch = WriteBatch::new();
            (batch.put(&key_bytes, &encode_hash_meta(meta.expire_ms, version)))
                .expect("write batch append invariant violated");
            for (field, value) in fields {
                (batch.put(&hash_field_key(self.db_index, key, version, &field), &value))
                    .expect("write batch append invariant violated");
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
    }
}
