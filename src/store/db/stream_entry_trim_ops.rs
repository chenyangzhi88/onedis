use super::*;

impl Db {
    fn try_stream_trim_packed(&self, key: &str, max_len: usize) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            let observed = self.store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(Some(0));
            };
            let Some((mut meta, mut entries)) = decode_packed_stream(raw) else {
                return Ok(None);
            };
            if entries.len() <= max_len {
                return Ok(Some(0));
            }
            let deleted = entries.len() - max_len;
            entries.drain(..deleted);
            meta.length = entries.len() as u64;
            let encoded = encode_packed_stream(meta, &entries)
                .ok_or_else(|| Error::msg("Failed to encode packed stream"))?;
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encoded)?;
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(deleted));
            }
        }
        self.promote_packed_stream(key)?;
        Ok(None)
    }

    async fn try_stream_trim_packed_async(
        &self,
        key: &str,
        max_len: usize,
    ) -> Result<Option<usize>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            let observed = self.store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(Some(0));
            };
            let Some((mut meta, mut entries)) = decode_packed_stream(raw) else {
                return Ok(None);
            };
            if entries.len() <= max_len {
                return Ok(Some(0));
            }
            let deleted = entries.len() - max_len;
            entries.drain(..deleted);
            meta.length = entries.len() as u64;
            let encoded = encode_packed_stream(meta, &entries)
                .ok_or_else(|| Error::msg("Failed to encode packed stream"))?;
            let mut batch = WriteBatch::new();
            batch.put(&key_bytes, &encoded)?;
            if self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(deleted));
            }
        }
        self.promote_packed_stream_async(key).await?;
        Ok(None)
    }

    pub fn stream_trim_maxlen(&self, key: &str, max_len: usize) -> Result<usize, Error> {
        self.expire_if_needed(key)?;
        if let Some(deleted) = self.try_stream_trim_packed(key, max_len)? {
            return Ok(deleted);
        }
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        if meta.length <= max_len as u64 {
            return Ok(0);
        }
        let delete_count = usize::try_from(meta.length - max_len as u64).unwrap_or(usize::MAX);
        let prefix = stream_entry_prefix(self.db_index, key, meta.version);
        let entries = self.store.scan_range_raw_limited(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            delete_count,
        )?;
        let mut batch = WriteBatch::new();
        for (entry_key, _) in &entries {
            (batch.delete(entry_key)).expect("write batch append invariant violated");
        }
        let deleted = entries.len();
        meta.length = meta.length.saturating_sub(deleted as u64);
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty(&batch)?;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }

    pub async fn stream_trim_maxlen_async(
        &self,
        key: &str,
        max_len: usize,
    ) -> Result<usize, Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
        self.expire_if_needed_async(key).await?;
        if let Some(deleted) = self.try_stream_trim_packed_async(key, max_len).await? {
            return Ok(deleted);
        }
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        if meta.length <= max_len as u64 {
            return Ok(0);
        }
        let delete_count = usize::try_from(meta.length - max_len as u64).unwrap_or(usize::MAX);
        let prefix = stream_entry_prefix(self.db_index, key, meta.version);
        let entries = self
            .store
            .scan_range_raw_limited_async(
                &prefix,
                prefix_exclusive_upper_bound(&prefix),
                delete_count,
            )
            .await?;
        let mut batch = WriteBatch::new();
        for (entry_key, _) in &entries {
            (batch.delete(entry_key)).expect("write batch append invariant violated");
        }
        let deleted = entries.len();
        meta.length = meta.length.saturating_sub(deleted as u64);
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty_async(&batch).await?;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }
}
