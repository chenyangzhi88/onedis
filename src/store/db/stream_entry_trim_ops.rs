use super::*;

impl Db {
    pub fn stream_trim_maxlen(&self, key: &str, max_len: usize) -> Result<usize, Error> {
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
        );
        let mut batch = WriteBatch::new();
        for (entry_key, _) in &entries {
            (batch.delete(entry_key)).expect("write batch append invariant violated");
        }
        let deleted = entries.len();
        meta.length = meta.length.saturating_sub(deleted as u64);
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }

    pub async fn stream_trim_maxlen_async(
        &self,
        key: &str,
        max_len: usize,
    ) -> Result<usize, Error> {
        let _stream_write_guard = self.set_write_lock(key).lock().await;
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
            .await;
        let mut batch = WriteBatch::new();
        for (entry_key, _) in &entries {
            (batch.delete(entry_key)).expect("write batch append invariant violated");
        }
        let deleted = entries.len();
        meta.length = meta.length.saturating_sub(deleted as u64);
        (batch.put(&self.mk(key), &encode_stream_meta(meta)))
            .expect("write batch append invariant violated");
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(deleted)
    }
}
