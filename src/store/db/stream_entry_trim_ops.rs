use super::*;

impl Db {
    pub fn stream_trim_maxlen(&self, key: &str, max_len: usize) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let entries = self.stream_entries_raw(key, meta.version);
        if entries.len() <= max_len {
            return Ok(0);
        }
        let delete_count = entries.len() - max_len;
        let mut batch = WriteBatch::new();
        for (id, _) in entries.into_iter().take(delete_count) {
            batch.delete(&stream_entry_key(self.db_index, key, meta.version, id));
        }
        meta.length = meta.length.saturating_sub(delete_count as u64);
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(delete_count)
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
        let entries = self.stream_entries_raw_async(key, meta.version).await;
        if entries.len() <= max_len {
            return Ok(0);
        }
        let delete_count = entries.len() - max_len;
        let mut batch = WriteBatch::new();
        for (id, _) in entries.into_iter().take(delete_count) {
            batch.delete(&stream_entry_key(self.db_index, key, meta.version, id));
        }
        meta.length = meta.length.saturating_sub(delete_count as u64);
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(delete_count)
    }
}
