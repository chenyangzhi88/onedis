use super::*;

impl Db {
    pub fn stream_delete(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta(key)? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            if self.store.get_raw(&entry_key).is_some() {
                batch.delete(&entry_key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            batch.put(&self.mk(key), &encode_stream_meta(meta));
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub async fn stream_delete_async(&self, key: &str, ids: &[StreamId]) -> Result<usize, Error> {
        let Some(mut meta) = self.stream_meta_async(key).await? else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        let mut batch = WriteBatch::new();
        for id in ids {
            let entry_key = stream_entry_key(self.db_index, key, meta.version, *id);
            if self.store.get_raw_async(&entry_key).await.is_some() {
                batch.delete(&entry_key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            meta.length = meta.length.saturating_sub(deleted as u64);
            batch.put(&self.mk(key), &encode_stream_meta(meta));
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    pub fn stream_set_id(&self, key: &str, id: StreamId) -> Result<(), Error> {
        let mut meta = self
            .stream_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        meta.last_id = id;
        let mut batch = WriteBatch::new();
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stream_set_id_async(&self, key: &str, id: StreamId) -> Result<(), Error> {
        let mut meta = self
            .stream_meta_async(key)
            .await?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        meta.last_id = id;
        let mut batch = WriteBatch::new();
        batch.put(&self.mk(key), &encode_stream_meta(meta));
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn stream_ack_delete(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        self.stream_ack(key, group, ids)?;
        self.stream_delete(key, ids)
    }

    pub async fn stream_ack_delete_async(
        &self,
        key: &str,
        group: &str,
        ids: &[StreamId],
    ) -> Result<usize, Error> {
        self.stream_ack_async(key, group, ids).await?;
        self.stream_delete_async(key, ids).await
    }
}
