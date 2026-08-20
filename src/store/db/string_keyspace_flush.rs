use super::*;

impl Db {
    pub fn flushdb(&self) -> Result<(), Error> {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            (batch.delete_range(&prefix, &end)).expect("write batch append invariant violated");
        } else {
            for (key, _) in self.store.scan_prefix_raw(&prefix)? {
                if key.as_slice() != KEY_ENCODING_LAYOUT_META_KEY {
                    (batch.delete(&key)).expect("write batch append invariant violated");
                }
            }
        }
        self.ttl_manager
            .remove_db_to_batch(&mut batch, self.db_index)?;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch)?;
        }
        self.fulltext_clear_runtimes_for_db();
        self.vector_runtimes.remove_db(self.db_index);
        Ok(())
    }

    pub async fn flushdb_async(&self) -> Result<(), Error> {
        let shards = if !self.store.is_transactional() {
            (0..KEY_WRITE_LOCK_SHARDS).collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let _write_guards = self.lock_write_shards(&shards).await;
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            (batch.delete_range(&prefix, &end)).expect("write batch append invariant violated");
        } else {
            for (key, _) in self.store.scan_prefix_raw_async(&prefix).await? {
                if key.as_slice() != KEY_ENCODING_LAYOUT_META_KEY {
                    (batch.delete(&key)).expect("write batch append invariant violated");
                }
            }
        }
        self.ttl_manager
            .remove_db_to_batch_async(&mut batch, self.db_index)
            .await?;
        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await?;
        }
        self.fulltext_clear_runtimes_for_db();
        self.vector_runtimes.remove_db(self.db_index);
        Ok(())
    }
}
