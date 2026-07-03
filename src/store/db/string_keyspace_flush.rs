use super::*;

impl Db {
    pub fn flushdb(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            batch.delete_range(&prefix, &end);
        } else {
            for (key, _) in self.store.scan_prefix_raw(&prefix) {
                batch.delete(&key);
            }
        }
        self.ttl_manager
            .remove_db_to_batch(&mut batch, self.db_index);
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }

    pub async fn flushdb_async(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            batch.delete_range(&prefix, &end);
        } else {
            for (key, _) in self.store.scan_prefix_raw_async(&prefix).await {
                batch.delete(&key);
            }
        }
        self.ttl_manager
            .remove_db_to_batch_async(&mut batch, self.db_index)
            .await;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }
}
