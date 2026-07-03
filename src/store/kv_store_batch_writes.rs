impl KvStore {
    pub fn write_batch(&self, batch: &WriteBatch) {
        let started = Instant::now();
        if self.txn.is_some() {
            self.with_transaction_mut(|txn| {
                for (write_type, key, value) in batch.iter() {
                    match write_type {
                        WriteType::Put | WriteType::PutBlobMedium | WriteType::PutBlobExternal => txn
                            .put(key, value)
                            .expect("failed to stage batch put into kv_engine transaction"),
                        WriteType::Delete => txn
                            .delete(key)
                            .expect("failed to stage batch delete into kv_engine transaction"),
                        WriteType::RangeDelete => txn
                            .delete_range(key, value)
                            .expect("failed to stage batch range delete into kv_engine transaction"),
                        WriteType::Merge => {
                            panic!("merge is not supported by onedis transaction write batches")
                        }
                    }
                }
            });
            global_metrics().record_storage_write(elapsed_us(started), false);
            return;
        }
        let table_batch = SchemalessWriteBatch::from_write_batch(batch.clone());
        self.table
            .write(&table_batch)
            .expect("failed to write batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn write_batch_async(&self, batch: &WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(batch);
            return;
        }
        let started = Instant::now();
        let table_batch = SchemalessWriteBatch::from_write_batch(batch.clone());
        self.table
            .write_async(&table_batch)
            .await
            .expect("failed to write batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn compare_and_write_batch_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> KvResult<()> {
        if self.txn.is_some() {
            self.write_batch(batch);
            return Ok(());
        }
        let started = Instant::now();
        let table_batch = SchemalessWriteBatch::from_write_batch(batch.clone());
        let result = self
            .table
            .compare_and_write_async(conditions, &table_batch)
            .await;
        global_metrics().record_storage_write(elapsed_us(started), result.is_err());
        result
    }

    /// 直接提交到底层 DB，绕过当前事务视图。
    ///
    /// Version high-water reservations intentionally use this path: gaps are
    /// safe, but the reserved high-water mark must be durable before any
    /// transaction can publish keys using those versions.
    pub fn write_batch_direct(&self, batch: &WriteBatch) {
        let started = Instant::now();
        let table_batch = SchemalessWriteBatch::from_write_batch(batch.clone());
        self.table
            .write(&table_batch)
            .expect("failed to write direct batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn write_batch_direct_async(&self, batch: WriteBatch) {
        let started = Instant::now();
        let table_batch = SchemalessWriteBatch::from_write_batch(batch);
        self.table
            .write_async(&table_batch)
            .await
            .expect("failed to write direct batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }
}
