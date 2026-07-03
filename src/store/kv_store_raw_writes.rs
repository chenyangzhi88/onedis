impl KvStore {
    pub fn merge_raw(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw is only supported on non-transactional onedis stores");
        }
        let started = Instant::now();
        self.table
            .merge(key, operand)
            .expect("failed to merge key into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn merge_raw_async(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw_async is only supported on non-transactional onedis stores");
        }
        let started = Instant::now();
        self.table
            .merge_async(key, operand)
            .await
            .expect("failed to merge key into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    /// 直接把原始 key/value 写入 kv_engine。
    pub fn put_raw(&self, key: &[u8], value: &[u8]) {
        let started = Instant::now();
        if let Some(result) = self.with_transaction_mut(|txn| txn.put(key, value)) {
            result.expect("failed to stage key into kv_engine transaction");
            global_metrics().record_storage_write(elapsed_us(started), false);
            return;
        }
        self.table
            .put(key, value)
            .expect("failed to write key into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub fn blob_put_raw(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw is only supported on non-transactional onedis stores");
        }
        let started = Instant::now();
        self.table
            .blob_put(key, value)
            .expect("failed to write blob key into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn blob_put_raw_async(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw_async is only supported on non-transactional onedis stores");
        }
        let started = Instant::now();
        self.table
            .blob_put_async(key, value)
            .await
            .expect("failed to write blob key into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub fn delete_key(&self, key: &[u8]) -> bool {
        let existed = self.contains_key(key);
        if existed {
            let started = Instant::now();
            if let Some(result) = self.with_transaction_mut(|txn| txn.delete(key)) {
                result.expect("failed to stage delete into kv_engine transaction");
            } else {
                self.table
                    .delete(key)
                    .expect("failed to delete key from kv_engine");
            }
            global_metrics().record_storage_write(elapsed_us(started), false);
        }
        existed
    }
}
