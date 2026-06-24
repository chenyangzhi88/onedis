impl KvStore {
    pub fn merge_raw(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw is only supported on non-transactional onedis stores");
        }
        self.table
            .merge(key, operand)
            .expect("failed to merge key into kv_engine");
    }

    pub async fn merge_raw_async(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw_async is only supported on non-transactional onedis stores");
        }
        self.table
            .merge_async(key, operand)
            .await
            .expect("failed to merge key into kv_engine");
    }

    /// 直接把原始 key/value 写入 kv_engine。
    pub fn put_raw(&self, key: &[u8], value: &[u8]) {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to write after transaction completion");
            txn.put(key, value)
                .expect("failed to stage key into kv_engine transaction");
            return;
        }
        self.table
            .put(key, value)
            .expect("failed to write key into kv_engine");
    }

    pub fn blob_put_raw(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw is only supported on non-transactional onedis stores");
        }
        self.table
            .blob_put(key, value)
            .expect("failed to write blob key into kv_engine");
    }

    pub async fn blob_put_raw_async(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw_async is only supported on non-transactional onedis stores");
        }
        self.table
            .blob_put_async(key, value)
            .await
            .expect("failed to write blob key into kv_engine");
    }

    pub fn delete_key(&self, key: &[u8]) -> bool {
        let existed = self.contains_key(key);
        if existed {
            if let Some(txn) = &self.txn {
                let mut guard = txn.lock().expect("transaction mutex poisoned");
                let txn = guard
                    .as_mut()
                    .expect("attempted to delete after transaction completion");
                txn.delete(key)
                    .expect("failed to stage delete into kv_engine transaction");
            } else {
                self.table
                    .delete(key)
                    .expect("failed to delete key from kv_engine");
            }
        }
        existed
    }
}
