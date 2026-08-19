impl KvStore {
    pub fn merge_raw(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            crate::store::health::storage_health().record_failure(
                "merge",
                "merge is not supported on a transactional onedis store",
            );
            return;
        }
        let started = Instant::now();
        let result = self.table.merge(key, operand, self.write_options.clone());
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("merge", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn merge_raw_async(&self, key: &[u8], operand: &[u8]) {
        if let Err(error) = self.try_merge_raw_async(key, operand).await {
            crate::store::health::storage_health().record_failure("merge_async", error);
        }
    }

    pub async fn try_merge_raw_async(&self, key: &[u8], operand: &[u8]) -> KvResult<()> {
        if self.txn.is_some() {
            return Err(Status::Unsupported(
                "merge is not supported on a transactional onedis store".to_string(),
            ));
        }
        let started = Instant::now();
        let result = self
            .table
            .merge_async(key, operand, self.write_options.clone())
            .await;
        global_metrics().record_storage_write(elapsed_us(started), result.is_err());
        result
    }

    /// 直接把原始 key/value 写入 kv_engine。
    pub fn put_raw(&self, key: &[u8], value: &[u8]) {
        let started = Instant::now();
        if let Some(result) = self.transaction_access_or(
            self.with_transaction_mut(|txn| txn.put(key, value)),
            || {
                Err(Status::InvalidArgument(
                    "unable to access onedis transaction".to_string(),
                ))
            },
            "access transaction for put",
        ) {
            let failed = result.is_err();
            if let Err(error) = result {
                crate::store::health::storage_health()
                    .record_failure("transaction put", error);
            }
            global_metrics().record_storage_write(elapsed_us(started), failed);
            return;
        }
        let result = self.table.put(key, value, self.write_options.clone());
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("put", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub fn blob_put_raw(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            crate::store::health::storage_health().record_failure(
                "blob put",
                "blob writes are not supported on a transactional onedis store",
            );
            return;
        }
        let started = Instant::now();
        let result = self.table.put(key, value, self.write_options.clone());
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("blob put", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn blob_put_raw_async(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            crate::store::health::storage_health().record_failure(
                "blob put async",
                "blob writes are not supported on a transactional onedis store",
            );
            return;
        }
        let started = Instant::now();
        let result = self
            .table
            .put_async(key, value, self.write_options.clone())
            .await;
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("blob put async", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub fn delete_key(&self, key: &[u8]) -> bool {
        let existed = self.contains_key(key);
        if existed {
            let started = Instant::now();
            let mut failed = false;
            if let Some(result) = self.transaction_access_or(
                self.with_transaction_mut(|txn| txn.delete(key)),
                || {
                    Err(Status::InvalidArgument(
                        "unable to access onedis transaction".to_string(),
                    ))
                },
                "access transaction for delete",
            ) {
                failed = result.is_err();
                if let Err(error) = result {
                    crate::store::health::storage_health()
                        .record_failure("transaction delete", error);
                }
            } else {
                if let Err(error) = self.table.delete(key, self.write_options.clone()) {
                    failed = true;
                    crate::store::health::storage_health().record_failure("delete", error);
                }
            }
            global_metrics().record_storage_write(elapsed_us(started), failed);
        }
        existed
    }
}
