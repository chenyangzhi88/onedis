impl KvStore {
    fn finish_storage_write<T>(
        &self,
        operation: &'static str,
        started: Instant,
        result: KvResult<T>,
    ) -> KvResult<T> {
        let failed = result.is_err();
        if let Err(error) = &result {
            self.health.record_failure(operation, error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
        result
    }

    pub fn merge_raw(&self, key: &[u8], operand: &[u8]) -> KvResult<()> {
        if self.txn.is_some() {
            return Err(Status::Unsupported(
                "merge is not supported on a transactional onedis store".to_string(),
            ));
        }
        let started = Instant::now();
        let result = self.table.merge(key, operand, self.write_options.clone());
        self.finish_storage_write("merge", started, result)
    }

    pub async fn merge_raw_async(&self, key: &[u8], operand: &[u8]) -> KvResult<()> {
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
        self.finish_storage_write("merge async", started, result)
    }

    /// Directly write one raw key/value pair through kv-engine.
    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        let started = Instant::now();
        let result = match self.with_transaction_mut(|txn| txn.put(key, value))? {
            Some(result) => result,
            None => self.table.put(key, value, self.write_options.clone()),
        };
        self.finish_storage_write("put", started, result)
    }

    pub fn blob_put_raw(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        if self.txn.is_some() {
            return Err(Status::Unsupported(
                "blob writes are not supported on a transactional onedis store".to_string(),
            ));
        }
        let started = Instant::now();
        let result = self.table.put(key, value, self.write_options.clone());
        self.finish_storage_write("blob put", started, result)
    }

    pub async fn blob_put_raw_async(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        if self.txn.is_some() {
            return Err(Status::Unsupported(
                "blob writes are not supported on a transactional onedis store".to_string(),
            ));
        }
        let started = Instant::now();
        let result = self
            .table
            .put_async(key, value, self.write_options.clone())
            .await;
        self.finish_storage_write("blob put async", started, result)
    }

    pub fn delete_key(&self, key: &[u8]) -> KvResult<bool> {
        let existed = self.contains_key(key)?;
        if !existed {
            return Ok(false);
        }
        let started = Instant::now();
        let result = match self.with_transaction_mut(|txn| txn.delete(key))? {
            Some(result) => result,
            None => self.table.delete(key, self.write_options.clone()),
        };
        self.finish_storage_write("delete", started, result)?;
        Ok(true)
    }
}
