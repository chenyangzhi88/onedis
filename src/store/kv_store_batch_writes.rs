impl KvStore {
    pub fn write_batch(&self, batch: &WriteBatch) {
        let started = Instant::now();
        if self.txn.is_some() {
            let failed = self
                .transaction_access_or(
                    self.with_transaction_mut(|txn| {
                        let mut failed = false;
                        for (write_type, key, value) in batch.iter() {
                            let result = match write_type {
                                WriteType::Put
                                | WriteType::PutBlobMedium
                                | WriteType::PutBlobExternal => txn.put(key, value),
                                WriteType::Delete => txn.delete(key),
                                WriteType::RangeDelete => txn.delete_range(key, value),
                                WriteType::Merge => Err(Status::Unsupported(
                                    "merge is not supported by onedis transaction write batches"
                                        .to_string(),
                                )),
                            };
                            if let Err(error) = result {
                                failed = true;
                                crate::store::health::storage_health()
                                    .record_failure("transaction batch staging", error);
                            }
                        }
                        failed
                    }),
                    || true,
                    "access transaction for batch write",
                )
                .unwrap_or(true);
            global_metrics().record_storage_write(elapsed_us(started), failed);
            return;
        }
        let result = bind_write_batch(&self.table, batch)
            .and_then(|table_batch| self.table.write(table_batch, self.write_options.clone()));
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("batch write", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn write_batch_async(&self, batch: &WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(batch);
            return;
        }
        let started = Instant::now();
        let result = match bind_write_batch(&self.table, batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("batch write async", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn write_batch_owned_async(&self, batch: WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(&batch);
            return;
        }
        let started = Instant::now();
        let result = match self.table.bind_write_batch(batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health()
                .record_failure("owned batch write async", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn compare_and_write_batch_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> KvResult<()> {
        let started = Instant::now();
        match self.with_transaction_mut(|txn| {
            for condition in conditions {
                let value = txn.get(&condition.key)?;
                if !condition.matches_transaction_value(value.as_deref()) {
                    return Err(Status::ConditionFailed(
                        "compare_and_write condition failed".to_string(),
                    ));
                }
            }
            stage_batch_in_transaction(txn, batch)
        }) {
            Ok(Some(result)) => {
                global_metrics().record_storage_write(elapsed_us(started), result.is_err());
                return result;
            }
            Ok(None) => {}
            Err(error) => {
                crate::store::health::storage_health()
                    .record_failure("access transaction for compare and write", &error);
                global_metrics().record_storage_write(elapsed_us(started), true);
                return Err(error);
            }
        }
        let table_batch = bind_write_batch(&self.table, batch)?;
        let engine_conditions = conditions
            .iter()
            .map(|condition| {
                condition.engine.clone().ok_or_else(|| {
                    Status::InvalidArgument(
                        "transaction observation cannot be used outside its transaction"
                            .to_string(),
                    )
                })
            })
            .collect::<KvResult<Vec<_>>>()?;
        let result = self
            .table
            .compare_and_write_async(
                engine_conditions,
                table_batch,
                self.write_options.clone(),
            )
            .await;
        global_metrics().record_storage_write(elapsed_us(started), result.is_err());
        result
    }

    pub fn compare_and_write_batch(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> KvResult<()> {
        let started = Instant::now();
        match self.with_transaction_mut(|txn| {
            for condition in conditions {
                let value = txn.get(&condition.key)?;
                if !condition.matches_transaction_value(value.as_deref()) {
                    return Err(Status::ConditionFailed(
                        "compare_and_write condition failed".to_string(),
                    ));
                }
            }
            stage_batch_in_transaction(txn, batch)
        }) {
            Ok(Some(result)) => {
                global_metrics().record_storage_write(elapsed_us(started), result.is_err());
                return result;
            }
            Ok(None) => {}
            Err(error) => {
                crate::store::health::storage_health()
                    .record_failure("access transaction for compare and write", &error);
                global_metrics().record_storage_write(elapsed_us(started), true);
                return Err(error);
            }
        }
        let table_batch = bind_write_batch(&self.table, batch)?;
        let engine_conditions = conditions
            .iter()
            .map(|condition| {
                condition.engine.clone().ok_or_else(|| {
                    Status::InvalidArgument(
                        "transaction observation cannot be used outside its transaction"
                            .to_string(),
                    )
                })
            })
            .collect::<KvResult<Vec<_>>>()?;
        let result = self.table.compare_and_write(
            engine_conditions,
            table_batch,
            self.write_options.clone(),
        );
        global_metrics().record_storage_write(elapsed_us(started), result.is_err());
        result
    }

    /// 直接提交到底层 DB，绕过当前事务视图。
    pub fn write_batch_direct(&self, batch: &WriteBatch) {
        let started = Instant::now();
        let result = bind_write_batch(&self.table, batch)
            .and_then(|table_batch| self.table.write(table_batch, self.write_options.clone()));
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("direct batch write", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }

    pub async fn write_batch_direct_async(&self, batch: WriteBatch) {
        let started = Instant::now();
        let result = match self.table.bind_write_batch(batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health()
                .record_failure("direct batch write async", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }
}

fn bind_write_batch(
    table: &SchemalessTable,
    batch: &WriteBatch,
) -> KvResult<SchemalessWriteBatch> {
    let mut table_batch = table.new_write_batch()?;
    for (write_type, key, value) in batch.iter() {
        match write_type {
            WriteType::Put | WriteType::PutBlobMedium | WriteType::PutBlobExternal => {
                table_batch.put(key, value)?
            }
            WriteType::Delete => table_batch.delete(key)?,
            WriteType::RangeDelete => table_batch.delete_range(key, value)?,
            WriteType::Merge => table_batch.merge(key, value)?,
        }
    }
    Ok(table_batch)
}

fn stage_batch_in_transaction(
    txn: &mut SchemalessTransaction,
    batch: &WriteBatch,
) -> KvResult<()> {
    for (write_type, key, value) in batch.iter() {
        match write_type {
            WriteType::Put | WriteType::PutBlobMedium | WriteType::PutBlobExternal => {
                txn.put(key, value)?
            }
            WriteType::Delete => txn.delete(key)?,
            WriteType::RangeDelete => txn.delete_range(key, value)?,
            WriteType::Merge => {
                return Err(Status::Unsupported(
                    "merge is not supported by onedis transaction write batches".to_string(),
                ));
            }
        }
    }
    Ok(())
}
