impl KvStore {
    pub fn write_batch(&self, batch: &WriteBatch) -> KvResult<()> {
        let started = Instant::now();
        if self.txn.is_some() {
            let result = match self
                .with_transaction_mut(|txn| stage_batch_in_transaction(txn, batch))?
            {
                Some(result) => result,
                None => Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                )),
            };
            return self.finish_storage_write("transaction batch staging", started, result);
        }
        let result = bind_write_batch(&self.table, batch)
            .and_then(|table_batch| self.table.write(table_batch, self.write_options.clone()));
        self.finish_storage_write("batch write", started, result)
    }

    pub async fn write_batch_async(&self, batch: &WriteBatch) -> KvResult<()> {
        if self.txn.is_some() {
            return self.write_batch(batch);
        }
        let started = Instant::now();
        let result = match bind_write_batch(&self.table, batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        self.finish_storage_write("batch write async", started, result)
    }

    pub async fn write_batch_owned_async(&self, batch: WriteBatch) -> KvResult<()> {
        if self.txn.is_some() {
            return self.write_batch(&batch);
        }
        let started = Instant::now();
        let result = match self.table.bind_write_batch(batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        self.finish_storage_write("owned batch write async", started, result)
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
                return self.finish_storage_write(
                    "transaction compare and write",
                    started,
                    result,
                );
            }
            Ok(None) => {}
            Err(error) => {
                return self.finish_storage_write(
                    "access transaction for compare and write",
                    started,
                    Err(error),
                );
            }
        }
        let result = match bind_write_batch(&self.table, batch) {
            Ok(table_batch) => match conditions
                .iter()
                .map(|condition| {
                    condition.engine.clone().ok_or_else(|| {
                        Status::InvalidArgument(
                            "transaction observation cannot be used outside its transaction"
                                .to_string(),
                        )
                    })
                })
                .collect::<KvResult<Vec<_>>>()
            {
                Ok(engine_conditions) => {
                    self.table
                        .compare_and_write_async(
                            engine_conditions,
                            table_batch,
                            self.write_options.clone(),
                        )
                        .await
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        self.finish_storage_write("compare and write async", started, result)
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
                return self.finish_storage_write(
                    "transaction compare and write",
                    started,
                    result,
                );
            }
            Ok(None) => {}
            Err(error) => {
                return self.finish_storage_write(
                    "access transaction for compare and write",
                    started,
                    Err(error),
                );
            }
        }
        let result = (|| {
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
            self.table.compare_and_write(
                engine_conditions,
                table_batch,
                self.write_options.clone(),
            )
        })();
        self.finish_storage_write("compare and write", started, result)
    }

    /// 直接提交到底层 DB，绕过当前事务视图。
    pub fn write_batch_direct(&self, batch: &WriteBatch) -> KvResult<()> {
        let started = Instant::now();
        let result = bind_write_batch(&self.table, batch)
            .and_then(|table_batch| self.table.write(table_batch, self.write_options.clone()));
        self.finish_storage_write("direct batch write", started, result)
    }

    pub async fn write_batch_direct_async(&self, batch: WriteBatch) -> KvResult<()> {
        let started = Instant::now();
        let result = match self.table.bind_write_batch(batch) {
            Ok(table_batch) => self
                .table
                .write_async(table_batch, self.write_options.clone())
                .await,
            Err(error) => Err(error),
        };
        self.finish_storage_write("direct batch write async", started, result)
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
