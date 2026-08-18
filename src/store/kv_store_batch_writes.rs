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
        let table_batch = bind_write_batch(&self.table, batch)
            .expect("failed to bind batch to kv_engine table");
        self.table
            .write(table_batch, self.write_options.clone())
            .expect("failed to write batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn write_batch_async(&self, batch: &WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(batch);
            return;
        }
        let started = Instant::now();
        let table_batch = bind_write_batch(&self.table, batch)
            .expect("failed to bind batch to kv_engine table");
        self.table
            .write_async(table_batch, self.write_options.clone())
            .await
            .expect("failed to write batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn write_batch_owned_async(&self, batch: WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(&batch);
            return;
        }
        let started = Instant::now();
        let table_batch = self
            .table
            .bind_write_batch(batch)
            .expect("failed to bind owned batch to kv_engine table");
        self.table
            .write_async(table_batch, self.write_options.clone())
            .await
            .expect("failed to write owned batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn compare_and_write_batch_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> KvResult<()> {
        let started = Instant::now();
        if let Some(result) = self.with_transaction_mut(|txn| {
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
            global_metrics().record_storage_write(elapsed_us(started), result.is_err());
            return result;
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
        if let Some(result) = self.with_transaction_mut(|txn| {
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
            global_metrics().record_storage_write(elapsed_us(started), result.is_err());
            return result;
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
        let table_batch = bind_write_batch(&self.table, batch)
            .expect("failed to bind direct batch to kv_engine table");
        self.table
            .write(table_batch, self.write_options.clone())
            .expect("failed to write direct batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }

    pub async fn write_batch_direct_async(&self, batch: WriteBatch) {
        let started = Instant::now();
        let table_batch = self
            .table
            .bind_write_batch(batch)
            .expect("failed to bind direct batch to kv_engine table");
        self.table
            .write_async(table_batch, self.write_options.clone())
            .await
            .expect("failed to write direct batch into kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
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
