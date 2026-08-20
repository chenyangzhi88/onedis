impl KvStore {
    fn finish_storage_read<T>(
        &self,
        operation: &str,
        started: Instant,
        result: KvResult<T>,
    ) -> KvResult<T> {
        global_metrics().record_storage_read(elapsed_us(started));
        if let Err(error) = &result {
            self.health.record_failure(operation, error);
        }
        result
    }

    pub fn get_raw(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let started = Instant::now();
        let result = (|| {
            if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
                return value.map(|value| value.map(|bytes| bytes.to_vec()));
            }
            self.table
                .get(key, &ReadOptions::default())
                .map(|value| value.map(|bytes| bytes.to_vec()))
        })();
        self.finish_storage_read("get", started, result)
    }

    pub async fn get_raw_async(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let started = Instant::now();
        let result = if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
            value.map(|value| value.map(|bytes| bytes.to_vec()))
        } else {
            self.table
                .get_async(key, &ReadOptions::default())
                .await
                .map(|value| value.map(|bytes| bytes.to_vec()))
        };
        self.finish_storage_read("get async", started, result)
    }

    pub async fn get_raw_observed_async(&self, key: &[u8]) -> KvResult<ObservedRawValue> {
        let started = Instant::now();
        let result = if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
            value.map(|value| ObservedRawValue::from_transaction(key, value))
        } else {
            self.table
                .get_observed_async(key)
                .await
                .map(|observed| ObservedRawValue::from_engine(key, observed))
        };
        self.finish_storage_read("observed get async", started, result)
    }

    pub fn get_raw_observed(&self, key: &[u8]) -> KvResult<ObservedRawValue> {
        let started = Instant::now();
        let result = (|| {
            if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
                return value.map(|value| ObservedRawValue::from_transaction(key, value));
            }
            self.table
                .get_observed(key)
                .map(|observed| ObservedRawValue::from_engine(key, observed))
        })();
        self.finish_storage_read("observed get", started, result)
    }

    pub async fn observe_raw_key_state_async(
        &self,
        key: &[u8],
    ) -> KvResult<ObservedRawKeyState> {
        let started = Instant::now();
        let result = if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
            value.map(|value| ObservedRawKeyState::from_transaction(key, value.is_some()))
        } else {
            self.table
                .observe_key_state_async(key)
                .await
                .map(|observed| ObservedRawKeyState::from_engine(key, observed))
        };
        self.finish_storage_read("observe key state", started, result)
    }

    /// Read the raw value while preserving the engine's `Bytes` allocation.
    pub fn get_raw_bytes(&self, key: &[u8]) -> KvResult<Option<Bytes>> {
        let started = Instant::now();
        let result = (|| {
            if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
                return value;
            }
            self.table.get(key, &ReadOptions::default())
        })();
        self.finish_storage_read("get bytes", started, result)
    }

    pub async fn get_raw_bytes_async(&self, key: &[u8]) -> KvResult<Option<Bytes>> {
        let started = Instant::now();
        let result = if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
            value
        } else {
            self.table.get_async(key, &ReadOptions::default()).await
        };
        self.finish_storage_read("get bytes async", started, result)
    }

    /// Batch-read raw values without hiding a partial or failed storage read.
    pub fn multi_get_raw(&self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let result = (|| {
            let values = if let Some(values) = self.with_transaction_mut(|txn| txn.multi_get(keys))?
            {
                values?
            } else {
                self.table.multi_get(keys, &ReadOptions::default())?
            };
            Ok(values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect())
        })();
        self.finish_storage_read("multi get", started, result)
    }

    pub async fn multi_get_raw_async(
        &self,
        keys: &[Vec<u8>],
    ) -> KvResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let result = if let Some(values) = self.with_transaction_mut(|txn| txn.multi_get(keys))? {
            values
        } else {
            self.table.multi_get_async(keys, &ReadOptions::default()).await
        }
        .map(|values| {
            values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
        });
        self.finish_storage_read("multi get async", started, result)
    }

    /// Batch-read values together with reusable compare-and-write observation tokens.
    pub async fn multi_get_raw_observed_async(
        &self,
        keys: &[Vec<u8>],
    ) -> KvResult<Vec<ObservedRawValue>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let result = if let Some(values) = self.with_transaction_mut(|txn| txn.multi_get(keys))? {
            values.map(|values| {
                keys.iter()
                    .zip(values)
                    .map(|(key, value)| ObservedRawValue::from_transaction(key, value))
                    .collect()
            })
        } else {
            self.table.multi_get_observed_async(keys).await.map(|observed| {
                observed
                    .into_iter()
                    .zip(keys)
                    .map(|(observed, key)| ObservedRawValue::from_engine(key, observed))
                    .collect()
            })
        };
        self.finish_storage_read("multi observed get async", started, result)
    }

    pub fn contains_key(&self, key: &[u8]) -> KvResult<bool> {
        let started = Instant::now();
        let result = (|| {
            if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
                return value.map(|value| value.is_some());
            }
            self.table.observe_key_state(key).map(|state| state.exists())
        })();
        self.finish_storage_read("contains key", started, result)
    }

    pub async fn contains_key_async(&self, key: &[u8]) -> KvResult<bool> {
        let started = Instant::now();
        let result = if let Some(value) = self.with_transaction_mut(|txn| txn.get(key))? {
            value.map(|value| value.is_some())
        } else {
            self.table
                .observe_key_state_async(key)
                .await
                .map(|state| state.exists())
        };
        self.finish_storage_read("contains key async", started, result)
    }
}
