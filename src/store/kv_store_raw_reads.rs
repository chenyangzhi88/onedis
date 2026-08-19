impl KvStore {
    pub fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction get")
                    .map(|value| value.to_vec())
            }),
            || None,
            "access transaction for get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = storage_read_or(
            self.table.get(key, &ReadOptions::default()),
            || None,
            "get",
        )
        .map(|value| value.to_vec());
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_async(&self, key: &[u8]) -> Option<Vec<u8>> {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction get async")
                    .map(|value| value.to_vec())
            }),
            || None,
            "access transaction for async get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = storage_read_or(
            self.table.get_async(key, &ReadOptions::default()).await,
            || None,
            "get async",
        )
        .map(|value| value.to_vec());
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_observed_async(&self, key: &[u8]) -> ObservedRawValue {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction observed get")
            }),
            || None,
            "access transaction for observed get",
        ) {
            let observed = ObservedRawValue::from_transaction(key, value);
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = match self.table.get_observed_async(key).await {
            Ok(observed) => observed,
            Err(error) => {
                crate::store::health::storage_health().record_failure("observed get async", error);
                global_metrics().record_storage_read(elapsed_us(started));
                return ObservedRawValue::from_transaction(key, None);
            }
        };
        global_metrics().record_storage_read(elapsed_us(started));
        ObservedRawValue::from_engine(key, observed)
    }

    pub fn get_raw_observed(&self, key: &[u8]) -> ObservedRawValue {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction observed get")
            }),
            || None,
            "access transaction for observed get",
        ) {
            let observed = ObservedRawValue::from_transaction(key, value);
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = match self.table.get_observed(key) {
            Ok(observed) => observed,
            Err(error) => {
                crate::store::health::storage_health().record_failure("observed get", error);
                global_metrics().record_storage_read(elapsed_us(started));
                return ObservedRawValue::from_transaction(key, None);
            }
        };
        global_metrics().record_storage_read(elapsed_us(started));
        ObservedRawValue::from_engine(key, observed)
    }

    pub async fn observe_raw_key_state_async(&self, key: &[u8]) -> ObservedRawKeyState {
        let started = Instant::now();
        if let Some(exists) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction observe key state").is_some()
            }),
            || false,
            "access transaction for key observation",
        ) {
            let observed = ObservedRawKeyState::from_transaction(key, exists);
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = match self.table.observe_key_state_async(key).await {
            Ok(observed) => observed,
            Err(error) => {
                crate::store::health::storage_health().record_failure("observe key state", error);
                global_metrics().record_storage_read(elapsed_us(started));
                return ObservedRawKeyState::from_transaction(key, false);
            }
        };
        global_metrics().record_storage_read(elapsed_us(started));
        ObservedRawKeyState::from_engine(key, observed)
    }

    /// 直接从 kv_engine 读取原始 value，尽量保留底层返回的 Bytes，减少只读热路径拷贝。
    pub fn get_raw_bytes(&self, key: &[u8]) -> Option<Bytes> {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction get bytes")
            }),
            || None,
            "access transaction for byte get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = storage_read_or(
            self.table.get(key, &ReadOptions::default()),
            || None,
            "get bytes",
        );
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_bytes_async(&self, key: &[u8]) -> Option<Bytes> {
        let started = Instant::now();
        if let Some(value) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction get bytes async")
            }),
            || None,
            "access transaction for async byte get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = storage_read_or(
            self.table.get_async(key, &ReadOptions::default()).await,
            || None,
            "get bytes async",
        );
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    /// 批量读取原始 value，用于批量命令避免逐 key 往返底层存储。
    pub fn multi_get_raw(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let started = Instant::now();
        if let Some(values) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(
                    txn.multi_get(keys),
                    || vec![None; keys.len()],
                    "transaction multi get",
                )
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
            }),
            || vec![None; keys.len()],
            "access transaction for multi get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return values;
        }
        let values = storage_read_or(
            self.table.multi_get(keys, &ReadOptions::default()),
            || vec![None; keys.len()],
            "multi get",
        )
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect();
        global_metrics().record_storage_read(elapsed_us(started));
        values
    }

    pub async fn multi_get_raw_async(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let started = Instant::now();
        if let Some(values) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(
                    txn.multi_get(keys),
                    || vec![None; keys.len()],
                    "transaction multi get async",
                )
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
            }),
            || vec![None; keys.len()],
            "access transaction for async multi get",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return values;
        }
        let values = storage_read_or(
            self.table.multi_get_async(keys, &ReadOptions::default()).await,
            || vec![None; keys.len()],
            "multi get async",
        )
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect();
        global_metrics().record_storage_read(elapsed_us(started));
        values
    }

    /// Batch-read values together with reusable compare-and-write observation tokens.
    pub async fn multi_get_raw_observed_async(&self, keys: &[Vec<u8>]) -> Vec<ObservedRawValue> {
        if keys.is_empty() {
            return Vec::new();
        }
        let started = Instant::now();
        if let Some(values) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(
                    txn.multi_get(keys),
                    || vec![None; keys.len()],
                    "transaction multi observed get",
                )
            }),
            || vec![None; keys.len()],
            "access transaction for multi observed get",
        ) {
            let observed = keys
                .iter()
                .zip(values)
                .map(|(key, value)| ObservedRawValue::from_transaction(key, value))
                .collect();
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = match self.table.multi_get_observed_async(keys).await {
            Ok(observed) => observed
                .into_iter()
                .zip(keys)
                .map(|(observed, key)| ObservedRawValue::from_engine(key, observed))
                .collect(),
            Err(error) => {
                crate::store::health::storage_health()
                    .record_failure("multi observed get async", error);
                keys.iter()
                    .map(|key| ObservedRawValue::from_transaction(key, None))
                    .collect()
            }
        };
        global_metrics().record_storage_read(elapsed_us(started));
        observed
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let started = Instant::now();
        if let Some(exists) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction contains key").is_some()
            }),
            || false,
            "access transaction for contains key",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return exists;
        }
        let exists = storage_read_or(
            self.table
                .observe_key_state(key)
                .map(|state| state.exists()),
            || false,
            "contains key",
        );
        global_metrics().record_storage_read(elapsed_us(started));
        exists
    }

    pub async fn contains_key_async(&self, key: &[u8]) -> bool {
        let started = Instant::now();
        if let Some(exists) = self.transaction_access_or(
            self.with_transaction_mut(|txn| {
                storage_read_or(txn.get(key), || None, "transaction contains key async").is_some()
            }),
            || false,
            "access transaction for async contains key",
        ) {
            global_metrics().record_storage_read(elapsed_us(started));
            return exists;
        }
        let exists = storage_read_or(
            self.table
                .observe_key_state_async(key)
                .await
                .map(|state| state.exists()),
            || false,
            "contains key async",
        );
        global_metrics().record_storage_read(elapsed_us(started));
        exists
    }
}

fn storage_read_or<T>(
    result: KvResult<T>,
    fallback: impl FnOnce() -> T,
    operation: &str,
) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            crate::store::health::storage_health().record_failure(operation, error);
            fallback()
        }
    }
}
