impl KvStore {
    pub fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        let started = Instant::now();
        if let Some(value) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(|value| value.to_vec())
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = self
            .table
            .get(key)
            .expect("failed to read key from kv_engine")
            .map(|value| value.to_vec());
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_async(&self, key: &[u8]) -> Option<Vec<u8>> {
        let started = Instant::now();
        if let Some(value) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(|value| value.to_vec())
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = self
            .table
            .get_async(key)
            .await
            .expect("failed to read key from kv_engine")
            .map(|value| value.to_vec());
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_observed_async(&self, key: &[u8]) -> ObservedRawValue {
        let started = Instant::now();
        if let Some(value) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
        }) {
            let observed = ObservedRawValue::from_transaction(key, value);
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = self
            .table
            .get_observed_async(key)
            .await
            .expect("failed to read observed key from kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        ObservedRawValue::from_engine(key, observed)
    }

    pub async fn observe_raw_key_state_async(&self, key: &[u8]) -> ObservedRawKeyState {
        let started = Instant::now();
        if let Some(exists) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .is_some()
        }) {
            let observed = ObservedRawKeyState::from_transaction(key, exists);
            global_metrics().record_storage_read(elapsed_us(started));
            return observed;
        }
        let observed = self
            .table
            .observe_key_state_async(key)
            .await
            .expect("failed to observe key state from kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        ObservedRawKeyState::from_engine(key, observed)
    }

    /// 直接从 kv_engine 读取原始 value，尽量保留底层返回的 Bytes，减少只读热路径拷贝。
    pub fn get_raw_bytes(&self, key: &[u8]) -> Option<Bytes> {
        let started = Instant::now();
        if let Some(value) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(Bytes::from)
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = self
            .table
            .get(key)
            .expect("failed to read key from kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    pub async fn get_raw_bytes_async(&self, key: &[u8]) -> Option<Bytes> {
        let started = Instant::now();
        if let Some(value) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(Bytes::from)
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return value;
        }
        let value = self
            .table
            .get_async(key)
            .await
            .expect("failed to read key from kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        value
    }

    /// 批量读取原始 value，用于批量命令避免逐 key 往返底层存储。
    pub fn multi_get_raw(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let started = Instant::now();
        if let Some(values) = self.with_transaction_mut(|txn| {
            txn.multi_get(keys)
                .expect("failed to read keys from kv_engine transaction")
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return values;
        }
        let values = self
            .table
            .multi_get(keys)
            .expect("failed to read keys from kv_engine")
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
        if let Some(values) = self.with_transaction_mut(|txn| {
            txn.multi_get(keys)
                .expect("failed to read keys from kv_engine transaction")
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return values;
        }
        let values = self
            .table
            .multi_get_async(keys)
            .await
            .expect("failed to read keys from kv_engine")
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect();
        global_metrics().record_storage_read(elapsed_us(started));
        values
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let started = Instant::now();
        if let Some(exists) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .is_some()
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return exists;
        }
        let exists = self
            .table
            .observe_key_state(key)
            .map(|state| state.exists())
            .expect("failed to check key existence in kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        exists
    }

    pub async fn contains_key_async(&self, key: &[u8]) -> bool {
        let started = Instant::now();
        if let Some(exists) = self.with_transaction_mut(|txn| {
            txn.get(key)
                .expect("failed to read key from kv_engine transaction")
                .is_some()
        }) {
            global_metrics().record_storage_read(elapsed_us(started));
            return exists;
        }
        let exists = self
            .table
            .observe_key_state_async(key)
            .await
            .map(|state| state.exists())
            .expect("failed to check key existence in kv_engine");
        global_metrics().record_storage_read(elapsed_us(started));
        exists
    }
}
