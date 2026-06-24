impl KvStore {
    pub fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(|value| value.to_vec());
        }
        self.table
            .get(key)
            .expect("failed to read key from kv_engine")
            .map(|value| value.to_vec())
    }

    pub async fn get_raw_async(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(|value| value.to_vec());
        }
        self.table
            .get_async(key)
            .await
            .expect("failed to read key from kv_engine")
            .map(|value| value.to_vec())
    }

    pub async fn get_raw_observed_async(&self, key: &[u8]) -> ObservedKvValue {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            let value = txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(Bytes::from);
            return ObservedKvValue {
                value,
                value_seq: None,
                read_seq: u64::MAX,
            };
        }
        self.table
            .get_observed_async(key)
            .await
            .expect("failed to read observed key from kv_engine")
    }

    pub async fn observe_raw_key_state_async(&self, key: &[u8]) -> ObservedKeyState {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            let value = txn
                .get(key)
                .expect("failed to read key from kv_engine transaction");
            return ObservedKeyState {
                exists: value.is_some(),
                value_seq: None,
                read_seq: u64::MAX,
            };
        }
        self.table
            .observe_key_state_async(key)
            .await
            .expect("failed to observe key state from kv_engine")
    }

    /// 直接从 kv_engine 读取原始 value，尽量保留底层返回的 Bytes，减少只读热路径拷贝。
    pub fn get_raw_bytes(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(Bytes::from);
        }
        self.table
            .get(key)
            .expect("failed to read key from kv_engine")
    }

    pub async fn get_raw_bytes_async(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .map(Bytes::from);
        }
        self.table
            .get_async(key)
            .await
            .expect("failed to read key from kv_engine")
    }

    /// 批量读取原始 value，用于批量命令避免逐 key 往返底层存储。
    pub fn multi_get_raw(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .multi_get(keys)
                .expect("failed to read keys from kv_engine transaction")
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect();
        }
        self.table
            .multi_get(keys)
            .expect("failed to read keys from kv_engine")
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect()
    }

    pub async fn multi_get_raw_async(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .multi_get(keys)
                .expect("failed to read keys from kv_engine transaction")
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect();
        }
        self.table
            .multi_get_async(keys)
            .await
            .expect("failed to read keys from kv_engine")
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect()
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .is_some();
        }
        self.table
            .observe_key_state(key)
            .map(|state| state.exists)
            .expect("failed to check key existence in kv_engine")
    }

    pub async fn contains_key_async(&self, key: &[u8]) -> bool {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to read after transaction completion");
            return txn
                .get(key)
                .expect("failed to read key from kv_engine transaction")
                .is_some();
        }
        self.table
            .observe_key_state_async(key)
            .await
            .map(|state| state.exists)
            .expect("failed to check key existence in kv_engine")
    }
}
