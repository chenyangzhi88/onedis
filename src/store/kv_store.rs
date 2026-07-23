use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use common::types::options::Options;
use common::types::status::{Result as KvResult, Status};
use common::types::write_batch::{WriteBatch, WriteType};
use kv_engine::{
    api::{
        DbImpl, KeyRange, KvBatch, KvProjection, KvScanCursor, KvScanRequest,
        ObservedKeyState as EngineObservedKeyState, ObservedKvValue as EngineObservedKvValue,
        SchemalessCompareCondition as EngineCompareCondition, SchemalessTable,
        SchemalessTableOptions, SchemalessTransaction, SchemalessWriteBatch,
    },
    function::MergeOperate,
};

use crate::observability::metrics::{elapsed_us, global_metrics};

fn trace_lrange_scan_sample() -> Option<u64> {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    static COUNT: AtomicU64 = AtomicU64::new(0);
    if !*ENABLED.get_or_init(|| std::env::var_os("ONEDIS_LRANGE_TRACE").is_some()) {
        return None;
    }
    let count = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    (count <= 20 || count.is_multiple_of(1000)).then_some(count)
}

/// `onedis` 使用的底层 KV 存储封装。
///
/// 内部持有 `Arc<DbImpl>`，天然线程安全，所有方法只需 `&self`。
#[derive(Clone)]
pub struct KvStore {
    db: Arc<DbImpl>,
    table: SchemalessTable,
    table_name: Arc<str>,
    txn: Option<Arc<KvStoreTransactionContext>>,
}

struct KvStoreTransactionContext {
    txns: Mutex<Option<BTreeMap<String, SchemalessTransaction>>>,
}

#[derive(Clone, Debug)]
enum ExpectedRawState {
    Value(Option<Vec<u8>>),
    Exists(bool),
}

/// A compare condition that can be evaluated by either a standalone table or an onedis
/// transaction. The engine condition carries its opaque observation token when one exists.
#[derive(Clone, Debug)]
pub struct CompareCondition {
    key: Vec<u8>,
    expected: ExpectedRawState,
    engine: Option<EngineCompareCondition>,
}

impl CompareCondition {
    pub fn with_expected<K: AsRef<[u8]>>(key: K, expected: Option<Vec<u8>>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            expected: ExpectedRawState::Value(expected.clone()),
            engine: Some(EngineCompareCondition::with_expected(key, expected)),
        }
    }

    pub fn exists_with<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: K, value: V) -> Self {
        Self::with_expected(key, Some(value.as_ref().to_vec()))
    }

    pub fn absent<K: AsRef<[u8]>>(key: K) -> Self {
        Self::with_expected(key, None)
    }

    pub fn from_observed(observed: &ObservedRawValue) -> Self {
        observed.condition.clone()
    }

    pub fn from_observed_state(observed: &ObservedRawKeyState) -> Self {
        observed.condition.clone()
    }

    fn matches_transaction_value(&self, value: Option<&[u8]>) -> bool {
        match &self.expected {
            ExpectedRawState::Value(expected) => expected.as_deref() == value,
            ExpectedRawState::Exists(expected) => *expected == value.is_some(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObservedRawValue {
    value: Option<Bytes>,
    condition: CompareCondition,
}

impl ObservedRawValue {
    fn from_engine(key: &[u8], observed: EngineObservedKvValue) -> Self {
        let value = observed.value().cloned();
        Self {
            condition: CompareCondition {
                key: key.to_vec(),
                expected: ExpectedRawState::Value(value.as_ref().map(|value| value.to_vec())),
                engine: Some(observed.condition()),
            },
            value,
        }
    }

    fn from_transaction(key: &[u8], value: Option<Bytes>) -> Self {
        Self {
            condition: CompareCondition {
                key: key.to_vec(),
                expected: ExpectedRawState::Value(value.as_ref().map(|value| value.to_vec())),
                engine: None,
            },
            value,
        }
    }

    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    pub fn into_value(self) -> Option<Bytes> {
        self.value
    }

    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    pub fn condition(&self) -> CompareCondition {
        self.condition.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ObservedRawKeyState {
    exists: bool,
    condition: CompareCondition,
}

impl ObservedRawKeyState {
    fn from_engine(key: &[u8], observed: EngineObservedKeyState) -> Self {
        let exists = observed.exists();
        Self {
            condition: CompareCondition {
                key: key.to_vec(),
                expected: ExpectedRawState::Exists(exists),
                engine: Some(observed.condition()),
            },
            exists,
        }
    }

    fn from_transaction(key: &[u8], exists: bool) -> Self {
        Self {
            condition: CompareCondition {
                key: key.to_vec(),
                expected: ExpectedRawState::Exists(exists),
                engine: None,
            },
            exists,
        }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn condition(&self) -> CompareCondition {
        self.condition.clone()
    }
}

include!("kv_store_lifecycle.rs");
include!("kv_store_raw_writes.rs");
include!("kv_store_raw_reads.rs");
include!("kv_store_scans.rs");
include!("kv_store_batch_writes.rs");
include!("kv_store_merge_operator.rs");
include!("kv_store_iterator_helpers.rs");
include!("kv_store_tests.rs");
