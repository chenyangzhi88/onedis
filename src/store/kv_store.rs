use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use common::types::options::Options;
use common::types::status::{Result as KvResult, Status};
use common::types::write_batch::{WriteBatch, WriteType};
use kv_engine::db::{
    DbImpl, DbIterator, IteratorOptions, MergeOperate, ObservedKeyState, ObservedKvValue,
    OptimisticTransactionDb, SchemalessCompareCondition as CompareCondition, SchemalessTable,
    SchemalessWriteBatch, Transaction, TransactionDB, TransactionOptions,
};

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
    txn_db: Arc<OptimisticTransactionDb>,
    txn: Option<Arc<Mutex<Option<Transaction>>>>,
}

include!("kv_store_lifecycle.rs");
include!("kv_store_raw_writes.rs");
include!("kv_store_raw_reads.rs");
include!("kv_store_scans.rs");
include!("kv_store_batch_writes.rs");
include!("kv_store_merge_operator.rs");
include!("kv_store_iterator_helpers.rs");
include!("kv_store_tests.rs");
