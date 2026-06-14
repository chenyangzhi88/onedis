use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use common::types::options::Options;
use common::types::status::{Result as KvResult, Status};
use common::types::write_batch::{WriteBatch, WriteType};
use kv_engine::db::{
    CompareCondition, DB, DbExt, DbImpl, DbIterator, IteratorOptions, MergeOperate,
    ObservedKeyState, ObservedKvValue, OptimisticTransactionDb, Transaction, TransactionDB,
    TransactionOptions,
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
    txn_db: Arc<OptimisticTransactionDb>,
    txn: Option<Arc<Mutex<Option<Transaction>>>>,
}

impl KvStore {
    pub fn open(options: Options) -> Self {
        let db = DbImpl::open_with_merge_operator(
            Arc::new(options),
            Some(Arc::new(OnedisIntegerMergeOperator)),
        )
        .expect("failed to open kv_engine for onedis");
        let txn_db = Arc::new(OptimisticTransactionDb::new(db.clone()));
        KvStore {
            db,
            txn_db,
            txn: None,
        }
    }

    pub fn new<P: AsRef<Path>>(db_path: P, wal_dir: P, engine_id: u32) -> Self {
        let mut options = Options::default();
        options.db_path = db_path.as_ref().to_path_buf();
        options.wal_dir = wal_dir.as_ref().to_path_buf();
        options.engine_id = engine_id;
        Self::open(options)
    }

    pub fn begin_transaction(&self) -> anyhow::Result<Self> {
        let txn = self
            .txn_db
            .clone()
            .begin_transaction(&TransactionOptions::default())?;
        Ok(KvStore {
            db: self.db.clone(),
            txn_db: self.txn_db.clone(),
            txn: Some(Arc::new(Mutex::new(Some(txn)))),
        })
    }

    pub fn non_transactional_view(&self) -> Self {
        KvStore {
            db: self.db.clone(),
            txn_db: self.txn_db.clone(),
            txn: None,
        }
    }

    pub fn commit_transaction(&self) -> anyhow::Result<()> {
        let Some(txn) = &self.txn else {
            return Ok(());
        };
        let mut guard = txn.lock().expect("transaction mutex poisoned");
        let Some(txn) = guard.take() else {
            return Ok(());
        };
        txn.commit()
            .map_err(|err| anyhow::Error::msg(err.to_string()))
    }

    pub fn discard_transaction(&self) {
        let Some(txn) = &self.txn else {
            return;
        };
        let mut guard = txn.lock().expect("transaction mutex poisoned");
        let _ = guard.take();
    }

    pub async fn commit_transaction_async(&self) -> anyhow::Result<()> {
        let Some(txn) = &self.txn else {
            return Ok(());
        };
        let txn = {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            guard.take()
        };
        let Some(txn) = txn else {
            return Ok(());
        };
        txn.commit_async()
            .await
            .map_err(|err| anyhow::Error::msg(err.to_string()))
    }

    pub fn is_transactional(&self) -> bool {
        self.txn.is_some()
    }

    pub fn db(&self) -> Arc<DbImpl> {
        self.db.clone()
    }

    pub fn merge_raw(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw is only supported on non-transactional onedis stores");
        }
        self.db
            .merge(key, operand)
            .expect("failed to merge key into kv_engine");
    }

    pub async fn merge_raw_async(&self, key: &[u8], operand: &[u8]) {
        if self.txn.is_some() {
            panic!("merge_raw_async is only supported on non-transactional onedis stores");
        }
        self.db
            .merge_async(key, operand)
            .await
            .expect("failed to merge key into kv_engine");
    }

    /// 直接把原始 key/value 写入 kv_engine。
    pub fn put_raw(&self, key: &[u8], value: &[u8]) {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to write after transaction completion");
            txn.put(key, value)
                .expect("failed to stage key into kv_engine transaction");
            return;
        }
        self.db
            .put(key, value)
            .expect("failed to write key into kv_engine");
    }

    pub fn blob_put_raw(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw is only supported on non-transactional onedis stores");
        }
        self.db
            .blob_put(key, value)
            .expect("failed to write blob key into kv_engine");
    }

    pub async fn blob_put_raw_async(&self, key: &[u8], value: &[u8]) {
        if self.txn.is_some() {
            panic!("blob_put_raw_async is only supported on non-transactional onedis stores");
        }
        self.db
            .blob_put_async(key, value)
            .await
            .expect("failed to write blob key into kv_engine");
    }

    /// 直接从 kv_engine 读取原始 value。
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
        self.db
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
        self.db
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
        self.db
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
        self.db
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
        self.db.get(key).expect("failed to read key from kv_engine")
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
        self.db
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
        self.db
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
        self.db
            .multi_get_async(keys)
            .await
            .expect("failed to read keys from kv_engine")
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect()
    }

    /// 直接从 kv_engine 删除 key，返回删除前是否存在。
    pub fn delete_key(&self, key: &[u8]) -> bool {
        let existed = self.contains_key(key);
        if existed {
            if let Some(txn) = &self.txn {
                let mut guard = txn.lock().expect("transaction mutex poisoned");
                let txn = guard
                    .as_mut()
                    .expect("attempted to delete after transaction completion");
                txn.delete(key)
                    .expect("failed to stage delete into kv_engine transaction");
            } else {
                self.db
                    .delete(key)
                    .expect("failed to delete key from kv_engine");
            }
        }
        existed
    }

    /// 直接调用 kv_engine 检查 key 是否存在。
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
        self.db
            .contains_key(key)
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
        self.db
            .contains_key_async(key)
            .await
            .expect("failed to check key existence in kv_engine")
    }

    /// 基于 kv_engine 的 prefix scan 返回原始 key/value 对。
    pub fn scan_prefix_raw(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to scan after transaction completion");
            let opts = IteratorOptions {
                lower_bound: Some(prefix.to_vec()),
                upper_bound: prefix_exclusive_upper_bound(prefix),
                ..IteratorOptions::default()
            };
            let mut iter = txn
                .new_iterator_with_opts(&opts)
                .expect("failed to create kv_engine transaction prefix iterator");
            return collect_iterator(&mut *iter);
        }
        let mut iter = self
            .db
            .scan_prefix(prefix)
            .expect("failed to create kv_engine prefix iterator");
        iter.seek_to_first()
            .expect("failed to seek kv_engine prefix iterator");

        let mut entries = Vec::new();
        while let Some((key, value)) = iter.next_ref() {
            entries.push((key.to_vec(), value.to_vec()));
        }
        entries
    }

    pub async fn scan_prefix_raw_async(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        if let Some(txn_cell) = &self.txn {
            let opts = IteratorOptions {
                lower_bound: Some(prefix.to_vec()),
                upper_bound: prefix_exclusive_upper_bound(prefix),
                ..IteratorOptions::default()
            };
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            let entries = match iter_result {
                Ok(mut iter) => collect_iterator_async(&mut *iter, usize::MAX).await,
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction prefix iterator: {err}");
                }
            };
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return entries;
        }
        let mut iter = self
            .db
            .scan_prefix_async(prefix)
            .await
            .expect("failed to create kv_engine async prefix iterator");
        iter.seek_to_first()
            .expect("failed to seek kv_engine async prefix iterator");
        collect_iterator_async(&mut *iter, usize::MAX).await
    }

    /// Scan a bounded raw range and stop after `limit` entries.
    pub fn scan_range_raw_limited(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to scan after transaction completion");
            let new_iter_started_at = trace_id.map(|_| Instant::now());
            let mut iter = txn
                .new_iterator_with_opts(&opts)
                .expect("failed to create kv_engine transaction range iterator");
            let new_iter_us = new_iter_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_iterator_limited(&mut *iter, limit);
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=true limit={} entries={} lower_len={} upper_len={} new_iter_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.len(),
                    lower_bound.len(),
                    opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                    new_iter_us.unwrap_or_default(),
                    collect_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            return entries;
        }
        let new_iter_started_at = trace_id.map(|_| Instant::now());
        let mut iter = self
            .db
            .new_iterator(&opts)
            .expect("failed to create kv_engine range iterator");
        let new_iter_us = new_iter_started_at.map(|started| started.elapsed().as_micros());
        let collect_started_at = trace_id.map(|_| Instant::now());
        let entries = collect_iterator_limited(&mut *iter, limit);
        if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
            eprintln!(
                "lrange-trace kv_scan sample={} txn=false limit={} entries={} lower_len={} upper_len={} new_iter_us={} collect_us={} total_us={}",
                trace_id,
                limit,
                entries.len(),
                lower_bound.len(),
                opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                new_iter_us.unwrap_or_default(),
                collect_started_at
                    .map(|started| started.elapsed().as_micros())
                    .unwrap_or_default(),
                total_started_at.elapsed().as_micros(),
            );
        }
        entries
    }

    pub async fn scan_range_raw_limited_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        if let Some(txn_cell) = &self.txn {
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            let entries = match iter_result {
                Ok(mut iter) => collect_iterator_async(&mut *iter, limit).await,
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction range iterator: {err}");
                }
            };
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return entries;
        }
        let mut iter = self
            .db
            .new_iterator_async(&opts)
            .await
            .expect("failed to create kv_engine async range iterator");
        collect_iterator_async(&mut *iter, limit).await
    }

    pub async fn scan_range_raw_visit_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        if limit == 0 {
            return 0;
        }
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut limited_visitor = |key: &[u8], value: &[u8]| {
            if seen >= limit {
                return false;
            }
            seen += 1;
            visitor(key, value) && seen < limit
        };
        if let Some(txn_cell) = &self.txn {
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            match iter_result {
                Ok(mut iter) => {
                    let scan_started_at = trace_id.map(|_| Instant::now());
                    iter.scan_ref_async(&mut limited_visitor)
                        .await
                        .expect("failed to advance kv_engine async transaction range iterator");
                    if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                        eprintln!(
                            "lrange-trace kv_visit sample={} txn=true limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                            trace_id,
                            limit,
                            seen,
                            lower_bound.len(),
                            opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                            scan_started_at
                                .map(|started| started.elapsed().as_micros())
                                .unwrap_or_default(),
                            total_started_at.elapsed().as_micros(),
                        );
                    }
                }
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction range iterator: {err}");
                }
            }
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return seen;
        }
        let mut iter = self
            .db
            .new_iterator_async(&opts)
            .await
            .expect("failed to create kv_engine async range iterator");
        let scan_started_at = trace_id.map(|_| Instant::now());
        iter.scan_ref_async(&mut limited_visitor)
            .await
            .expect("failed to advance kv_engine async range iterator");
        if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
            eprintln!(
                "lrange-trace kv_visit sample={} txn=false limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                trace_id,
                limit,
                seen,
                lower_bound.len(),
                opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                scan_started_at
                    .map(|started| started.elapsed().as_micros())
                    .unwrap_or_default(),
                total_started_at.elapsed().as_micros(),
            );
        }
        seen
    }

    /// 范围删除 [start, end)，用于批量清理 sub-keys。
    pub fn delete_range(&self, start: &[u8], end: &[u8]) {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to delete range after transaction completion");
            txn.delete_range(start, end)
                .expect("failed to stage delete_range into kv_engine transaction");
            return;
        }
        self.db
            .delete_range(start, end)
            .expect("failed to delete_range in kv_engine");
    }

    /// 原子提交一组底层写操作。
    pub fn write_batch(&self, batch: &WriteBatch) {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to write batch after transaction completion");
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
            return;
        }
        self.db
            .write(batch)
            .expect("failed to write batch into kv_engine");
    }

    pub async fn write_batch_async(&self, batch: &WriteBatch) {
        if self.txn.is_some() {
            self.write_batch(batch);
            return;
        }
        self.db
            .write_async(batch)
            .await
            .expect("failed to write batch into kv_engine");
    }

    pub async fn compare_and_write_batch_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> KvResult<()> {
        if self.txn.is_some() {
            self.write_batch(batch);
            return Ok(());
        }
        self.db.compare_and_write_async(conditions, batch).await
    }

    /// 直接提交到底层 DB，绕过当前事务视图。
    ///
    /// Version high-water reservations intentionally use this path: gaps are
    /// safe, but the reserved high-water mark must be durable before any
    /// transaction can publish keys using those versions.
    pub fn write_batch_direct(&self, batch: &WriteBatch) {
        self.db
            .write(batch)
            .expect("failed to write direct batch into kv_engine");
    }

    pub async fn write_batch_direct_async(&self, batch: WriteBatch) {
        self.db
            .write_owned_async(batch)
            .await
            .expect("failed to write direct batch into kv_engine");
    }
}

#[derive(Debug)]
struct OnedisIntegerMergeOperator;

impl OnedisIntegerMergeOperator {
    const TYPE_STRING: u8 = 1;

    fn decode_operand(bytes: &[u8], context: &str) -> KvResult<i64> {
        let array: [u8; 8] = bytes.try_into().map_err(|_| {
            Status::InvalidArgument(format!("{context} must be an 8-byte big-endian i64"))
        })?;
        Ok(i64::from_be_bytes(array))
    }

    fn decode_existing(bytes: &[u8]) -> KvResult<(u64, i64)> {
        if bytes.len() < 17 || bytes[16] != Self::TYPE_STRING {
            return Err(Status::InvalidArgument(
                "existing value is not an onedis string".to_string(),
            ));
        }
        let expire_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let text = std::str::from_utf8(&bytes[17..]).map_err(|_| {
            Status::InvalidArgument("existing string value is not valid UTF-8".to_string())
        })?;
        let value = text.parse::<i64>().map_err(|_| {
            Status::InvalidArgument("existing string value is not an integer".to_string())
        })?;
        Ok((expire_ms, value))
    }

    fn encode_string(value: i64, expire_ms: u64) -> Vec<u8> {
        let value = value.to_string();
        let mut encoded = Vec::with_capacity(17 + value.len());
        encoded.extend_from_slice(&expire_ms.to_be_bytes());
        encoded.extend_from_slice(&0u64.to_be_bytes());
        encoded.push(Self::TYPE_STRING);
        encoded.extend_from_slice(value.as_bytes());
        encoded
    }
}

impl MergeOperate for OnedisIntegerMergeOperator {
    fn name(&self) -> &str {
        "onedis_integer"
    }

    fn full_merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> KvResult<Option<Vec<u8>>> {
        let (expire_ms, mut value) = match existing_value {
            Some(existing) => Self::decode_existing(existing)?,
            None => (0, 0),
        };
        for operand in operands {
            let delta = Self::decode_operand(operand, "merge operand")?;
            value = value.checked_add(delta).ok_or_else(|| {
                Status::InvalidArgument("integer merge would overflow".to_string())
            })?;
        }
        Ok(Some(Self::encode_string(value, expire_ms)))
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> KvResult<Vec<u8>> {
        let left = Self::decode_operand(left, "left merge operand")?;
        let right = Self::decode_operand(right, "right merge operand")?;
        let merged = left.checked_add(right).ok_or_else(|| {
            Status::InvalidArgument("integer merge operand would overflow".to_string())
        })?;
        Ok(merged.to_be_bytes().to_vec())
    }
}

fn collect_iterator(iter: &mut dyn DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.seek_to_first()
        .expect("failed to seek kv_engine iterator");
    let mut entries = Vec::new();
    iter.scan_ref(&mut |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        true
    })
    .expect("failed to advance kv_engine iterator");
    entries
}

fn collect_iterator_limited(iter: &mut dyn DbIterator, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.seek_to_first()
        .expect("failed to seek kv_engine iterator");
    let mut entries = Vec::new();
    iter.scan_ref(&mut |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        entries.len() < limit
    })
    .expect("failed to advance kv_engine iterator");
    entries
}

async fn collect_iterator_async(
    iter: &mut dyn DbIterator,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    if limit == 0 {
        return entries;
    }
    while let Some((key, value)) = iter
        .next_async()
        .await
        .expect("failed to advance kv_engine async iterator")
    {
        entries.push((key.to_vec(), value.to_vec()));
        if entries.len() >= limit {
            break;
        }
    }
    entries
}

fn prefix_exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper_bound = prefix.to_vec();
    for idx in (0..upper_bound.len()).rev() {
        if upper_bound[idx] != u8::MAX {
            upper_bound[idx] += 1;
            upper_bound.truncate(idx + 1);
            return Some(upper_bound);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_engine::db::CompareCondition;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_store() -> KvStore {
        let unique = format!(
            "onedis-kv-store-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target"))
            .join("onedis-test-data")
            .join(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        KvStore::new(db_path, wal_dir, 1)
    }

    #[test]
    fn test_put_get_delete() {
        let store = test_store();
        store.put_raw(b"key1", b"val1");
        assert_eq!(store.get_raw(b"key1"), Some(b"val1".to_vec()));
        assert!(store.delete_key(b"key1"));
        assert_eq!(store.get_raw(b"key1"), None);
    }

    #[test]
    fn test_write_batch_atomic() {
        let store = test_store();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        store.write_batch(&batch);
        assert_eq!(store.get_raw(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get_raw(b"b"), Some(b"2".to_vec()));
    }

    #[tokio::test]
    async fn async_raw_blob_observe_multi_get_and_compare_write_paths_work() {
        let store = test_store();

        assert_eq!(store.multi_get_raw(&[]), Vec::<Option<Vec<u8>>>::new());
        assert_eq!(
            store.multi_get_raw_async(&[]).await,
            Vec::<Option<Vec<u8>>>::new()
        );

        store.put_raw(b"a", b"1");
        store.put_raw(b"b", b"2");
        store.blob_put_raw(b"blob:sync", b"blob-value");
        store.blob_put_raw_async(b"blob:async", b"blob-async").await;

        assert_eq!(store.get_raw_async(b"a").await, Some(b"1".to_vec()));
        assert_eq!(store.get_raw_bytes(b"b").unwrap().as_ref(), b"2");
        assert_eq!(
            store
                .get_raw_bytes_async(b"blob:async")
                .await
                .unwrap()
                .as_ref(),
            b"blob-async"
        );
        assert!(store.contains_key(b"blob:sync"));
        assert!(store.contains_key_async(b"blob:async").await);

        let observed = store.get_raw_observed_async(b"a").await;
        assert_eq!(observed.value.as_deref(), Some(&b"1"[..]));
        assert!(observed.read_seq > 0);
        let state = store.observe_raw_key_state_async(b"missing").await;
        assert!(!state.exists);
        assert!(state.read_seq > 0);

        let keys = vec![b"a".to_vec(), b"missing".to_vec(), b"b".to_vec()];
        assert_eq!(
            store.multi_get_raw(&keys),
            vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
        );
        assert_eq!(
            store.multi_get_raw_async(&keys).await,
            vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
        );

        let mut ok_batch = WriteBatch::new();
        ok_batch.put(b"cas", b"ok");
        store
            .compare_and_write_batch_async(
                &[CompareCondition::with_expected(
                    b"a".to_vec(),
                    Some(b"1".to_vec()),
                )],
                &ok_batch,
            )
            .await
            .unwrap();
        assert_eq!(store.get_raw(b"cas"), Some(b"ok".to_vec()));

        let mut failed_batch = WriteBatch::new();
        failed_batch.put(b"cas", b"bad");
        assert!(
            store
                .compare_and_write_batch_async(
                    &[CompareCondition::with_expected(
                        b"a".to_vec(),
                        Some(b"wrong".to_vec())
                    )],
                    &failed_batch,
                )
                .await
                .is_err()
        );
        assert_eq!(store.get_raw(b"cas"), Some(b"ok".to_vec()));
    }

    #[tokio::test]
    async fn range_scan_visit_delete_range_and_direct_batches_cover_sync_and_async() {
        let store = test_store();
        for idx in 0..5 {
            store.put_raw(format!("p:{idx}").as_bytes(), format!("v{idx}").as_bytes());
        }
        store.put_raw(b"q:0", b"out");

        let scan = store.scan_prefix_raw(b"p:");
        assert_eq!(scan.len(), 5);
        assert!(scan.iter().all(|(key, _)| key.starts_with(b"p:")));

        let async_scan = store.scan_prefix_raw_async(b"p:").await;
        assert_eq!(async_scan.len(), 5);

        let limited = store.scan_range_raw_limited(b"p:", Some(b"p;".to_vec()), 3);
        assert_eq!(limited.len(), 3);
        assert_eq!(
            store
                .scan_range_raw_limited_async(b"p:", Some(b"p;".to_vec()), 2)
                .await
                .len(),
            2
        );

        let visited = store
            .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 10, |key, _| key != b"p:2")
            .await;
        assert_eq!(visited, 3);
        assert_eq!(
            store
                .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 0, |_, _| true)
                .await,
            0
        );

        let mut direct = WriteBatch::new();
        direct.put(b"direct:sync", b"1");
        store.write_batch_direct(&direct);
        assert_eq!(store.get_raw(b"direct:sync"), Some(b"1".to_vec()));

        let mut direct_async = WriteBatch::new();
        direct_async.put(b"direct:async", b"2");
        store.write_batch_direct_async(direct_async).await;
        assert_eq!(store.get_raw(b"direct:async"), Some(b"2".to_vec()));

        let mut async_batch = WriteBatch::new();
        async_batch.put(b"async:put", b"3");
        async_batch.delete(b"direct:sync");
        store.write_batch_async(&async_batch).await;
        assert_eq!(store.get_raw(b"async:put"), Some(b"3".to_vec()));
        assert_eq!(store.get_raw(b"direct:sync"), None);

        store.delete_range(b"p:", b"p;");
        assert!(store.scan_prefix_raw(b"p:").is_empty());
        assert_eq!(store.get_raw(b"q:0"), Some(b"out".to_vec()));
        assert!(!store.delete_key(b"missing"));
    }

    #[tokio::test]
    async fn transaction_commit_discard_scan_and_batch_paths_work() {
        let store = test_store();
        store.put_raw(b"base", b"old");

        let txn = store.begin_transaction().unwrap();
        assert!(txn.is_transactional());
        assert!(!store.is_transactional());
        txn.put_raw(b"base", b"new");
        txn.put_raw(b"txn:1", b"a");
        txn.put_raw(b"txn:2", b"b");
        assert_eq!(txn.get_raw(b"base"), Some(b"new".to_vec()));
        assert_eq!(store.get_raw(b"base"), Some(b"old".to_vec()));
        assert!(txn.contains_key(b"txn:1"));
        assert_eq!(
            txn.multi_get_raw(&[b"txn:1".to_vec(), b"missing".to_vec()]),
            vec![Some(b"a".to_vec()), None]
        );
        txn.commit_transaction().unwrap();
        txn.commit_transaction().unwrap();
        assert_eq!(store.get_raw(b"base"), Some(b"new".to_vec()));

        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"discarded", b"value");
        txn.discard_transaction();
        txn.discard_transaction();
        assert_eq!(store.get_raw(b"discarded"), None);

        let txn = store.begin_transaction().unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"batched", b"value");
        batch.delete(b"base");
        txn.write_batch(&batch);
        txn.commit_transaction_async().await.unwrap();
        txn.commit_transaction_async().await.unwrap();
        assert_eq!(store.get_raw(b"batched"), Some(b"value".to_vec()));
        assert_eq!(store.get_raw(b"base"), None);

        let view = txn.non_transactional_view();
        assert!(!view.is_transactional());
        assert_eq!(view.get_raw(b"batched"), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn transaction_async_read_observe_and_commit_paths_work() {
        let store = test_store();
        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"async:txn", b"value");
        assert_eq!(
            txn.get_raw_async(b"async:txn").await,
            Some(b"value".to_vec())
        );
        assert_eq!(
            txn.get_raw_bytes_async(b"async:txn")
                .await
                .unwrap()
                .as_ref(),
            b"value"
        );
        assert!(txn.contains_key_async(b"async:txn").await);
        assert_eq!(
            txn.multi_get_raw_async(&[b"async:txn".to_vec(), b"missing".to_vec()])
                .await,
            vec![Some(b"value".to_vec()), None]
        );
        let observed = txn.get_raw_observed_async(b"async:txn").await;
        assert_eq!(observed.value.as_deref(), Some(&b"value"[..]));
        assert_eq!(observed.read_seq, u64::MAX);
        assert!(txn.observe_raw_key_state_async(b"async:txn").await.exists);
        txn.commit_transaction_async().await.unwrap();
        assert_eq!(store.get_raw(b"async:txn"), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn transaction_async_scans_visits_delete_range_and_compare_write_are_isolated_until_commit()
     {
        let store = test_store();
        store.put_raw(b"txnscan:0", b"old");
        store.put_raw(b"txnscan:outside", b"outside");

        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"txnscan:0", b"v0");
        txn.put_raw(b"txnscan:1", b"v1");
        txn.put_raw(b"txnscan:2", b"v2");
        txn.put_raw(b"txnscan:stop", b"stop");

        let prefix_entries = txn.scan_prefix_raw_async(b"txnscan:").await;
        assert!(
            prefix_entries
                .iter()
                .any(|(key, value)| key == b"txnscan:1" && value == b"v1")
        );

        assert!(
            txn.scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 0)
                .is_empty()
        );
        let range_entries = txn.scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 2);
        assert_eq!(range_entries.len(), 2);
        let async_range_entries = txn
            .scan_range_raw_limited_async(b"txnscan:", Some(b"txnscan;".to_vec()), 3)
            .await;
        assert_eq!(async_range_entries.len(), 3);

        let visited = txn
            .scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 10, |key, _| {
                key != b"txnscan:stop"
            })
            .await;
        assert!(visited >= 4);
        assert_eq!(
            txn.scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 2, |_, _| {
                true
            })
            .await,
            2
        );

        let mut compare_batch = WriteBatch::new();
        compare_batch.put(b"txnscan:compare", b"ok");
        txn.compare_and_write_batch_async(
            &[CompareCondition::with_expected(
                b"txnscan:0".to_vec(),
                Some(b"v0".to_vec()),
            )],
            &compare_batch,
        )
        .await
        .unwrap();
        assert_eq!(txn.get_raw(b"txnscan:compare"), Some(b"ok".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:compare"), None);

        txn.delete_range(b"txnscan:1", b"txnscan:3");
        assert_eq!(txn.get_raw(b"txnscan:1"), None);
        assert_eq!(txn.get_raw(b"txnscan:2"), None);
        txn.commit_transaction_async().await.unwrap();

        assert_eq!(store.get_raw(b"txnscan:0"), Some(b"v0".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:1"), None);
        assert_eq!(store.get_raw(b"txnscan:2"), None);
        assert_eq!(store.get_raw(b"txnscan:compare"), Some(b"ok".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:outside"), Some(b"outside".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn raw_store_handles_concurrent_writes_and_integer_merge_paths() {
        let store = Arc::new(test_store());
        let wrote = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for task_id in 0..8 {
            let store = store.clone();
            let wrote = wrote.clone();
            tasks.push(tokio::spawn(async move {
                for item in 0..25 {
                    let key = format!("concurrent:{task_id}:{item}");
                    store.put_raw(key.as_bytes(), b"value");
                    assert!(store.contains_key_async(key.as_bytes()).await);
                    wrote.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(wrote.load(Ordering::Relaxed), 200);
        assert_eq!(store.scan_prefix_raw(b"concurrent:").len(), 200);

        store.merge_raw(b"counter", &5i64.to_be_bytes());
        store.merge_raw(b"counter", &7i64.to_be_bytes());
        store
            .merge_raw_async(b"counter", &(-2i64).to_be_bytes())
            .await;
        let encoded = store.get_raw(b"counter").unwrap();
        assert_eq!(encoded[0..8], 0u64.to_be_bytes());
        assert_eq!(encoded[16], OnedisIntegerMergeOperator::TYPE_STRING);
        assert_eq!(&encoded[17..], b"10");

        let mut existing = OnedisIntegerMergeOperator::encode_string(9, 12345);
        store.put_raw(b"counter:ttl", &existing);
        store.merge_raw(b"counter:ttl", &1i64.to_be_bytes());
        existing = store.get_raw(b"counter:ttl").unwrap();
        assert_eq!(
            u64::from_be_bytes(existing[0..8].try_into().unwrap()),
            12345
        );
        assert_eq!(&existing[17..], b"10");
    }

    #[test]
    fn prefix_bound_and_merge_operator_error_edges_are_covered() {
        assert_eq!(prefix_exclusive_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_exclusive_upper_bound(&[0xFF, 0xFF]), None);

        let op = OnedisIntegerMergeOperator;
        assert_eq!(op.name(), "onedis_integer");
        assert!(OnedisIntegerMergeOperator::decode_operand(b"short", "operand").is_err());
        assert!(OnedisIntegerMergeOperator::decode_existing(b"short").is_err());

        let mut wrong_type = OnedisIntegerMergeOperator::encode_string(1, 0);
        wrong_type[16] = 99;
        assert!(OnedisIntegerMergeOperator::decode_existing(&wrong_type).is_err());

        let mut invalid_utf8 = OnedisIntegerMergeOperator::encode_string(1, 0);
        invalid_utf8[17] = 0xFF;
        assert!(OnedisIntegerMergeOperator::decode_existing(&invalid_utf8).is_err());

        let mut not_integer = OnedisIntegerMergeOperator::encode_string(1, 0);
        not_integer.truncate(17);
        not_integer.extend_from_slice(b"nan");
        assert!(OnedisIntegerMergeOperator::decode_existing(&not_integer).is_err());

        assert!(
            op.partial_merge(b"k", &i64::MAX.to_be_bytes(), &1i64.to_be_bytes())
                .is_err()
        );
        assert!(
            op.full_merge(
                b"k",
                Some(&OnedisIntegerMergeOperator::encode_string(i64::MAX, 0)),
                &[&1i64.to_be_bytes()]
            )
            .is_err()
        );
    }
}
