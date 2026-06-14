//! Industrial-grade Redis TTL expiration engine.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                        TtlManager                            │
//! │                                                              │
//! │  ┌───────────────────┐    ┌────────────────────────────────┐ │
//! │  │ TTL namespace     │    │  Background Sweeper (tokio)    │ │
//! │  │ in kv_engine      │───►│  ┌──────────────────────────┐  │ │
//! │  │ ordered by        │    │  │ 1. scan expired entries  │  │ │
//! │  │ (db, expire, key) │    │  │ 2. Lazy Double Check     │  │ │
//! │  └───────────────────┘    │  │ 3. WriteBatch + DelRange │  │ │
//! │                           │  └──────────────────────────┘  │ │
//! │                           └────────────────────────────────┘ │
//! │  ┌──────────────────────────────────────────────────────┐    │
//! │  │  Notify — wake sweeper on short-TTL inserts          │    │
//! │  └──────────────────────────────────────────────────────┘    │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Design Decisions
//!
//! - **Append-only index**: EXPIRE on an existing key appends a new entry;
//!   the stale entry is filtered during sweep via Double Check. Write paths do
//!   not coordinate through an in-memory TTL tree or key-to-deadline map.
//!
//! - **Lazy Double Check**: Before physical delete the sweeper verifies
//!   (1) meta key still exists, (2) stored expire_ms matches the index entry.
//!   This eliminates all races with user DEL / PERSIST / re-EXPIRE commands.
//!
//! - **Version-based DeleteRange**: Sub-keys are prefixed with a monotonic
//!   version, enabling O(1) bulk cleanup via a single DeleteRange per
//!   namespace instead of scan + individual delete.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::types::write_batch::WriteBatch;
use log::{debug, info};

use super::kv_store::KvStore;

type ExpireHook = dyn Fn(u16, &str, u8, &mut WriteBatch) -> bool + Send + Sync;

// ============================================================================
// Type Tags — encoded at meta value byte offset 16 (after expire_ms + version)
// ============================================================================

pub const TYPE_STRING: u8 = 1;
pub const TYPE_HASH: u8 = 2;
pub const TYPE_SET: u8 = 3;
pub const TYPE_SORTED_SET: u8 = 4;
pub const TYPE_LIST: u8 = 5;
pub const TYPE_JSON: u8 = 6;
pub const TYPE_VECTOR: u8 = 7;
pub const TYPE_STREAM: u8 = 8;

// ============================================================================
// Namespace byte patterns (must mirror db.rs constants)
// ============================================================================

const HASH_FIELD_NS: [u8; 3] = [0xFF, b'h', 0x00];
const HASH_FIELD_EXPIRE_NS: [u8; 3] = [0xFF, b'H', 0x00];
const LIST_ITEM_NS: [u8; 3] = [0xFF, b'l', 0x00];
const SET_MEMBER_NS: [u8; 3] = [0xFF, b's', 0x00];
const SET_SLOT_NS: [u8; 3] = [0xFF, b'S', 0x00];
const SET_MEMBER_SLOT_NS: [u8; 3] = [0xFF, b't', 0x00];
const ZSET_MEMBER_NS: [u8; 3] = [0xFF, b'z', 0x00];
const ZSET_RANK_NS: [u8; 3] = [0xFF, b'Z', 0x00];
const STREAM_ENTRY_NS: [u8; 3] = [0xFF, b'x', 0x00];
const STREAM_GROUP_NS: [u8; 3] = [0xFF, b'g', 0x00];
const STREAM_PEL_NS: [u8; 3] = [0xFF, b'p', 0x00];
const STREAM_CONSUMER_NS: [u8; 3] = [0xFF, b'c', 0x00];
const JSON_NODE_NS: [u8; 3] = [0xFF, b'j', 0x00];
const VECTOR_META_NS: [u8; 3] = [0xFF, b'v', 0x00];
const VECTOR_DOC_NS: [u8; 3] = [0xFF, b'v', 0x01];
const VECTOR_TAG_NS: [u8; 3] = [0xFF, b'v', 0x02];
const VECTOR_NUMERIC_NS: [u8; 3] = [0xFF, b'v', 0x03];
const VECTOR_SEGMENT_NS: [u8; 3] = [0xFF, b'v', 0x04];
const VECTOR_GRAPH_NS: [u8; 3] = [0xFF, b'v', 0x05];
const LIST_META_MAGIC: [u8; 4] = *b"ULST";
const STREAM_META_MAGIC: [u8; 4] = *b"USTR";
const TTL_INDEX_PREFIX: &[u8] = b"\xFF\xFFonedis:ttl:";
const TTL_INDEX_VALUE: &[u8] = b"\x01";
const VERSION_COUNTER_KEY: &[u8] = b"\xFF\xFFonedis:version";
const VERSION_MARK_PREFIX: &[u8] = b"\xFF\xFFonedis:version:";
const VERSION_RESERVATION_BLOCK: u64 = 4096;

// ============================================================================
// Meta Header — fast decode without full bincode deserialization
// ============================================================================

/// Decoded header fields common to every meta value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaHeader {
    pub expire_ms: u64,
    pub version: u64,
    pub type_tag: u8,
}

/// Decode the fixed-size header from a raw meta value.
///
/// **Regular** format: `[expire_ms:8][version:8][type_tag:1][bincode…]`
/// **List** format:    `[ULST:4][expire_ms:8][version:8][head:8][tail:8]` (36 B)
pub fn decode_meta_header(raw: &[u8]) -> Option<MetaHeader> {
    // List meta: 36 bytes, starts with b"ULST"
    if raw.len() == 36 && raw[..4] == LIST_META_MAGIC {
        return Some(MetaHeader {
            expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
            version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
            type_tag: TYPE_LIST,
        });
    }
    // Stream meta: 52 bytes, starts with b"USTR"
    if raw.len() == 52 && raw[..4] == STREAM_META_MAGIC {
        return Some(MetaHeader {
            expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
            version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
            type_tag: TYPE_STREAM,
        });
    }
    // Regular meta: at least 17 bytes
    if raw.len() < 17 {
        return None;
    }
    Some(MetaHeader {
        expire_ms: u64::from_be_bytes(raw[0..8].try_into().ok()?),
        version: u64::from_be_bytes(raw[8..16].try_into().ok()?),
        type_tag: raw[16],
    })
}

pub fn patch_meta_expire_ms(raw: &[u8], expire_ms: u64) -> Option<Vec<u8>> {
    let mut patched = raw.to_vec();
    if raw.len() == 36 && raw[..4] == LIST_META_MAGIC {
        patched[4..12].copy_from_slice(&expire_ms.to_be_bytes());
        return Some(patched);
    }
    if raw.len() == 52 && raw[..4] == STREAM_META_MAGIC {
        patched[4..12].copy_from_slice(&expire_ms.to_be_bytes());
        return Some(patched);
    }
    if patched.len() < 8 {
        return None;
    }
    patched[0..8].copy_from_slice(&expire_ms.to_be_bytes());
    Some(patched)
}

// ============================================================================
// Version Counter
// ============================================================================

/// Monotonically increasing, lock-free version generator.
///
/// A new version is allocated each time a key is created or changes type.
/// Sub-keys carry the version in their encoding, which allows the TTL
/// sweeper to issue a single `DeleteRange` per namespace to reclaim all
/// sub-keys that belong to an expired (key, version) pair.
pub struct VersionCounter {
    counter: AtomicU64,
    reserved_until: AtomicU64,
    reservation_lock: AtomicBool,
}

impl VersionCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
            reserved_until: AtomicU64::new(0),
            reservation_lock: AtomicBool::new(false),
        }
    }

    /// Allocate the next version number.
    #[inline]
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Allocate the next version, reserving a durable high-water mark in blocks.
    ///
    /// The reservation callback runs before a new block is made visible to
    /// other allocators. Crashes may leave gaps, but versions are only required
    /// to be unique across restarts so old sub-key namespaces are never reused.
    pub fn next_reserved<F>(&self, mut persist_high_water: F) -> u64
    where
        F: FnMut(u64),
    {
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current < reserved_until {
                if self
                    .counter
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return current + 1;
                }
                continue;
            }

            let _guard = self.acquire_reservation_lock_blocking();
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current >= reserved_until {
                let high_water = current.saturating_add(VERSION_RESERVATION_BLOCK);
                persist_high_water(high_water);
                self.reserved_until.store(high_water, Ordering::Release);
            }
        }
    }

    pub async fn next_reserved_async<F, Fut>(&self, mut persist_high_water: F) -> u64
    where
        F: FnMut(u64) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current < reserved_until {
                if self
                    .counter
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return current + 1;
                }
                continue;
            }

            let _guard = self.acquire_reservation_lock_async().await;
            let current = self.counter.load(Ordering::Relaxed);
            let reserved_until = self.reserved_until.load(Ordering::Acquire);
            if current >= reserved_until {
                let high_water = current.saturating_add(VERSION_RESERVATION_BLOCK);
                persist_high_water(high_water).await;
                self.reserved_until.store(high_water, Ordering::Release);
            }
        }
    }

    fn acquire_reservation_lock_blocking(&self) -> VersionReservationGuard<'_> {
        while self
            .reservation_lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::thread::yield_now();
        }
        VersionReservationGuard {
            lock: &self.reservation_lock,
        }
    }

    async fn acquire_reservation_lock_async(&self) -> VersionReservationGuard<'_> {
        while self
            .reservation_lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            tokio::task::yield_now().await;
        }
        VersionReservationGuard {
            lock: &self.reservation_lock,
        }
    }

    /// Return the most-recently-observed maximum version.
    #[inline]
    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// Update the high-water mark if `v` exceeds the current maximum.
    ///
    /// Called during startup rebuild so the counter picks up where it left off.
    pub fn observe(&self, v: u64) {
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if v > cur { Some(v) } else { None }
            });
        let _ = self
            .reserved_until
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if v > cur { Some(v) } else { None }
            });
    }
}

struct VersionReservationGuard<'a> {
    lock: &'a AtomicBool,
}

impl Drop for VersionReservationGuard<'_> {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

// ============================================================================
// Sub-key range helpers  (for DeleteRange)
// ============================================================================
//
// Sub-key layout (after the version migration):
//
//   [db_prefix:2][namespace:3][key_bytes][0x00][version:8 BE][field/member/…]
//
// A DeleteRange from `prefix(version)` to `prefix(version+1)` covers exactly
// the sub-keys that belong to this (key, version) pair.

fn sub_key_range_start(db_index: u16, ns: &[u8; 3], key: &str, version: u64) -> Vec<u8> {
    let pfx = db_index.to_be_bytes();
    let mut buf = Vec::with_capacity(2 + 3 + key.len() + 1 + 8);
    buf.extend_from_slice(&pfx);
    buf.extend_from_slice(ns);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&version.to_be_bytes());
    buf
}

#[inline]
fn sub_key_range_end(db_index: u16, ns: &[u8; 3], key: &str, version: u64) -> Vec<u8> {
    sub_key_range_start(db_index, ns, key, version + 1)
}

/// Append `DeleteRange` ops to `batch` for every sub-key namespace that the
/// given type uses.
pub fn delete_sub_keys_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    type_tag: u8,
) {
    match type_tag {
        TYPE_HASH => {
            batch.delete_range(
                &sub_key_range_start(db_index, &HASH_FIELD_NS, key, version),
                &sub_key_range_end(db_index, &HASH_FIELD_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &HASH_FIELD_EXPIRE_NS, key, version),
                &sub_key_range_end(db_index, &HASH_FIELD_EXPIRE_NS, key, version),
            );
        }
        TYPE_SET => {
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_MEMBER_NS, key, version),
                &sub_key_range_end(db_index, &SET_MEMBER_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_SLOT_NS, key, version),
                &sub_key_range_end(db_index, &SET_SLOT_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_MEMBER_SLOT_NS, key, version),
                &sub_key_range_end(db_index, &SET_MEMBER_SLOT_NS, key, version),
            );
        }
        TYPE_SORTED_SET => {
            // member index
            batch.delete_range(
                &sub_key_range_start(db_index, &ZSET_MEMBER_NS, key, version),
                &sub_key_range_end(db_index, &ZSET_MEMBER_NS, key, version),
            );
            // rank index
            batch.delete_range(
                &sub_key_range_start(db_index, &ZSET_RANK_NS, key, version),
                &sub_key_range_end(db_index, &ZSET_RANK_NS, key, version),
            );
        }
        TYPE_LIST => {
            batch.delete_range(
                &sub_key_range_start(db_index, &LIST_ITEM_NS, key, version),
                &sub_key_range_end(db_index, &LIST_ITEM_NS, key, version),
            );
        }
        TYPE_STREAM => {
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_ENTRY_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_ENTRY_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_GROUP_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_GROUP_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_PEL_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_PEL_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_CONSUMER_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_CONSUMER_NS, key, version),
            );
        }
        TYPE_JSON => {
            batch.delete(&sub_key_range_start(db_index, &JSON_NODE_NS, key, version));
            batch.delete_range(
                &sub_key_range_start(db_index, &JSON_NODE_NS, key, version),
                &sub_key_range_end(db_index, &JSON_NODE_NS, key, version),
            );
        }
        TYPE_VECTOR => {
            for ns in [
                &VECTOR_META_NS,
                &VECTOR_DOC_NS,
                &VECTOR_TAG_NS,
                &VECTOR_NUMERIC_NS,
                &VECTOR_SEGMENT_NS,
                &VECTOR_GRAPH_NS,
            ] {
                batch.delete_range(
                    &sub_key_range_start(db_index, ns, key, version),
                    &sub_key_range_end(db_index, ns, key, version),
                );
            }
        }
        // String — no sub-keys
        _ => {}
    }
}

// ============================================================================
// TTL Index
// ============================================================================

/// One entry in the append-only TTL index.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TtlEntry {
    expire_ms: u64,
    db_index: u16,
    key: String,
}

// ============================================================================
// TtlManager — public API
// ============================================================================

/// Tuning knobs for the background sweeper.
pub struct TtlConfig {
    /// Maximum sleep between sweep cycles (ms).  The sweeper will wake
    /// earlier if a short-TTL entry is inserted or the nearest deadline
    /// arrives.
    pub sweep_interval_ms: u64,
    /// Maximum keys to physically delete per cycle (back-pressure).
    pub batch_size: usize,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            sweep_interval_ms: 100,
            batch_size: 1000,
        }
    }
}

/// Runtime counters for monitoring / debugging.
pub struct TtlStats {
    pub keys_expired: AtomicU64,
    pub stale_entries_skipped: AtomicU64,
    pub sweep_cycles: AtomicU64,
}

/// Thread-safe TTL expiration manager.
///
/// Create via [`TtlManager::new`], call [`TtlManager::start_sweeper`] once
/// the tokio runtime is available, then register expirations with [`TtlManager::add`].
pub struct TtlManager {
    db_count: AtomicU32,
    store: KvStore,
    config: TtlConfig,
    notify: tokio::sync::Notify,
    shutdown: AtomicBool,
    stats: TtlStats,
    expire_hook: RwLock<Option<Arc<ExpireHook>>>,
}

impl TtlManager {
    pub fn new(store: KvStore, config: TtlConfig) -> Arc<Self> {
        Arc::new(Self {
            db_count: AtomicU32::new(1),
            store,
            config,
            notify: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
            stats: TtlStats {
                keys_expired: AtomicU64::new(0),
                stale_entries_skipped: AtomicU64::new(0),
                sweep_cycles: AtomicU64::new(0),
            },
            expire_hook: RwLock::new(None),
        })
    }

    pub fn set_expire_hook(&self, hook: Arc<ExpireHook>) {
        let mut guard = self
            .expire_hook
            .write()
            .expect("ttl expire hook lock poisoned");
        *guard = Some(hook);
    }

    // ------------------------------------------------------------------ add
    /// Register a key's TTL.
    ///
    /// Called by the command layer whenever a deadline is set (EXPIRE, SET EX,
    /// PEXPIRE, etc.).  The entry is appended; stale duplicates for the same
    /// key are harmless and will be discarded by the Double Check.
    pub fn add(&self, expire_ms: u64, db_index: u16, key: String) {
        if expire_ms == 0 {
            return;
        }
        let mut batch = WriteBatch::new();
        self.add_to_batch(&mut batch, expire_ms, db_index, &key);
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }
    }

    pub fn add_to_batch(&self, batch: &mut WriteBatch, expire_ms: u64, db_index: u16, key: &str) {
        if expire_ms == 0 {
            return;
        }
        batch.put(&ttl_index_key(expire_ms, db_index, key), TTL_INDEX_VALUE);
        self.notify.notify_one();
    }

    pub fn remove(&self, db_index: u16, key: &str) {
        let mut batch = WriteBatch::new();
        self.remove_to_batch(&mut batch, db_index, key);
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }
    }

    pub fn remove_to_batch(&self, _batch: &mut WriteBatch, db_index: u16, key: &str) {
        let _ = (db_index, key);
    }

    pub fn remove_known_to_batch(
        &self,
        batch: &mut WriteBatch,
        expire_ms: u64,
        db_index: u16,
        key: &str,
    ) {
        if expire_ms == 0 {
            return;
        }
        batch.delete(&ttl_index_key(expire_ms, db_index, key));
    }

    pub fn remove_db_to_batch(&self, batch: &mut WriteBatch, db_index: u16) {
        for (key, _) in self.store.scan_prefix_raw(&ttl_db_prefix(db_index)) {
            batch.delete(&key);
        }
    }

    pub async fn remove_db_to_batch_async(&self, batch: &mut WriteBatch, db_index: u16) {
        for (key, _) in self
            .store
            .scan_prefix_raw_async(&ttl_db_prefix(db_index))
            .await
        {
            batch.delete(&key);
        }
    }

    // ---------------------------------------------------------- rebuild
    /// Scan persisted metadata needed by the TTL subsystem.
    ///
    /// The TTL namespace itself is the sweeper's source of truth, so rebuild no
    /// longer materializes an in-memory expiration tree.
    pub fn rebuild_from_store(&self, num_dbs: u16, version_counter: &VersionCounter) {
        self.db_count
            .store(num_dbs.max(1) as u32, Ordering::Release);
        let mut with_ttl = 0usize;
        for db_idx in 0..num_dbs {
            for (ttl_key, _) in self.store.scan_prefix_raw(&ttl_db_prefix(db_idx)) {
                if parse_ttl_index_key(&ttl_key).is_some() {
                    with_ttl += 1;
                }
            }
        }

        if let Some(raw) = self.store.get_raw(VERSION_COUNTER_KEY) {
            if raw.len() == 8 {
                let max_version = u64::from_be_bytes(raw[0..8].try_into().unwrap());
                version_counter.observe(max_version);
                info!(
                    "TTL index rebuilt from namespace: {} keys with TTL, checkpoint_version = {}",
                    with_ttl,
                    version_counter.current()
                );
            }
        }

        for (version_key, _) in self.store.scan_prefix_raw(VERSION_MARK_PREFIX) {
            if let Some(version) = parse_version_mark_key(&version_key) {
                version_counter.observe(version);
            }
        }

        let max_version = version_counter.current();
        if max_version > 0 {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, max_version);
            if batch.count() > 0 {
                self.store.write_batch(&batch);
            }
        }

        info!(
            "TTL index rebuilt from namespace: {} keys with TTL, max_version = {}",
            with_ttl,
            version_counter.current()
        );
    }

    pub async fn rebuild_from_store_async(&self, num_dbs: u16, version_counter: &VersionCounter) {
        self.db_count
            .store(num_dbs.max(1) as u32, Ordering::Release);
        let mut with_ttl = 0usize;
        for db_idx in 0..num_dbs {
            for (ttl_key, _) in self
                .store
                .scan_prefix_raw_async(&ttl_db_prefix(db_idx))
                .await
            {
                if parse_ttl_index_key(&ttl_key).is_some() {
                    with_ttl += 1;
                }
            }
        }

        if let Some(raw) = self.store.get_raw(VERSION_COUNTER_KEY)
            && raw.len() == 8
        {
            let max_version = u64::from_be_bytes(raw[0..8].try_into().unwrap());
            version_counter.observe(max_version);
            info!(
                "TTL index rebuilt from namespace: {} keys with TTL, checkpoint_version = {}",
                with_ttl,
                version_counter.current()
            );
        }

        for (version_key, _) in self.store.scan_prefix_raw_async(VERSION_MARK_PREFIX).await {
            if let Some(version) = parse_version_mark_key(&version_key) {
                version_counter.observe(version);
            }
        }

        let max_version = version_counter.current();
        if max_version > 0 {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, max_version);
            if batch.count() > 0 {
                self.store.write_batch(&batch);
            }
        }

        info!(
            "TTL index rebuilt from namespace: {} keys with TTL, max_version = {}",
            with_ttl,
            version_counter.current()
        );
    }

    // -------------------------------------------------------- sweeper lifecycle
    /// Spawn the background sweeper as a tokio task.
    pub fn start_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move { mgr.sweeper_loop().await });
        info!(
            "TTL sweeper started (interval = {} ms, batch = {})",
            self.config.sweep_interval_ms, self.config.batch_size
        );
    }

    /// Signal the sweeper to exit.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    pub fn stats(&self) -> &TtlStats {
        &self.stats
    }

    pub fn index_size(&self) -> usize {
        self.store.scan_prefix_raw(TTL_INDEX_PREFIX).len()
    }

    pub async fn index_size_async(&self) -> usize {
        self.store
            .scan_prefix_raw_async(TTL_INDEX_PREFIX)
            .await
            .len()
    }

    // ================================================================
    // Sweeper internals
    // ================================================================

    async fn sweeper_loop(&self) {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                info!("TTL sweeper shutting down");
                return;
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.config.sweep_interval_ms)) => {}
                _ = self.notify.notified() => {}
            }
            if self.shutdown.load(Ordering::Acquire) {
                info!("TTL sweeper shutting down");
                return;
            }

            let more_expired = self.sweep_once_async().await;
            if more_expired {
                tokio::task::yield_now().await;
            }
        }
    }

    /// One sweep cycle: drain expired entries, Double Check, delete.
    #[allow(dead_code)]
    fn sweep_once(&self) -> bool {
        let now = now_ms();
        let expired = self.scan_expired_batch(now, self.config.batch_size);

        if expired.is_empty() {
            return false;
        }

        self.stats.sweep_cycles.fetch_add(1, Ordering::Relaxed);

        let mut deleted = 0usize;
        let mut stale = 0usize;
        let mut batch = WriteBatch::new();

        for entry in expired.iter().take(self.config.batch_size) {
            match self.plan_expire_key(entry, &mut batch) {
                ExpireResult::Deleted => deleted += 1,
                ExpireResult::Stale | ExpireResult::NotFound => stale += 1,
            }
        }
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }

        self.stats
            .keys_expired
            .fetch_add(deleted as u64, Ordering::Relaxed);
        self.stats
            .stale_entries_skipped
            .fetch_add(stale as u64, Ordering::Relaxed);

        if deleted > 0 || stale > 0 {
            debug!("TTL sweep: {} deleted, {} stale/skipped", deleted, stale);
        }

        expired.len() == self.config.batch_size
    }

    async fn sweep_once_async(&self) -> bool {
        let now = now_ms();
        let expired = self
            .scan_expired_batch_async(now, self.config.batch_size)
            .await;

        if expired.is_empty() {
            return false;
        }

        self.stats.sweep_cycles.fetch_add(1, Ordering::Relaxed);

        let mut deleted = 0usize;
        let mut stale = 0usize;
        let mut batch = WriteBatch::new();

        for entry in expired.iter().take(self.config.batch_size) {
            match self.plan_expire_key(entry, &mut batch) {
                ExpireResult::Deleted => deleted += 1,
                ExpireResult::Stale | ExpireResult::NotFound => stale += 1,
            }
        }
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }

        self.stats
            .keys_expired
            .fetch_add(deleted as u64, Ordering::Relaxed);
        self.stats
            .stale_entries_skipped
            .fetch_add(stale as u64, Ordering::Relaxed);

        if deleted > 0 || stale > 0 {
            debug!("TTL sweep: {} deleted, {} stale/skipped", deleted, stale);
        }

        expired.len() == self.config.batch_size
    }

    #[allow(dead_code)]
    fn scan_expired_batch(&self, now: u64, batch_size: usize) -> Vec<TtlEntry> {
        let mut expired = Vec::with_capacity(batch_size);
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        for db_idx in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            let lower = ttl_db_prefix(db_idx);
            let upper = ttl_db_expire_upper_bound(db_idx, now);
            let remaining = batch_size - expired.len();
            for (ttl_key, _) in
                self.store
                    .scan_range_raw_limited(&lower, Some(upper.clone()), remaining)
            {
                if let Some((expire_ms, parsed_db, key)) = parse_ttl_index_key(&ttl_key) {
                    debug_assert_eq!(parsed_db, db_idx);
                    expired.push(TtlEntry {
                        expire_ms,
                        db_index: parsed_db,
                        key,
                    });
                }
            }
        }
        expired
    }

    async fn scan_expired_batch_async(&self, now: u64, batch_size: usize) -> Vec<TtlEntry> {
        let mut expired = Vec::with_capacity(batch_size);
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        for db_idx in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            let lower = ttl_db_prefix(db_idx);
            let upper = ttl_db_expire_upper_bound(db_idx, now);
            let remaining = batch_size - expired.len();
            for (ttl_key, _) in self
                .store
                .scan_range_raw_limited_async(&lower, Some(upper.clone()), remaining)
                .await
            {
                if let Some((expire_ms, parsed_db, key)) = parse_ttl_index_key(&ttl_key) {
                    debug_assert_eq!(parsed_db, db_idx);
                    expired.push(TtlEntry {
                        expire_ms,
                        db_index: parsed_db,
                        key,
                    });
                }
            }
        }
        expired
    }

    // ================================================================
    // Lazy Double Check
    // ================================================================
    //
    // Protocol:
    //
    //   1. Read meta key from KV engine.
    //      → absent?  Already DEL'd by user → discard index entry.
    //
    //   2. Compare real expire_ms with the index entry's expire_ms.
    //      → mismatch? User called EXPIRE / PERSIST / SET EX again after
    //        this entry was inserted → discard (the new deadline has its
    //        own index entry).
    //
    //   3. Both checks pass → build WriteBatch:
    //        • Delete(meta_key)
    //        • DeleteRange(sub-keys, bounded by version)
    //      Commit atomically.

    fn plan_expire_key(&self, entry: &TtlEntry, batch: &mut WriteBatch) -> ExpireResult {
        let meta_key = main_key(entry.db_index, &entry.key);

        // ── Check 1: meta key still alive? ──
        let Some(raw) = self.store.get_raw(&meta_key) else {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::NotFound;
        };

        // ── Check 2: expire_ms matches index entry? ──
        let Some(header) = decode_meta_header(&raw) else {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::Stale;
        };
        if header.expire_ms != entry.expire_ms {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::Stale;
        }

        let hook = self
            .expire_hook
            .read()
            .expect("ttl expire hook lock poisoned")
            .clone();
        if let Some(hook) = hook
            && !hook(entry.db_index, &entry.key, header.type_tag, batch)
        {
            return ExpireResult::Stale;
        }

        // ── Double Check passed — physical deletion ──
        batch.delete(&meta_key);
        batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
        delete_sub_keys_to_batch(
            batch,
            entry.db_index,
            &entry.key,
            header.version,
            header.type_tag,
        );
        if header.type_tag == TYPE_JSON {
            for (node_key, _) in self.store.scan_prefix_raw(&json_node_prefix(
                entry.db_index,
                &entry.key,
                header.version,
            )) {
                batch.delete(&node_key);
            }
        }
        ExpireResult::Deleted
    }
}

enum ExpireResult {
    Deleted,
    Stale,
    NotFound,
}

// ============================================================================
// Helpers
// ============================================================================

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main_key(db_index: u16, key: &str) -> Vec<u8> {
    let pfx = db_index.to_be_bytes();
    let mut k = Vec::with_capacity(2 + key.len());
    k.extend_from_slice(&pfx);
    k.extend_from_slice(key.as_bytes());
    k
}

fn json_node_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + JSON_NODE_NS.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_index.to_be_bytes());
    prefix.extend_from_slice(&JSON_NODE_NS);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub fn reserve_version_high_water_to_batch(batch: &mut WriteBatch, high_water: u64) {
    batch.put(VERSION_COUNTER_KEY, &high_water.to_be_bytes());
}

fn parse_version_mark_key(key: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(VERSION_MARK_PREFIX)?;
    if suffix.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(suffix.try_into().ok()?))
}

fn ttl_db_prefix(db_index: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(TTL_INDEX_PREFIX.len() + 2);
    key.extend_from_slice(TTL_INDEX_PREFIX);
    key.extend_from_slice(&db_index.to_be_bytes());
    key
}

fn ttl_db_expire_upper_bound(db_index: u16, now_ms: u64) -> Vec<u8> {
    let mut key = ttl_db_prefix(db_index);
    key.extend_from_slice(&now_ms.saturating_add(1).to_be_bytes());
    key
}

fn ttl_index_key(expire_ms: u64, db_index: u16, user_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TTL_INDEX_PREFIX.len() + 2 + 8 + user_key.len());
    key.extend_from_slice(TTL_INDEX_PREFIX);
    key.extend_from_slice(&db_index.to_be_bytes());
    key.extend_from_slice(&expire_ms.to_be_bytes());
    key.extend_from_slice(user_key.as_bytes());
    key
}

fn parse_ttl_index_key(key: &[u8]) -> Option<(u64, u16, String)> {
    let suffix = key.strip_prefix(TTL_INDEX_PREFIX)?;
    if suffix.len() < 10 {
        return None;
    }
    let db_index = u16::from_be_bytes(suffix[0..2].try_into().ok()?);
    let expire_ms = u64::from_be_bytes(suffix[2..10].try_into().ok()?);
    let user_key = String::from_utf8(suffix[10..].to_vec()).ok()?;
    Some((expire_ms, db_index, user_key))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[cfg(test)]
mod tests {
    include!("ttl/tests.rs");
}
