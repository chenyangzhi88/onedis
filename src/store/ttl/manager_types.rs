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
