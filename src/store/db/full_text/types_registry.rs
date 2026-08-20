use super::*;
#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    pub(super) indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
    /// Query-facing generations are published separately from mutable writers.
    /// A query briefly resolves the slot in the registry and then takes one
    /// atomic Arc snapshot without acquiring the writer lock.
    pub(super) search_generations:
        DashMap<FullTextRuntimeKey, Arc<ArcSwapOption<FullTextSearchGeneration>>>,
    pub(super) outbox_mutations_since_compaction: DashMap<FullTextRuntimeKey, usize>,
    pub(super) lifecycle_locks: DashMap<FullTextRuntimeKey, Weak<RwLock<()>>>,
    pub(super) refresh_locks: DashMap<FullTextRuntimeKey, Weak<Mutex<()>>>,
    pub(super) lock_prune_ticks: AtomicU64,
    pub(super) source_routes: DashMap<u16, Arc<Vec<FullTextSourceRoute>>>,
    pub(super) outbox_pending: DashMap<FullTextRuntimeKey, u64>,
    pub(super) latest_outbox_seq: DashMap<FullTextRuntimeKey, u64>,
    pub(super) progress_signals: DashMap<FullTextRuntimeKey, Arc<FullTextProgressSignal>>,
    pub(super) config_values: DashMap<(u16, String), Option<String>>,
    pub(super) aliases: DashMap<(u16, String), String>,
    pub(super) query_asts: DashMap<FullTextQueryCacheKey, FullTextQueryCacheEntry>,
    pub(super) query_cache_clock: AtomicU64,
    pub(super) query_cache_eviction_lock: Mutex<()>,
    pub(super) maintenance_cursor: AtomicUsize,
}

pub(super) struct FullTextQueryCacheEntry {
    pub(super) ast: Arc<FullTextQueryAst>,
    pub(super) last_access: AtomicU64,
}

#[derive(Default)]
pub(super) struct FullTextProgressSignal {
    generation: Mutex<u64>,
    changed: Condvar,
}

impl FullTextProgressSignal {
    pub(super) fn generation(&self) -> Result<u64, Error> {
        self.generation
            .lock()
            .map(|generation| *generation)
            .map_err(|_| Error::msg("ERR fulltext progress signal lock poisoned"))
    }

    pub(super) fn notify(&self) {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        self.changed.notify_all();
    }

    pub(super) fn wait_for_change(&self, observed: u64, deadline: Instant) -> Result<bool, Error> {
        let generation = self
            .generation
            .lock()
            .map_err(|_| Error::msg("ERR fulltext progress signal lock poisoned"))?;
        if *generation != observed {
            return Ok(true);
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Ok(false);
        };
        let (generation, _) = self
            .changed
            .wait_timeout_while(generation, remaining, |generation| *generation == observed)
            .map_err(|_| Error::msg("ERR fulltext progress signal lock poisoned"))?;
        Ok(*generation != observed)
    }
}

#[derive(Clone)]
pub(super) struct FullTextSourceRoute {
    pub(super) index: String,
    pub(super) source_type: FullTextSourceType,
    pub(super) prefixes: Vec<String>,
    pub(super) meta: FullTextIndexMeta,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct FullTextQueryCacheKey {
    pub(super) db_index: u16,
    pub(super) index: String,
    pub(super) incarnation: u64,
    pub(super) dialect: u8,
    pub(super) query: String,
}
