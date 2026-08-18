use super::*;
#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    pub(super) indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
    pub(super) outbox_mutations_since_compaction: DashMap<FullTextRuntimeKey, usize>,
    pub(super) lifecycle_locks: DashMap<FullTextRuntimeKey, Weak<RwLock<()>>>,
    pub(super) refresh_locks: DashMap<FullTextRuntimeKey, Weak<Mutex<()>>>,
    pub(super) lock_prune_ticks: AtomicU64,
    pub(super) source_routes: DashMap<u16, Arc<Vec<FullTextSourceRoute>>>,
    pub(super) outbox_pending: DashMap<FullTextRuntimeKey, u64>,
    pub(super) latest_outbox_seq: DashMap<FullTextRuntimeKey, u64>,
    pub(super) config_values: DashMap<(u16, String), Option<String>>,
    pub(super) aliases: DashMap<(u16, String), String>,
    pub(super) query_asts: DashMap<FullTextQueryCacheKey, Arc<FullTextQueryAst>>,
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
