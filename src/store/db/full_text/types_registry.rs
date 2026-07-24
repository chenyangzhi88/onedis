use super::*;
#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    pub(super) indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
    pub(super) outbox_mutations_since_compaction: DashMap<FullTextRuntimeKey, usize>,
    pub(super) lifecycle_locks: DashMap<FullTextRuntimeKey, Arc<RwLock<()>>>,
    pub(super) source_routes: DashMap<u16, Arc<Vec<FullTextSourceRoute>>>,
    pub(super) outbox_pending: DashMap<FullTextRuntimeKey, u64>,
}

#[derive(Clone)]
pub(super) struct FullTextSourceRoute {
    pub(super) index: String,
    pub(super) source_type: FullTextSourceType,
    pub(super) prefixes: Vec<String>,
}
