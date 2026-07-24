#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
    outbox_mutations_since_compaction: DashMap<FullTextRuntimeKey, usize>,
}
