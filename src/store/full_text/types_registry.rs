#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
}
