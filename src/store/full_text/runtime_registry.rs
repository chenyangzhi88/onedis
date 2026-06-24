impl FullTextRuntimeRegistry {
    fn key(db_index: u16, index: &str) -> FullTextRuntimeKey {
        FullTextRuntimeKey {
            db_index,
            index: index.to_string(),
        }
    }

    fn insert(&self, db_index: u16, index: &str, runtime: FullTextRuntime) {
        self.indexes
            .insert(Self::key(db_index, index), Arc::new(RwLock::new(runtime)));
    }

    fn get(&self, db_index: u16, index: &str) -> Option<Arc<RwLock<FullTextRuntime>>> {
        self.indexes
            .get(&Self::key(db_index, index))
            .map(|entry| entry.value().clone())
    }

    fn remove(&self, db_index: u16, index: &str) {
        self.indexes.remove(&Self::key(db_index, index));
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
    }
}
