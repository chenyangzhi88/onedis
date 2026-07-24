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
        let key = Self::key(db_index, index);
        self.indexes.remove(&key);
        self.outbox_mutations_since_compaction.remove(&key);
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
        self.outbox_mutations_since_compaction
            .retain(|key, _| key.db_index != db_index);
    }

    fn note_outbox_mutation(
        &self,
        db_index: u16,
        index: &str,
        compact_threshold: usize,
    ) -> bool {
        if compact_threshold == 0 || compact_threshold == usize::MAX {
            return false;
        }
        let mut pending = self
            .outbox_mutations_since_compaction
            .entry(Self::key(db_index, index))
            .or_default();
        *pending = pending.saturating_add(1);
        if *pending <= compact_threshold {
            return false;
        }
        *pending = 0;
        true
    }
}
