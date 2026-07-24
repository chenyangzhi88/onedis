use super::*;
impl FullTextRuntimeRegistry {
    pub(super) fn key(db_index: u16, index: &str) -> FullTextRuntimeKey {
        FullTextRuntimeKey {
            db_index,
            index: index.to_string(),
        }
    }

    pub(super) fn insert(&self, db_index: u16, index: &str, runtime: FullTextRuntime) {
        self.indexes
            .insert(Self::key(db_index, index), Arc::new(RwLock::new(runtime)));
    }

    pub(super) fn get(&self, db_index: u16, index: &str) -> Option<Arc<RwLock<FullTextRuntime>>> {
        self.indexes
            .get(&Self::key(db_index, index))
            .map(|entry| entry.value().clone())
    }

    pub(super) fn remove(&self, db_index: u16, index: &str) {
        let key = Self::key(db_index, index);
        self.indexes.remove(&key);
        self.outbox_mutations_since_compaction.remove(&key);
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
        self.outbox_mutations_since_compaction
            .retain(|key, _| key.db_index != db_index);
        self.lifecycle_locks
            .retain(|key, _| key.db_index != db_index);
        self.source_routes.remove(&db_index);
        self.outbox_pending
            .retain(|key, _| key.db_index != db_index);
    }

    pub(super) fn lifecycle_lock(&self, db_index: u16, index: &str) -> Arc<RwLock<()>> {
        self.lifecycle_locks
            .entry(Self::key(db_index, index))
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    pub(super) fn invalidate_source_routes(&self, db_index: u16) {
        self.source_routes.remove(&db_index);
    }

    pub(super) fn source_routes(&self, db_index: u16) -> Option<Arc<Vec<FullTextSourceRoute>>> {
        self.source_routes
            .get(&db_index)
            .map(|entry| entry.value().clone())
    }

    pub(super) fn set_source_routes(&self, db_index: u16, routes: Vec<FullTextSourceRoute>) {
        self.source_routes.insert(db_index, Arc::new(routes));
    }

    pub(super) fn set_outbox_pending(&self, db_index: u16, index: &str, pending: u64) {
        self.outbox_pending
            .insert(Self::key(db_index, index), pending);
    }

    pub(super) fn outbox_pending(&self, db_index: u16, index: &str) -> Option<u64> {
        self.outbox_pending
            .get(&Self::key(db_index, index))
            .map(|entry| *entry.value())
    }

    pub(super) fn add_outbox_pending(&self, db_index: u16, index: &str, delta: u64) {
        let mut pending = self
            .outbox_pending
            .entry(Self::key(db_index, index))
            .or_default();
        *pending = pending.saturating_add(delta);
    }

    pub(super) fn remove_outbox_pending(&self, db_index: u16, index: &str, delta: u64) {
        let mut pending = self
            .outbox_pending
            .entry(Self::key(db_index, index))
            .or_default();
        *pending = pending.saturating_sub(delta);
    }

    pub(super) fn clear_outbox_pending(&self, db_index: u16, index: &str) {
        self.outbox_pending.remove(&Self::key(db_index, index));
    }

    pub(super) fn note_outbox_mutation(
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
