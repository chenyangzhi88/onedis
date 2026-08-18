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

    pub(super) fn get_or_try_insert(
        &self,
        db_index: u16,
        index: &str,
        create: impl FnOnce() -> Result<FullTextRuntime, Error>,
    ) -> Result<Arc<RwLock<FullTextRuntime>>, Error> {
        match self.indexes.entry(Self::key(db_index, index)) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let runtime = Arc::new(RwLock::new(create()?));
                entry.insert(runtime.clone());
                Ok(runtime)
            }
        }
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
        self.outbox_pending.remove(&key);
        self.latest_outbox_seq.remove(&key);
        self.aliases
            .retain(|(db, _), target| *db != db_index || target != index);
        self.query_asts
            .retain(|cached, _| cached.db_index != db_index || cached.index != index);
    }

    pub(super) fn remove_if_incarnation(
        &self,
        db_index: u16,
        index: &str,
        incarnation: u64,
    ) -> bool {
        let key = Self::key(db_index, index);
        let removed = match self.indexes.entry(key.clone()) {
            Entry::Occupied(entry) => {
                let matches = entry
                    .get()
                    .read()
                    .is_ok_and(|runtime| runtime.incarnation == incarnation);
                if matches {
                    entry.remove();
                }
                matches
            }
            Entry::Vacant(_) => false,
        };
        if removed {
            self.outbox_mutations_since_compaction.remove(&key);
            self.outbox_pending.remove(&key);
            self.latest_outbox_seq.remove(&key);
            self.aliases
                .retain(|(db, _), target| *db != db_index || target != index);
            self.query_asts
                .retain(|cached, _| cached.db_index != db_index || cached.index != index);
        }
        removed
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
        self.outbox_mutations_since_compaction
            .retain(|key, _| key.db_index != db_index);
        self.lifecycle_locks
            .retain(|key, _| key.db_index != db_index);
        self.refresh_locks.retain(|key, _| key.db_index != db_index);
        self.source_routes.remove(&db_index);
        self.outbox_pending
            .retain(|key, _| key.db_index != db_index);
        self.latest_outbox_seq
            .retain(|key, _| key.db_index != db_index);
        self.config_values.retain(|(db, _), _| *db != db_index);
        self.aliases.retain(|(db, _), _| *db != db_index);
        self.query_asts
            .retain(|cached, _| cached.db_index != db_index);
    }

    pub(super) fn config_value(
        &self,
        db_index: u16,
        name: &str,
        load: impl FnOnce() -> Result<Option<String>, Error>,
    ) -> Result<Option<String>, Error> {
        match self.config_values.entry((db_index, name.to_string())) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let value = load()?;
                entry.insert(value.clone());
                Ok(value)
            }
        }
    }

    pub(super) fn set_config_value(&self, db_index: u16, name: &str, value: String) {
        self.config_values
            .insert((db_index, name.to_string()), Some(value));
    }

    pub(super) fn alias_target(&self, db_index: u16, alias: &str) -> Option<String> {
        self.aliases
            .get(&(db_index, alias.to_string()))
            .map(|entry| entry.value().clone())
    }

    pub(super) fn set_alias_target(&self, db_index: u16, alias: &str, index: &str) {
        self.aliases
            .insert((db_index, alias.to_string()), index.to_string());
    }

    pub(super) fn remove_alias(&self, db_index: u16, alias: &str) {
        self.aliases.remove(&(db_index, alias.to_string()));
    }

    pub(super) fn query_ast(
        &self,
        db_index: u16,
        index: &str,
        incarnation: u64,
        dialect: u8,
        query: &str,
    ) -> Result<Arc<FullTextQueryAst>, Error> {
        const MAX_CACHED_QUERIES: usize = 4_096;
        let key = FullTextQueryCacheKey {
            db_index,
            index: index.to_string(),
            incarnation,
            dialect,
            query: query.to_string(),
        };
        if let Some(cached) = self.query_asts.get(&key) {
            global_metrics().record_fulltext_query_cache(true);
            return Ok(cached.value().clone());
        }
        let parsed = Arc::new(FullTextQueryParser::new(query, dialect).parse()?);
        if self.query_asts.len() >= MAX_CACHED_QUERIES {
            self.query_asts.clear();
        }
        let cached = self.query_asts.entry(key).or_insert_with(|| parsed.clone());
        global_metrics().record_fulltext_query_cache(false);
        Ok(cached.value().clone())
    }

    pub(super) fn lifecycle_lock(&self, db_index: u16, index: &str) -> Arc<RwLock<()>> {
        self.prune_dead_locks();
        match self.lifecycle_locks.entry(Self::key(db_index, index)) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(RwLock::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(RwLock::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    pub(super) fn refresh_lock(&self, db_index: u16, index: &str) -> Arc<Mutex<()>> {
        self.prune_dead_locks();
        match self.refresh_locks.entry(Self::key(db_index, index)) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    fn prune_dead_locks(&self) {
        const PRUNE_THRESHOLD: usize = 64;
        const PRUNE_INTERVAL: u64 = 64;
        if self.lifecycle_locks.len() + self.refresh_locks.len() < PRUNE_THRESHOLD
            || !self
                .lock_prune_ticks
                .fetch_add(1, AtomicOrdering::Relaxed)
                .is_multiple_of(PRUNE_INTERVAL)
        {
            return;
        }
        self.lifecycle_locks
            .retain(|_, lock| lock.strong_count() > 0);
        self.refresh_locks.retain(|_, lock| lock.strong_count() > 0);
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

    pub(super) fn latest_outbox_seq(&self, db_index: u16, index: &str) -> Option<u64> {
        self.latest_outbox_seq
            .get(&Self::key(db_index, index))
            .map(|entry| *entry.value())
    }

    pub(super) fn note_latest_outbox_seq(&self, db_index: u16, index: &str, seq: u64) {
        let mut latest = self
            .latest_outbox_seq
            .entry(Self::key(db_index, index))
            .or_default();
        *latest = (*latest).max(seq);
    }

    pub(super) fn note_outbox_mutations(
        &self,
        db_index: u16,
        index: &str,
        delta: usize,
        compact_threshold: usize,
    ) -> bool {
        if delta == 0 || compact_threshold == 0 || compact_threshold == usize::MAX {
            return false;
        }
        let mut pending = self
            .outbox_mutations_since_compaction
            .entry(Self::key(db_index, index))
            .or_default();
        *pending = pending.saturating_add(delta);
        if *pending <= compact_threshold {
            return false;
        }
        *pending = 0;
        true
    }
}
