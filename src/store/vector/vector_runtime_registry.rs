#[derive(Default)]
pub struct VectorRuntimeRegistry {
    indexes: DashMap<VectorRuntimeKey, Arc<RwLock<VectorRuntime>>>,
    write_locks: DashMap<VectorWriteLockKey, Arc<Mutex<()>>>,
    dirty_indexes: DashMap<VectorRuntimeKey, ()>,
    active_runtimes: AtomicUsize,
    maintenance_cursor: AtomicUsize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct VectorRuntimeKey {
    db_index: u16,
    index: String,
    version: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct VectorWriteLockKey {
    db_index: u16,
    index: String,
}

impl VectorRuntimeRegistry {
    pub(crate) fn has_active_runtimes(&self) -> bool {
        self.active_runtimes.load(AtomicOrdering::Acquire) != 0
    }

    fn insert_runtime(&self, key: VectorRuntimeKey, runtime: Arc<RwLock<VectorRuntime>>) {
        match self.indexes.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                entry.insert(runtime);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // Publish the conservative non-empty state before making the runtime visible.
                // Readers may do one harmless extra reconcile, but can never miss a live runtime.
                self.active_runtimes.fetch_add(1, AtomicOrdering::Release);
                entry.insert(runtime);
            }
        }
    }

    fn key(db_index: u16, index: &str, version: u64) -> VectorRuntimeKey {
        VectorRuntimeKey {
            db_index,
            index: index.to_string(),
            version,
        }
    }

    fn reset(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        config: VectorRuntimeConfig,
    ) {
        self.insert_runtime(
            Self::key(db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::new(
                config,
                1,
            ))),
        );
    }

    fn write_lock(&self, db_index: u16, index: &str) -> Arc<Mutex<()>> {
        self.write_locks
            .entry(VectorWriteLockKey {
                db_index,
                index: index.to_string(),
            })
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    fn get(&self, db_index: u16, index: &str, version: u64) -> Option<Arc<RwLock<VectorRuntime>>> {
        self.indexes
            .get(&Self::key(db_index, index, version))
            .map(|entry| entry.value().clone())
    }

    fn take_dirty_indexes_for_db(
        &self,
        db_index: u16,
        limit: usize,
    ) -> Vec<(String, u64)> {
        let mut keys = self
            .dirty_indexes
            .iter()
            .filter(|entry| entry.key().db_index == db_index)
            .map(|entry| (entry.key().index.clone(), entry.key().version))
            .collect::<Vec<_>>();
        keys.sort_unstable();
        let count = keys.len().min(limit);
        if count == 0 {
            return Vec::new();
        }
        let start = self
            .maintenance_cursor
            .fetch_add(count, AtomicOrdering::AcqRel)
            % keys.len();
        let keys = (0..count)
            .map(|offset| keys[(start + offset) % keys.len()].clone())
            .collect::<Vec<_>>();
        for (index, version) in &keys {
            self.dirty_indexes
                .remove(&Self::key(db_index, index, *version));
        }
        keys
    }

    fn indexes_for_db(&self, db_index: u16) -> Vec<(String, u64)> {
        self.indexes
            .iter()
            .filter(|entry| entry.key().db_index == db_index)
            .map(|entry| (entry.key().index.clone(), entry.key().version))
            .collect()
    }

    fn mark_dirty(&self, db_index: u16, index: &str, version: u64) {
        self.dirty_indexes
            .insert(Self::key(db_index, index, version), ());
    }

    fn upsert(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        config: VectorRuntimeConfig,
        entry: VectorRuntimeEntry,
    ) -> Result<(), Error> {
        let runtime = match self.indexes.entry(Self::key(db_index, index, version)) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let runtime = Arc::new(RwLock::new(VectorRuntime::new(
                    config,
                    1,
                )));
                self.active_runtimes.fetch_add(1, AtomicOrdering::Release);
                entry.insert(runtime.clone());
                runtime
            }
        };
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .upsert_with_attrs(entry.id, entry.doc_version, entry.vector, entry.attrs_json)?;
        self.mark_dirty(db_index, index, version);
        Ok(())
    }

    fn config(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
    ) -> Result<Option<VectorRuntimeConfig>, Error> {
        self.get(db_index, index, version)
            .map(|runtime| {
                runtime
                    .read()
                    .map(|runtime| runtime.config.clone())
                    .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))
            })
            .transpose()
    }

    fn mark_deleted(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        doc: VectorDocRecord,
    ) {
        if let Some(runtime) = self.get(db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.mark_deleted(doc);
            drop(runtime);
            self.mark_dirty(db_index, index, version);
        }
    }

    fn apply_docs(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        docs: Vec<VectorDocRecord>,
    ) -> Result<(), Error> {
        let runtime = self
            .get(db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let mut runtime = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        for doc in docs {
            runtime.apply_doc(doc);
        }
        drop(runtime);
        self.mark_dirty(db_index, index, version);
        Ok(())
    }

    fn set_attrs(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        id: &str,
        attrs_json: String,
    ) {
        if let Some(runtime) = self.get(db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.set_attrs(id, attrs_json);
        }
    }

    fn reconcile_docs(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        docs: Vec<VectorDocRecord>,
        flushed_through: u64,
    ) -> Result<(), Error> {
        let runtime = self
            .get(db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .reconcile_docs(docs, flushed_through);
        Ok(())
    }

    fn remove(&self, db_index: u16, index: &str, version: u64) {
        if self
            .indexes
            .remove(&Self::key(db_index, index, version))
            .is_some()
        {
            self.active_runtimes.fetch_sub(1, AtomicOrdering::AcqRel);
        }
        self.dirty_indexes
            .remove(&Self::key(db_index, index, version));
        self.cleanup_write_lock_if_idle(db_index, index);
    }

    fn retain_index_version(&self, db_index: u16, index: &str, version: Option<u64>) {
        let mut removed = 0usize;
        self.indexes.retain(|key, _| {
            let keep = key.db_index != db_index
                || key.index != index
                || version.is_some_and(|version| key.version == version);
            if !keep {
                removed += 1;
            }
            keep
        });
        self.dirty_indexes.retain(|key, _| {
            key.db_index != db_index
                || key.index != index
                || version.is_some_and(|version| key.version == version)
        });
        if removed != 0 {
            // Keep the count conservative until every removed runtime is no longer visible.
            self.active_runtimes
                .fetch_sub(removed, AtomicOrdering::AcqRel);
        }
        if version.is_none() {
            self.cleanup_write_lock_if_idle(db_index, index);
        }
    }

    fn cleanup_write_lock_if_idle(&self, db_index: u16, index: &str) {
        let key = VectorWriteLockKey {
            db_index,
            index: index.to_string(),
        };
        if let dashmap::mapref::entry::Entry::Occupied(entry) = self.write_locks.entry(key)
            && Arc::strong_count(entry.get()) == 1
        {
            entry.remove();
        }
    }

    pub(crate) fn remove_expired(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
    ) {
        self.remove(db_index, index, version);
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        let mut removed = 0usize;
        self.indexes.retain(|key, _| {
            let keep = key.db_index != db_index;
            if !keep {
                removed += 1;
            }
            keep
        });
        if removed != 0 {
            self.active_runtimes
                .fetch_sub(removed, AtomicOrdering::AcqRel);
        }
        self.write_locks
            .retain(|key, _| key.db_index != db_index);
        self.dirty_indexes
            .retain(|key, _| key.db_index != db_index);
    }
}
