#[derive(Default)]
pub struct VectorRuntimeRegistry {
    indexes: DashMap<VectorRuntimeKey, Arc<RwLock<VectorRuntime>>>,
    write_locks: DashMap<VectorWriteLockKey, Arc<Mutex<()>>>,
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
        self.indexes.insert(
            Self::key(db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::new(
                config.dim,
                config.distance,
                config.m,
                config.ef_construction,
                config.initial_cap,
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

    fn upsert(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        config: VectorRuntimeConfig,
        entry: VectorRuntimeEntry,
    ) -> Result<(), Error> {
        let runtime = self
            .indexes
            .entry(Self::key(db_index, index, version))
            .or_insert_with(|| {
                Arc::new(RwLock::new(VectorRuntime::new(
                    config.dim,
                    config.distance,
                    config.m,
                    config.ef_construction,
                    config.initial_cap,
                    1,
                )))
            })
            .value()
            .clone();
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .upsert(entry.id, entry.doc_version, entry.vector)
    }

    fn mark_deleted(&self, db_index: u16, index: &str, version: u64, id: &str) {
        if let Some(runtime) = self.get(db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.mark_deleted(id);
        }
    }

    fn reconcile_docs(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        docs: Vec<VectorDocRecord>,
    ) -> Result<(), Error> {
        let runtime = self
            .get(db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .reconcile_docs(docs)
    }

    fn remove(&self, db_index: u16, index: &str, version: u64) {
        self.indexes.remove(&Self::key(db_index, index, version));
        self.write_locks.remove(&VectorWriteLockKey {
            db_index,
            index: index.to_string(),
        });
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
        self.write_locks
            .retain(|key, _| key.db_index != db_index);
    }
}
