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
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
    ) {
        self.indexes.insert(
            Self::key(db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::new(
                dim,
                distance,
                m,
                ef_construction,
                initial_cap,
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
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
        id: String,
        doc_version: u64,
        vector: Vec<f32>,
    ) -> Result<(), Error> {
        let runtime = self
            .indexes
            .entry(Self::key(db_index, index, version))
            .or_insert_with(|| {
                Arc::new(RwLock::new(VectorRuntime::new(
                    dim,
                    distance,
                    m,
                    ef_construction,
                    initial_cap,
                    1,
                )))
            })
            .value()
            .clone();
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .upsert(id, doc_version, vector)
    }

    fn mark_deleted(&self, db_index: u16, index: &str, version: u64, id: &str) {
        if let Some(runtime) = self.get(db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.mark_deleted(id);
        }
    }

    fn remove(&self, db_index: u16, index: &str, version: u64) {
        self.indexes.remove(&Self::key(db_index, index, version));
    }
}
