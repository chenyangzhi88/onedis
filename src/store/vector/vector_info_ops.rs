impl Db {
    pub fn vector_card(&self, index: &str) -> Result<u64, Error> {
        match self.read_vector_meta(index) {
            Ok((_, _, meta)) => Ok(meta.doc_count),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub async fn vector_card_async(&self, index: &str) -> Result<u64, Error> {
        match self.read_vector_meta_async(index).await {
            Ok((_, _, meta)) => Ok(meta.doc_count),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub fn vector_dim(&self, index: &str) -> Result<Option<u32>, Error> {
        match self.read_vector_meta(index) {
            Ok((_, _, meta)) => Ok(Some(meta.dim)),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn vector_dim_async(&self, index: &str) -> Result<Option<u32>, Error> {
        match self.read_vector_meta_async(index).await {
            Ok((_, _, meta)) => Ok(Some(meta.dim)),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn vector_ids(&self, index: &str) -> Result<Vec<String>, Error> {
        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let mut ids = self
            .store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(_, raw)| decode_record::<VectorDocRecord>(&raw).ok())
            .filter(|doc| !doc.deleted)
            .map(|doc| doc.id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub(super) fn visit_vector_elements<F>(&self, index: &str, mut visitor: F) -> Result<(), Error>
    where
        F: FnMut(String, Vec<f32>) -> Result<bool, Error>,
    {
        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(()),
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let mut result = Ok(());
        self.store.scan_range_raw_visit(
            &prefix,
            super::prefix_exclusive_upper_bound(&prefix),
            usize::MAX,
            |_, raw| {
                let doc = match decode_record::<VectorDocRecord>(raw) {
                    Ok(doc) => doc,
                    Err(error) => {
                        result = Err(error);
                        return false;
                    }
                };
                if doc.deleted {
                    return true;
                }
                match visitor(doc.id, doc.vector) {
                    Ok(keep_going) => keep_going,
                    Err(error) => {
                        result = Err(error);
                        false
                    }
                }
            },
        );
        result
    }

    pub async fn vector_ids_async(&self, index: &str) -> Result<Vec<String>, Error> {
        let (_, version, _) = match self.read_vector_meta_async(index).await {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let mut ids = self
            .store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(_, raw)| decode_record::<VectorDocRecord>(&raw).ok())
            .filter(|doc| !doc.deleted)
            .map(|doc| doc.id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub fn vector_random_ids(&self, index: &str, count: Option<i64>) -> Result<Vec<String>, Error> {
        let requested = count
            .map(i64::unsigned_abs)
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Error::msg("ERR invalid vector count"))?
            .unwrap_or(1);
        if requested == 0 {
            return Ok(Vec::new());
        }

        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let upper = super::prefix_exclusive_upper_bound(&prefix);
        let mut random = VectorRandom::new();

        if count.is_some_and(|count| count < 0) {
            let mut cardinality = 0usize;
            self.store
                .scan_range_raw_visit(&prefix, upper.clone(), usize::MAX, |_, raw| {
                    if decode_record::<VectorDocRecord>(raw).is_ok_and(|doc| !doc.deleted) {
                        cardinality = cardinality.saturating_add(1);
                    }
                    true
                });
            if cardinality == 0 {
                return Ok(Vec::new());
            }
            let mut targets = (0..requested)
                .map(|output_index| (random.index(cardinality), output_index))
                .collect::<Vec<_>>();
            targets.sort_unstable_by_key(|target| target.0);
            let mut output = vec![None; requested];
            let mut live_index = 0usize;
            let mut target_index = 0usize;
            self.store
                .scan_range_raw_visit(&prefix, upper, usize::MAX, |_, raw| {
                    let Ok(doc) = decode_record::<VectorDocRecord>(raw) else {
                        return true;
                    };
                    if doc.deleted {
                        return true;
                    }
                    while target_index < targets.len() && targets[target_index].0 == live_index {
                        output[targets[target_index].1] = Some(doc.id.clone());
                        target_index += 1;
                    }
                    live_index += 1;
                    target_index < targets.len()
                });
            return Ok(output.into_iter().flatten().collect());
        }

        let mut reservoir = Vec::with_capacity(requested);
        let mut seen = 0usize;
        self.store
            .scan_range_raw_visit(&prefix, upper, usize::MAX, |_, raw| {
                let Ok(doc) = decode_record::<VectorDocRecord>(raw) else {
                    return true;
                };
                if doc.deleted {
                    return true;
                }
                seen = seen.saturating_add(1);
                if reservoir.len() < requested {
                    reservoir.push(doc.id);
                } else {
                    let replacement = random.index(seen);
                    if replacement < requested {
                        reservoir[replacement] = doc.id;
                    }
                }
                true
            });
        Ok(reservoir)
    }

    pub async fn vector_random_ids_async(
        &self,
        index: &str,
        count: Option<i64>,
    ) -> Result<Vec<String>, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_random_ids(&index, count))
            .await
    }

    pub fn vector_links(
        &self,
        index: &str,
        id: &str,
    ) -> Result<Option<VectorLinkLayers>, Error> {
        let (_, version, meta) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(None),
            Err(err) => return Err(err),
        };
        self.ensure_vector_runtime(index, version, &meta)?;
        let runtime = self
            .vector_runtimes
            .get(self.db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        Ok(runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .links(id))
    }

    pub async fn vector_links_async(
        &self,
        index: &str,
        id: &str,
    ) -> Result<Option<VectorLinkLayers>, Error> {
        let index = index.to_string();
        let id = id.to_string();
        self.run_blocking_store_task(move |db| db.vector_links(&index, &id))
            .await
    }

    pub fn vector_info(&self, index: &str) -> Result<Vec<(String, String)>, Error> {
        let (_, version, meta) = self.read_vector_meta(index)?;
        Ok(vec![
            ("dim".to_string(), meta.dim.to_string()),
            (
                "distance".to_string(),
                distance_name(meta.distance).to_string(),
            ),
            ("doc_count".to_string(), meta.doc_count.to_string()),
            ("schema_fields".to_string(), meta.schema.len().to_string()),
            ("m".to_string(), meta.m.to_string()),
            (
                "ef_construction".to_string(),
                meta.ef_construction.to_string(),
            ),
            ("ef_runtime".to_string(), meta.ef_runtime.to_string()),
            (
                "hnsw_nodes".to_string(),
                self.vector_runtime_len(index, version, meta.doc_count)
                    .to_string(),
            ),
            (
                "snapshot_doc_version".to_string(),
                meta.snapshot_doc_version.to_string(),
            ),
        ])
    }

    pub async fn vector_info_async(&self, index: &str) -> Result<Vec<(String, String)>, Error> {
        let (_, version, meta) = self.read_vector_meta_async(index).await?;
        Ok(vec![
            ("dim".to_string(), meta.dim.to_string()),
            (
                "distance".to_string(),
                distance_name(meta.distance).to_string(),
            ),
            ("doc_count".to_string(), meta.doc_count.to_string()),
            ("schema_fields".to_string(), meta.schema.len().to_string()),
            ("m".to_string(), meta.m.to_string()),
            (
                "ef_construction".to_string(),
                meta.ef_construction.to_string(),
            ),
            ("ef_runtime".to_string(), meta.ef_runtime.to_string()),
            (
                "hnsw_nodes".to_string(),
                self.vector_runtime_len(index, version, meta.doc_count)
                    .to_string(),
            ),
            (
                "snapshot_doc_version".to_string(),
                meta.snapshot_doc_version.to_string(),
            ),
        ])
    }

    pub fn vector_observability_snapshot(&self) -> VectorObservabilitySnapshot {
        let mut snapshot = VectorObservabilitySnapshot::default();
        let now = super::now_ms();
        for key in self.logical_keys() {
            let Some(raw) = self.store.get_raw(&self.mk(&key)) else {
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                continue;
            };
            if header.type_tag == TYPE_VECTOR && (header.expire_ms == 0 || now < header.expire_ms) {
                snapshot.indexes += 1;
            }
        }
        snapshot
    }
}

struct VectorRandom {
    state: u64,
}

impl VectorRandom {
    fn new() -> Self {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let random_state = std::collections::hash_map::RandomState::new();
        let mut hasher = random_state.build_hasher();
        NONCE
            .fetch_add(1, AtomicOrdering::Relaxed)
            .hash(&mut hasher);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        let state = hasher.finish();
        Self {
            state: if state == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                state
            },
        }
    }

    fn index(&mut self, upper: usize) -> usize {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        (value % upper as u64) as usize
    }
}
