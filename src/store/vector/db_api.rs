impl Db {
    pub fn vector_create(&self, index: &str, options: VectorCreateOptions) -> Result<(), Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let raw_key = self.mk(index);
        if let Some(raw) = self.store.get_raw(&raw_key) {
            let header =
                decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
            if header.type_tag == TYPE_VECTOR {
                return Err(Error::msg("ERR vector index already exists"));
            }
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let distance = parse_distance(&options.distance)?;
        if options.dim == 0 {
            return Err(Error::msg("ERR vector dimension must be positive"));
        }
        validate_schema(&options.schema)?;
        let segment_max_docs = options
            .segment_max_docs
            .filter(|value| *value > 0)
            .unwrap_or_else(vector_segment_max_docs);
        let m = normalize_hnsw_m(options.m)?;
        let ef_construction = options
            .ef_construction
            .unwrap_or(DEFAULT_HNSW_EF_CONSTRUCTION as usize)
            .max(m);
        let ef_runtime = options
            .ef_runtime
            .unwrap_or(DEFAULT_HNSW_EF_RUNTIME as usize)
            .max(1);
        let initial_cap = options
            .initial_cap
            .unwrap_or(segment_max_docs as usize)
            .max(1);

        let version = self.next_persisted_version();
        let meta = VectorIndexMeta {
            dim: options.dim as u32,
            distance,
            schema: options.schema,
            m: m as u32,
            ef_construction: ef_construction as u32,
            ef_runtime: ef_runtime as u32,
            initial_cap: initial_cap as u64,
            next_doc_version: 1,
            doc_count: 0,
            next_segment_id: 1,
            snapshot_doc_version: 0,
            segment_max_docs,
        };
        let marker = Structure::VectorCollection(Vector {
            dimension: options.dim,
            vectors: Default::default(),
            norms: Default::default(),
        });
        let mut batch = WriteBatch::new();
        batch.put(&raw_key, &encode_entry(&marker, 0, version));
        batch.put(
            &vector_meta_key(self.db_index, index, version),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.reset(
            self.db_index,
            index,
            version,
            options.dim,
            distance,
            m,
            ef_construction,
            initial_cap,
        );
        Ok(())
    }

    pub async fn vector_create_async(
        &self,
        index: &str,
        options: VectorCreateOptions,
    ) -> Result<(), Error> {
        self.vector_create(index, options)
    }

    pub fn vector_add(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
    ) -> Result<(), Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta) = self.read_vector_meta(index)?;
        if expire_ms > 0 && super::now_ms() >= expire_ms {
            return Err(Error::msg("ERR vector index does not exist"));
        }
        validate_vector(&vector, meta.dim as usize)?;
        validate_vector_for_distance(&vector, meta.distance)?;
        let attrs_json = attrs_json.unwrap_or_else(|| "{}".to_string());
        let attrs = parse_attrs(&attrs_json)?;
        validate_attrs_against_schema(&meta.schema, &attrs)?;

        let old_doc = self
            .store
            .get_raw(&vector_doc_key(self.db_index, index, version, id))
            .and_then(|raw| decode_record::<VectorDocRecord>(&raw).ok());
        let doc_version = meta.next_doc_version;
        meta.next_doc_version = meta.next_doc_version.saturating_add(1);
        if old_doc.as_ref().is_none_or(|doc| doc.deleted) {
            meta.doc_count = meta.doc_count.saturating_add(1);
        }

        let doc = VectorDocRecord {
            id: id.to_string(),
            doc_version,
            vector,
            attrs_json: attrs_json.clone(),
            deleted: false,
        };

        let mut batch = WriteBatch::new();
        put_vector_marker_to_batch(
            &mut batch,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
        );
        batch.put(
            &vector_meta_key(self.db_index, index, version),
            &encode_record(&meta)?,
        );
        batch.put(
            &vector_doc_key(self.db_index, index, version, id),
            &encode_record(&doc)?,
        );
        if let Some(old_doc) = old_doc {
            if let Ok(old_attrs) = parse_attrs(&old_doc.attrs_json) {
                delete_attr_index_entries_to_batch(
                    &mut batch,
                    self.db_index,
                    index,
                    version,
                    &meta.schema,
                    &old_doc.id,
                    old_doc.doc_version,
                    &old_attrs,
                );
            }
        }
        put_attr_index_entries_to_batch(
            &mut batch,
            self.db_index,
            index,
            version,
            &meta.schema,
            id,
            doc_version,
            &attrs,
        )?;
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.upsert(
            self.db_index,
            index,
            version,
            meta.dim as usize,
            meta.distance,
            meta.m as usize,
            meta.ef_construction as usize,
            meta.initial_cap as usize,
            id.to_string(),
            doc_version,
            doc.vector.clone(),
        )?;
        self.maybe_freeze_vector_segment(index, version, &mut meta)?;
        Ok(())
    }

    pub async fn vector_add_async(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
    ) -> Result<(), Error> {
        self.vector_add(index, id, vector, attrs_json)
    }

    pub fn vector_add_autocreate(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
        m: Option<usize>,
        ef_runtime: Option<usize>,
    ) -> Result<bool, Error> {
        let existed = self.vector_element(index, id)?.is_some();
        match self.vector_add(index, id, vector.clone(), attrs_json.clone()) {
            Ok(()) => return Ok(!existed),
            Err(err) if err.to_string() == "ERR vector index does not exist" => {}
            Err(err) => return Err(err),
        }
        if let Err(err) = self.vector_create(
            index,
            VectorCreateOptions {
                dim: vector.len(),
                distance: "L2".to_string(),
                schema: Vec::new(),
                segment_max_docs: None,
                m,
                ef_construction: None,
                ef_runtime,
                initial_cap: None,
            },
        ) {
            if err.to_string() != "ERR vector index already exists" {
                return Err(err);
            }
        }
        self.vector_add(index, id, vector, attrs_json)?;
        Ok(true)
    }

    pub async fn vector_add_autocreate_async(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
        m: Option<usize>,
        ef_runtime: Option<usize>,
    ) -> Result<bool, Error> {
        self.vector_add_autocreate(index, id, vector, attrs_json, m, ef_runtime)
    }

    pub fn vector_del(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta) = self.read_vector_meta(index)?;
        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        for id in ids {
            let key = vector_doc_key(self.db_index, index, version, id);
            let Some(raw) = self.store.get_raw(&key) else {
                continue;
            };
            let mut doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.deleted {
                continue;
            }
            if let Ok(attrs) = parse_attrs(&doc.attrs_json) {
                delete_attr_index_entries_to_batch(
                    &mut batch,
                    self.db_index,
                    index,
                    version,
                    &meta.schema,
                    &doc.id,
                    doc.doc_version,
                    &attrs,
                );
            }
            doc.doc_version = meta.next_doc_version;
            meta.next_doc_version = meta.next_doc_version.saturating_add(1);
            doc.deleted = true;
            batch.put(&key, &encode_record(&doc)?);
            self.vector_runtimes
                .mark_deleted(self.db_index, index, version, &doc.id);
            deleted += 1;
        }
        if deleted > 0 {
            meta.doc_count = meta.doc_count.saturating_sub(deleted as u64);
            put_vector_marker_to_batch(
                &mut batch,
                self.db_index,
                index,
                expire_ms,
                version,
                meta.dim,
            );
            batch.put(
                &vector_meta_key(self.db_index, index, version),
                &encode_record(&meta)?,
            );
            self.write_batch_if_not_empty(&batch);
            self.gc_obsolete_vector_segments(index, version)?;
        }
        Ok(deleted)
    }

    pub async fn vector_del_async(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        self.vector_del(index, ids)
    }

    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let (_, version, meta) = self.read_vector_meta(index)?;
        validate_vector(query, meta.dim as usize)?;
        validate_vector_for_distance(query, meta.distance)?;
        let filters = options
            .filter
            .as_deref()
            .map(parse_filter)
            .transpose()?
            .unwrap_or_default();
        let allow_doc_ids = self.indexed_filter_doc_ids(index, version, &meta, &filters)?;
        self.ensure_vector_runtime(index, version, &meta)?;
        let mut used_hnsw = false;
        let mut results = Vec::new();
        if let Some(candidates) = self.hnsw_candidates(
            index,
            version,
            &meta,
            query,
            &options,
            allow_doc_ids.as_ref(),
        )? {
            used_hnsw = true;
            results = self.vector_results_from_candidates(
                index, version, &meta, query, &options, &filters, candidates,
            )?;
            sort_and_limit_results(&mut results, options.k);
        }
        if !used_hnsw || results.len() < options.k {
            results = self.vector_exact_results(
                index,
                version,
                &meta,
                query,
                &options,
                &filters,
                allow_doc_ids.as_ref(),
            )?;
        }
        Ok(window_results(results, &options))
    }

    pub async fn vector_search_async(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let (_, version, meta) = self.read_vector_meta_async(index).await?;
        validate_vector(query, meta.dim as usize)?;
        validate_vector_for_distance(query, meta.distance)?;
        let filters = options
            .filter
            .as_deref()
            .map(parse_filter)
            .transpose()?
            .unwrap_or_default();
        let allow_doc_ids = self.indexed_filter_doc_ids(index, version, &meta, &filters)?;
        self.ensure_vector_runtime_async(index, version, &meta)
            .await?;
        let mut used_hnsw = false;
        let mut results = Vec::new();
        if let Some(candidates) = self.hnsw_candidates(
            index,
            version,
            &meta,
            query,
            &options,
            allow_doc_ids.as_ref(),
        )? {
            used_hnsw = true;
            results = self
                .vector_results_from_candidates_async(
                    index, version, &meta, query, &options, &filters, candidates,
                )
                .await?;
            sort_and_limit_results(&mut results, options.k);
        }
        if !used_hnsw || results.len() < options.k {
            results = self
                .vector_exact_results_async(
                    index,
                    version,
                    &meta,
                    query,
                    &options,
                    &filters,
                    allow_doc_ids.as_ref(),
                )
                .await?;
        }
        Ok(window_results(results, &options))
    }

    pub fn vector_element(&self, index: &str, id: &str) -> Result<Option<VectorElement>, Error> {
        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(None),
            Err(err) => return Err(err),
        };
        let Some(raw) = self
            .store
            .get_raw(&vector_doc_key(self.db_index, index, version, id))
        else {
            return Ok(None);
        };
        let doc = decode_record::<VectorDocRecord>(&raw)?;
        if doc.deleted {
            return Ok(None);
        }
        Ok(Some(VectorElement {
            vector: doc.vector,
            attrs_json: doc.attrs_json,
        }))
    }

    pub async fn vector_element_async(
        &self,
        index: &str,
        id: &str,
    ) -> Result<Option<VectorElement>, Error> {
        let (_, version, _) = match self.read_vector_meta_async(index).await {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(None),
            Err(err) => return Err(err),
        };
        let Some(raw) = self
            .store
            .get_raw_async(&vector_doc_key(self.db_index, index, version, id))
            .await
        else {
            return Ok(None);
        };
        let doc = decode_record::<VectorDocRecord>(&raw)?;
        if doc.deleted {
            return Ok(None);
        }
        Ok(Some(VectorElement {
            vector: doc.vector,
            attrs_json: doc.attrs_json,
        }))
    }

    pub fn vector_set_attrs(
        &self,
        index: &str,
        id: &str,
        attrs_json: Option<String>,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, meta) = self.read_vector_meta(index)?;
        let key = vector_doc_key(self.db_index, index, version, id);
        let Some(raw) = self.store.get_raw(&key) else {
            return Ok(false);
        };
        let mut doc = decode_record::<VectorDocRecord>(&raw)?;
        if doc.deleted {
            return Ok(false);
        }
        let new_attrs_json = attrs_json.unwrap_or_else(|| "{}".to_string());
        let new_attrs = parse_attrs(&new_attrs_json)?;
        validate_attrs_against_schema(&meta.schema, &new_attrs)?;
        let old_attrs = parse_attrs(&doc.attrs_json)?;
        let mut batch = WriteBatch::new();
        delete_attr_index_entries_to_batch(
            &mut batch,
            self.db_index,
            index,
            version,
            &meta.schema,
            &doc.id,
            doc.doc_version,
            &old_attrs,
        );
        put_attr_index_entries_to_batch(
            &mut batch,
            self.db_index,
            index,
            version,
            &meta.schema,
            &doc.id,
            doc.doc_version,
            &new_attrs,
        )?;
        doc.attrs_json = new_attrs_json;
        put_vector_marker_to_batch(
            &mut batch,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
        );
        batch.put(&key, &encode_record(&doc)?);
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }

    pub async fn vector_set_attrs_async(
        &self,
        index: &str,
        id: &str,
        attrs_json: Option<String>,
    ) -> Result<bool, Error> {
        self.vector_set_attrs(index, id, attrs_json)
    }

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
        let prefix = vector_doc_prefix(self.db_index, index, version);
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

    pub async fn vector_ids_async(&self, index: &str) -> Result<Vec<String>, Error> {
        let (_, version, _) = match self.read_vector_meta_async(index).await {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.db_index, index, version);
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

    pub fn vector_drop(&self, index: &str) -> Result<usize, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (_, version, _) = self.read_vector_meta(index)?;
        let mut batch = WriteBatch::new();
        batch.delete(&main_key(self.db_index, index));
        delete_vector_namespace_to_batch(&self.store, &mut batch, self.db_index, index, version);
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.remove(self.db_index, index, version);
        Ok(1)
    }

    pub async fn vector_drop_async(&self, index: &str) -> Result<usize, Error> {
        self.vector_drop(index)
    }

    pub fn vector_rebuild(&self, index: &str) -> Result<(), Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta) = self.read_vector_meta(index)?;
        let mut batch = WriteBatch::new();
        delete_vector_segments_to_batch(&self.store, &mut batch, self.db_index, index, version);
        meta.next_segment_id = 1;
        meta.snapshot_doc_version = 0;
        put_vector_marker_to_batch(
            &mut batch,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
        );
        batch.put(
            &vector_meta_key(self.db_index, index, version),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.reset(
            self.db_index,
            index,
            version,
            meta.dim as usize,
            meta.distance,
            meta.m as usize,
            meta.ef_construction as usize,
            meta.initial_cap as usize,
        );
        let prefix = vector_doc_prefix(self.db_index, index, version);
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if !doc.deleted {
                self.vector_runtimes.upsert(
                    self.db_index,
                    index,
                    version,
                    meta.dim as usize,
                    meta.distance,
                    meta.m as usize,
                    meta.ef_construction as usize,
                    meta.initial_cap as usize,
                    doc.id,
                    doc.doc_version,
                    doc.vector,
                )?;
            }
        }
        Ok(())
    }

    pub async fn vector_rebuild_async(&self, index: &str) -> Result<(), Error> {
        self.vector_rebuild(index)
    }

    pub fn vector_compact(&self, index: &str) -> Result<(), Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (_, version, meta) = self.read_vector_meta(index)?;
        self.ensure_vector_runtime(index, version, &meta)?;
        self.gc_obsolete_vector_segments(index, version)
    }

    pub async fn vector_compact_async(&self, index: &str) -> Result<(), Error> {
        self.vector_compact(index)
    }

    fn maybe_freeze_vector_segment(
        &self,
        index: &str,
        version: u64,
        meta: &mut VectorIndexMeta,
    ) -> Result<(), Error> {
        let max_docs = meta.segment_max_docs.max(1);
        let latest_doc_version = meta.next_doc_version.saturating_sub(1);
        if latest_doc_version <= meta.snapshot_doc_version {
            return Ok(());
        }
        if meta.doc_count < max_docs && latest_doc_version - meta.snapshot_doc_version < max_docs {
            return Ok(());
        }
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(());
        };
        let Some((mut segment, snapshot)) = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .freeze_active()
        else {
            return Ok(());
        };
        let segment_id = segment.segment_id;
        let graph_key = vector_graph_key(self.db_index, index, version, segment_id);
        segment.graph_key = graph_key.clone();
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .set_segment_graph_key(segment_id, graph_key);
        meta.next_segment_id = meta.next_segment_id.max(segment_id.saturating_add(1));
        meta.snapshot_doc_version = meta.snapshot_doc_version.max(segment.max_doc_version);
        persist_vector_segment_snapshot(
            &self.store,
            self.db_index,
            index,
            version,
            &segment,
            &encode_record(&snapshot)?,
        )?;
        self.gc_obsolete_vector_segments(index, version)?;
        Ok(())
    }

    fn gc_obsolete_vector_segments(&self, index: &str, version: u64) -> Result<(), Error> {
        let segments = self.vector_runtime_segment_entries(index, version)?;
        let mut obsolete_ids = HashSet::new();
        let mut obsolete_graph_keys = Vec::new();
        for (segment_id, graph_key, entries) in segments {
            let mut has_live_doc = false;
            for (id, doc_version) in entries {
                let Some(raw) =
                    self.store
                        .get_raw(&vector_doc_key(self.db_index, index, version, &id))
                else {
                    continue;
                };
                let doc = decode_record::<VectorDocRecord>(&raw)?;
                if !doc.deleted && doc.doc_version == doc_version {
                    has_live_doc = true;
                    break;
                }
            }
            if !has_live_doc {
                obsolete_ids.insert(segment_id);
                obsolete_graph_keys.push(graph_key);
            }
        }
        if obsolete_ids.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for segment_id in &obsolete_ids {
            batch.delete(&vector_segment_key(
                self.db_index,
                index,
                version,
                *segment_id,
            ));
        }
        for graph_key in obsolete_graph_keys {
            if !graph_key.is_empty() {
                batch.delete(&graph_key);
            }
        }
        self.write_batch_if_not_empty(&batch);
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.remove_segments(&obsolete_ids);
        }
        Ok(())
    }

    fn vector_runtime_segment_entries(
        &self,
        index: &str,
        version: u64,
    ) -> Result<Vec<(u64, Vec<u8>, Vec<(String, u64)>)>, Error> {
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(Vec::new());
        };
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        Ok(runtime
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.meta.segment_id,
                    segment.meta.graph_key.clone(),
                    segment
                        .graph
                        .nodes
                        .iter()
                        .filter(|node| !node.deleted)
                        .map(|node| (node.id.clone(), node.doc_version))
                        .collect(),
                )
            })
            .collect())
    }

    fn read_vector_meta(&self, index: &str) -> Result<(u64, u64, VectorIndexMeta), Error> {
        self.expire_if_needed(index);
        let Some(raw) = self.store.get_raw(&main_key(self.db_index, index)) else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) =
            self.store
                .get_raw(&vector_meta_key(self.db_index, index, header.version))
        else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        Ok((
            header.expire_ms,
            header.version,
            decode_record::<VectorIndexMeta>(&meta_raw)?,
        ))
    }

    fn ensure_vector_runtime(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(), Error> {
        if self
            .vector_runtimes
            .get(self.db_index, index, version)
            .is_some()
        {
            return Ok(());
        }
        let (segments, replay_after, next_segment_id) =
            self.load_vector_graph_segments(index, version, meta)?;
        self.vector_runtimes.indexes.insert(
            VectorRuntimeRegistry::key(self.db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::with_segments(
                meta.dim as usize,
                meta.distance,
                meta.m as usize,
                meta.ef_construction as usize,
                meta.initial_cap as usize,
                next_segment_id,
                segments,
            ))),
        );
        let prefix = vector_doc_prefix(self.db_index, index, version);
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.doc_version <= replay_after {
                continue;
            }
            if doc.deleted {
                self.vector_runtimes
                    .mark_deleted(self.db_index, index, version, &doc.id);
            } else {
                self.vector_runtimes.upsert(
                    self.db_index,
                    index,
                    version,
                    meta.dim as usize,
                    meta.distance,
                    meta.m as usize,
                    meta.ef_construction as usize,
                    meta.initial_cap as usize,
                    doc.id,
                    doc.doc_version,
                    doc.vector,
                )?;
            }
        }
        Ok(())
    }

    async fn ensure_vector_runtime_async(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(), Error> {
        if self
            .vector_runtimes
            .get(self.db_index, index, version)
            .is_some()
        {
            return Ok(());
        }
        let (segments, replay_after, next_segment_id) = self
            .load_vector_graph_segments_async(index, version, meta)
            .await?;
        self.vector_runtimes.indexes.insert(
            VectorRuntimeRegistry::key(self.db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::with_segments(
                meta.dim as usize,
                meta.distance,
                meta.m as usize,
                meta.ef_construction as usize,
                meta.initial_cap as usize,
                next_segment_id,
                segments,
            ))),
        );
        let prefix = vector_doc_prefix(self.db_index, index, version);
        for (_, raw) in self.store.scan_prefix_raw_async(&prefix).await {
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.doc_version <= replay_after {
                continue;
            }
            if doc.deleted {
                self.vector_runtimes
                    .mark_deleted(self.db_index, index, version, &doc.id);
            } else {
                self.vector_runtimes.upsert(
                    self.db_index,
                    index,
                    version,
                    meta.dim as usize,
                    meta.distance,
                    meta.m as usize,
                    meta.ef_construction as usize,
                    meta.initial_cap as usize,
                    doc.id,
                    doc.doc_version,
                    doc.vector,
                )?;
            }
        }
        Ok(())
    }

    fn load_vector_graph_segments(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(Vec<VectorSegmentRuntime>, u64, u64), Error> {
        let prefix = vector_segment_prefix(self.db_index, index, version);
        let mut segments = Vec::new();
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            let segment = decode_record::<VectorSegmentMeta>(&raw)?;
            if segment.graph_key.is_empty() {
                continue;
            }
            let Some(snapshot_raw) = self.store.get_raw(&segment.graph_key) else {
                continue;
            };
            let snapshot = decode_record::<HnswGraphSnapshot>(&snapshot_raw)?;
            segments.push(VectorSegmentRuntime {
                meta: segment,
                graph: HnswGraph::from_snapshot(snapshot)?,
            });
        }
        segments.sort_by_key(|segment| segment.meta.segment_id);
        let replay_after = segments
            .iter()
            .map(|segment| segment.meta.max_doc_version)
            .max()
            .unwrap_or(0);
        let next_segment_id = meta.next_segment_id.max(
            segments
                .iter()
                .map(|segment| segment.meta.segment_id.saturating_add(1))
                .max()
                .unwrap_or(1),
        );
        Ok((segments, replay_after, next_segment_id))
    }

    async fn load_vector_graph_segments_async(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(Vec<VectorSegmentRuntime>, u64, u64), Error> {
        let prefix = vector_segment_prefix(self.db_index, index, version);
        let mut segments = Vec::new();
        for (_, raw) in self.store.scan_prefix_raw_async(&prefix).await {
            let segment = decode_record::<VectorSegmentMeta>(&raw)?;
            if segment.graph_key.is_empty() {
                continue;
            }
            let Some(snapshot_raw) = self.store.get_raw_async(&segment.graph_key).await else {
                continue;
            };
            let snapshot = decode_record::<HnswGraphSnapshot>(&snapshot_raw)?;
            segments.push(VectorSegmentRuntime {
                meta: segment,
                graph: HnswGraph::from_snapshot(snapshot)?,
            });
        }
        segments.sort_by_key(|segment| segment.meta.segment_id);
        let replay_after = segments
            .iter()
            .map(|segment| segment.meta.max_doc_version)
            .max()
            .unwrap_or(0);
        let next_segment_id = meta.next_segment_id.max(
            segments
                .iter()
                .map(|segment| segment.meta.segment_id.saturating_add(1))
                .max()
                .unwrap_or(1),
        );
        Ok((segments, replay_after, next_segment_id))
    }

    fn hnsw_candidates(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        query: &[f32],
        options: &VectorSearchOptions,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Option<Vec<VectorCandidate>>, Error> {
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(None);
        };
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        if runtime.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        let candidate_limit = options.k.saturating_mul(32).max(64).min(runtime.len());
        let ef = options
            .ef
            .unwrap_or(meta.ef_runtime as usize)
            .max(candidate_limit)
            .max(options.k);
        runtime
            .search(query, candidate_limit, ef, allow_doc_ids)
            .map(Some)
    }

    fn vector_results_from_candidates(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        query: &[f32],
        options: &VectorSearchOptions,
        filters: &[FilterPredicate],
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results = Vec::new();
        for candidate in candidates {
            let Some(raw) = self.store.get_raw(&vector_doc_key(
                self.db_index,
                index,
                version,
                &candidate.id,
            )) else {
                continue;
            };
            if let Some(result) = doc_to_search_result(
                raw,
                meta,
                query,
                &options.with_attrs,
                filters,
                Some(candidate.doc_version),
            )? {
                results.push(result);
            }
        }
        Ok(results)
    }

    async fn vector_results_from_candidates_async(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        query: &[f32],
        options: &VectorSearchOptions,
        filters: &[FilterPredicate],
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results = Vec::new();
        for candidate in candidates {
            let Some(raw) = self
                .store
                .get_raw_async(&vector_doc_key(
                    self.db_index,
                    index,
                    version,
                    &candidate.id,
                ))
                .await
            else {
                continue;
            };
            if let Some(result) = doc_to_search_result(
                raw,
                meta,
                query,
                &options.with_attrs,
                filters,
                Some(candidate.doc_version),
            )? {
                results.push(result);
            }
        }
        Ok(results)
    }

    fn vector_exact_results(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        query: &[f32],
        options: &VectorSearchOptions,
        filters: &[FilterPredicate],
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results = Vec::new();
        if let Some(allow_doc_ids) = allow_doc_ids {
            for id in allow_doc_ids {
                if let Some(raw) =
                    self.store
                        .get_raw(&vector_doc_key(self.db_index, index, version, id))
                    && let Some(result) =
                        doc_to_search_result(raw, meta, query, &options.with_attrs, filters, None)?
                {
                    results.push(result);
                }
            }
        } else {
            let prefix = vector_doc_prefix(self.db_index, index, version);
            for (_, raw) in self.store.scan_prefix_raw(&prefix) {
                if let Some(result) =
                    doc_to_search_result(raw, meta, query, &options.with_attrs, filters, None)?
                {
                    results.push(result);
                }
            }
        }
        sort_and_limit_results(&mut results, options.k);
        Ok(results)
    }

    async fn vector_exact_results_async(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        query: &[f32],
        options: &VectorSearchOptions,
        filters: &[FilterPredicate],
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results = Vec::new();
        if let Some(allow_doc_ids) = allow_doc_ids {
            for id in allow_doc_ids {
                if let Some(raw) = self
                    .store
                    .get_raw_async(&vector_doc_key(self.db_index, index, version, id))
                    .await
                    && let Some(result) =
                        doc_to_search_result(raw, meta, query, &options.with_attrs, filters, None)?
                {
                    results.push(result);
                }
            }
        } else {
            let prefix = vector_doc_prefix(self.db_index, index, version);
            for (_, raw) in self.store.scan_prefix_raw_async(&prefix).await {
                if let Some(result) =
                    doc_to_search_result(raw, meta, query, &options.with_attrs, filters, None)?
                {
                    results.push(result);
                }
            }
        }
        sort_and_limit_results(&mut results, options.k);
        Ok(results)
    }

    fn vector_runtime_len(&self, index: &str, version: u64, fallback: u64) -> usize {
        self.vector_runtimes
            .get(self.db_index, index, version)
            .and_then(|graph| graph.read().ok().map(|graph| graph.len()))
            .unwrap_or(fallback as usize)
    }

    fn indexed_filter_doc_ids(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        filters: &[FilterPredicate],
    ) -> Result<Option<HashSet<String>>, Error> {
        let mut allow: Option<HashSet<String>> = None;
        for predicate in filters {
            let Some(field) = indexed_filter_field(meta, predicate) else {
                continue;
            };
            let doc_ids = match predicate {
                FilterPredicate::TagEq(_, value) => {
                    self.doc_ids_for_tag_value(index, version, field, value)
                }
                FilterPredicate::TagIn(_, values) => {
                    let mut ids = HashSet::new();
                    for value in values {
                        ids.extend(self.doc_ids_for_tag_value(index, version, field, value));
                    }
                    ids
                }
                FilterPredicate::NumericCmp(_, op, value) => {
                    self.doc_ids_for_numeric_cmp(index, version, field, *op, *value)?
                }
            };
            allow = Some(match allow {
                Some(existing) => existing.intersection(&doc_ids).cloned().collect(),
                None => doc_ids,
            });
        }
        Ok(allow)
    }

    fn doc_ids_for_tag_value(
        &self,
        index: &str,
        version: u64,
        field: &str,
        value: &str,
    ) -> HashSet<String> {
        let prefix = vector_tag_prefix(self.db_index, index, version, field, value);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(key, _)| String::from_utf8(key.get(prefix.len()..)?.to_vec()).ok())
            .collect()
    }

    fn doc_ids_for_numeric_cmp(
        &self,
        index: &str,
        version: u64,
        field: &str,
        op: NumericOp,
        expected: f64,
    ) -> Result<HashSet<String>, Error> {
        let prefix = vector_numeric_field_prefix(self.db_index, index, version, field);
        let mut ids = HashSet::new();
        for (key, _) in self.store.scan_prefix_raw(&prefix) {
            let Some(suffix) = key.get(prefix.len()..) else {
                continue;
            };
            if suffix.len() < 8 {
                continue;
            }
            let mut encoded = [0u8; 8];
            encoded.copy_from_slice(&suffix[..8]);
            let actual = unsortable_f64(u64::from_be_bytes(encoded));
            let matches = match op {
                NumericOp::Gt => actual > expected,
                NumericOp::Ge => actual >= expected,
                NumericOp::Lt => actual < expected,
                NumericOp::Le => actual <= expected,
            };
            if matches && let Ok(id) = String::from_utf8(suffix[8..].to_vec()) {
                ids.insert(id);
            }
        }
        Ok(ids)
    }

    async fn read_vector_meta_async(
        &self,
        index: &str,
    ) -> Result<(u64, u64, VectorIndexMeta), Error> {
        self.expire_if_needed_async(index).await;
        let Some(raw) = self
            .store
            .get_raw_async(&main_key(self.db_index, index))
            .await
        else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) = self
            .store
            .get_raw_async(&vector_meta_key(self.db_index, index, header.version))
            .await
        else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        Ok((
            header.expire_ms,
            header.version,
            decode_record::<VectorIndexMeta>(&meta_raw)?,
        ))
    }


}
