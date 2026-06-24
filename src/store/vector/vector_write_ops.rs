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
}
