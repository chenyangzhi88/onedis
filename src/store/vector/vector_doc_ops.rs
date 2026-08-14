impl Db {
    pub fn vector_element(&self, index: &str, id: &str) -> Result<Option<VectorElement>, Error> {
        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(None),
            Err(err) => return Err(err),
        };
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            if let Some(doc) = runtime.memtable.get(id) {
                return Ok((!doc.deleted).then(|| VectorElement {
                    vector: doc.vector.clone(),
                    attrs_json: doc.attrs_json.clone(),
                }));
            }
        }
        let Some(raw) = self.store.get_raw(&vector_doc_key(
            self.key_layout,
            self.db_index,
            index,
            version,
            id,
        )) else {
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
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            if let Some(doc) = runtime.memtable.get(id) {
                return Ok((!doc.deleted).then(|| VectorElement {
                    vector: doc.vector.clone(),
                    attrs_json: doc.attrs_json.clone(),
                }));
            }
        }
        let Some(raw) = self
            .store
            .get_raw_async(&vector_doc_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                id,
            ))
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

    /// Fetch several vector documents with one metadata read and one storage multi-get.
    pub async fn vector_elements_async(
        &self,
        index: &str,
        ids: &[String],
    ) -> Result<Vec<Option<VectorElement>>, Error> {
        let (_, version, _) = match self.read_vector_meta_async(index).await {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(vec![None; ids.len()]);
            }
            Err(err) => return Err(err),
        };
        let keys = ids
            .iter()
            .map(|id| vector_doc_key(self.key_layout, self.db_index, index, version, id))
            .collect::<Vec<_>>();
        self.store
            .multi_get_raw_async(&keys)
            .await
            .into_iter()
            .map(|raw| {
                let Some(raw) = raw else {
                    return Ok(None);
                };
                let doc = decode_record::<VectorDocRecord>(&raw)?;
                if doc.deleted {
                    Ok(None)
                } else {
                    Ok(Some(VectorElement {
                        vector: doc.vector,
                        attrs_json: doc.attrs_json,
                    }))
                }
            })
            .collect()
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
        let (expire_ms, version, meta, expected_marker, expected_meta) = match self
            .read_vector_meta_observed(index)
        {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(false),
            Err(err) => return Err(err),
        };
        self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        let key = vector_doc_key(self.key_layout, self.db_index, index, version, id);
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
        let attr_context = VectorAttrIndexContext {
            layout: self.key_layout,
            db_index: self.db_index,
            index,
            version,
            schema: &meta.schema,
            doc_id: &doc.id,
        };
        delete_attr_index_entries_to_batch(&mut batch, &attr_context, &old_attrs)?;
        put_attr_index_entries_to_batch(&mut batch, &attr_context, doc.doc_version, &new_attrs)?;
        doc.attrs_json = new_attrs_json.clone();
        put_vector_marker_to_batch(
            &mut batch,
            self.key_layout,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
            meta.internal,
        )?;
        batch.put(&key, &encode_record(&doc)?)?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;
        self.vector_runtimes
            .set_attrs(self.db_index, index, version, id, new_attrs_json);
        Ok(true)
    }

    pub async fn vector_set_attrs_async(
        &self,
        index: &str,
        id: &str,
        attrs_json: Option<String>,
    ) -> Result<bool, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        let id = id.to_string();
        self.run_blocking_store_task(move |db| db.vector_set_attrs(&index, &id, attrs_json))
            .await
    }
}
