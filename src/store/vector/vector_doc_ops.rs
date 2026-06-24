impl Db {
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
}
