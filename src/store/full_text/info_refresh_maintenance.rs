impl Db {
    pub(crate) fn fulltext_clear_runtimes_for_db(&self) {
        self.fulltext_runtimes.remove_db(self.db_index);
        if let Err(err) = delete_fulltext_aggregate_cursors_for_db(self.db_index) {
            log::error!("failed to clear fulltext cursors for DB {}: {err}", self.db_index);
        }
    }

    pub(crate) fn fulltext_maintenance_tick(&self) -> Result<(), Error> {
        let indexes = self
            .read_all_fulltext_metas()?
            .into_iter()
            .map(|(index, meta)| (index, meta.state))
            .collect::<Vec<_>>();
        for (index, state) in indexes {
            let meta = self.read_fulltext_meta_direct(&index)?;
            if self.fulltext_index_expired(&index, &meta) {
                self.fulltext_purge_index(&index, &meta)?;
                continue;
            }
            if matches!(state, FullTextIndexState::Dirty) {
                self.fulltext_rebuild_index(&index)?;
            } else {
                self.fulltext_refresh_index(&index, true)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn fulltext_maintenance_tick_async(&self) -> Result<(), Error> {
        self.run_blocking_store_task(|db| db.fulltext_maintenance_tick())
            .await
    }

    pub(crate) fn fulltext_request_refresh(&self, key: &str) -> Result<(), Error> {
        self.fulltext_request_refresh_for_source(key, FullTextSourceType::Hash)
    }

    pub(crate) fn fulltext_request_json_refresh(&self, key: &str) -> Result<(), Error> {
        self.fulltext_request_refresh_for_source(key, FullTextSourceType::Json)
    }

    pub(crate) fn fulltext_reconcile_committed_keys(
        &self,
        raw_keys: &[Vec<u8>],
        refresh_immediately: bool,
    ) -> Result<(), Error> {
        let mut keys = HashSet::new();
        for raw_key in raw_keys {
            let Ok(key) = String::from_utf8(raw_key.clone()) else {
                continue;
            };
            keys.insert(key);
        }
        for key in keys {
            let mut batch = WriteBatch::new();
            match self
                .store
                .get_raw(&self.mk(&key))
                .and_then(|raw| decode_meta_header(&raw))
                .map(|header| header.type_tag)
            {
                Some(TYPE_HASH) => {
                    self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_json_delete_to_batch(&mut batch, &key)?;
                }
                Some(TYPE_JSON) => {
                    self.fulltext_enqueue_json_upsert_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_hash_delete_to_batch(&mut batch, &key)?;
                }
                _ => {
                    self.fulltext_enqueue_hash_delete_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_json_delete_to_batch(&mut batch, &key)?;
                }
            }
            if batch.count() > 0 {
                self.store.write_batch_direct(&batch);
                if refresh_immediately {
                    self.fulltext_request_refresh(&key)?;
                    self.fulltext_request_json_refresh(&key)?;
                }
            }
        }
        Ok(())
    }
}
