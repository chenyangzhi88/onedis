use super::*;

impl Db {
    pub(in crate::store::db) fn delete_main_key_with_ttl_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        expire_ms: u64,
    ) {
        batch.delete(&self.mk(key));
        self.ttl_manager
            .remove_known_to_batch(batch, expire_ms, self.db_index, key);
    }

    pub(in crate::store::db) fn remove_internal(
        &self,
        key: &str,
        count_change: bool,
    ) -> Option<Structure> {
        let key_bytes = self.mk(key);
        let raw = self.store.get_raw(&key_bytes)?;

        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
        }

        if let Some(meta) = decode_list_meta(&raw) {
            let list = self.read_list_items(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
            self.write_batch_if_not_empty(&batch);
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::List(list));
        }

        if let Some(meta) = decode_stream_meta(&raw) {
            let entries = self.read_stream_entries(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_STREAM);
            self.write_batch_if_not_empty(&batch);
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::Stream(entries));
        }

        let (_, version, structure) = decode_entry(&raw)?;
        let type_tag = structure_type_tag(&structure);
        let result = match &structure {
            Structure::Hash(_) => {
                let hash = self.read_hash_fields(key, version);
                Some(Structure::Hash(hash))
            }
            Structure::SortedSet(_) => {
                let set = self.read_zset_members(key, version);
                Some(Structure::SortedSet(set))
            }
            Structure::Set(_) => {
                let set = self.read_set_members(key, version);
                Some(Structure::Set(set))
            }
            Structure::List(_) => {
                let list = self.read_list_items(key, version);
                Some(Structure::List(list))
            }
            Structure::Stream(_) => {
                let entries = self.read_stream_entries(key, version);
                Some(Structure::Stream(entries))
            }
            _ => Some(structure),
        };

        delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, type_tag);
        if type_tag == TYPE_JSON {
            delete_json_nodes_to_batch(&self.store, &mut batch, self.db_index, key, version);
        }
        match type_tag {
            TYPE_HASH => {
                if let Err(error) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                    log::error!("failed to enqueue fulltext delete for {key}: {error}");
                    return None;
                }
            }
            TYPE_JSON => {
                if let Err(error) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                    log::error!("failed to enqueue fulltext JSON delete for {key}: {error}");
                    return None;
                }
            }
            _ => {}
        }
        self.write_batch_if_not_empty(&batch);
        let refresh = match type_tag {
            TYPE_HASH => self.fulltext_request_refresh(key),
            TYPE_JSON => self.fulltext_request_json_refresh(key),
            _ => Ok(()),
        };
        if let Err(error) = refresh {
            log::error!("failed to refresh fulltext delete for {key}: {error}");
        }
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(in crate::store::db) async fn remove_internal_async(
        &self,
        key: &str,
        count_change: bool,
    ) -> Option<Structure> {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let raw = observed.value()?;
            let header = decode_meta_header(raw)?;

            let result = if let Some(meta) = decode_list_meta(raw) {
                Structure::List(self.read_list_items_async(key, meta.version).await)
            } else if let Some(meta) = decode_stream_meta(raw) {
                Structure::Stream(self.read_stream_entries_async(key, meta.version).await)
            } else {
                let (_, version, structure) = decode_entry(raw)?;
                match structure {
                    Structure::Hash(_) => {
                        Structure::Hash(self.read_hash_fields_async(key, version).await)
                    }
                    Structure::SortedSet(_) => {
                        Structure::SortedSet(self.read_zset_members_async(key, version).await)
                    }
                    Structure::Set(_) => {
                        Structure::Set(self.read_set_members_async(key, version).await)
                    }
                    Structure::List(_) => {
                        Structure::List(self.read_list_items_async(key, version).await)
                    }
                    Structure::Stream(_) => {
                        Structure::Stream(self.read_stream_entries_async(key, version).await)
                    }
                    structure => structure,
                }
            };

            let mut batch = WriteBatch::new();
            batch.delete(&key_bytes);
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            if header.type_tag == TYPE_JSON {
                delete_json_nodes_to_batch_async(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                )
                .await;
            }
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(error) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)
                    {
                        log::error!("failed to enqueue fulltext delete for {key}: {error}");
                        return None;
                    }
                }
                TYPE_JSON => {
                    if let Err(error) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key)
                    {
                        log::error!("failed to enqueue fulltext JSON delete for {key}: {error}");
                        return None;
                    }
                }
                _ => {}
            }
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => {
                    let refresh = match header.type_tag {
                        TYPE_HASH => self.fulltext_request_refresh(key),
                        TYPE_JSON => self.fulltext_request_json_refresh(key),
                        _ => Ok(()),
                    };
                    if let Err(error) = refresh {
                        log::error!("failed to refresh fulltext delete for {key}: {error}");
                    }
                    if count_change {
                        self.changes.fetch_add(1, Ordering::Relaxed);
                    }
                    return Some(result);
                }
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to remove key {key}: {error}");
                    return None;
                }
            }
        }
        log::warn!("gave up removing repeatedly modified key {key}");
        None
    }

    pub(in crate::store::db) fn delete_key_internal(&self, key: &str, count_change: bool) -> bool {
        let key_bytes = self.mk(key);
        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return false;
        };
        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            if header.type_tag == TYPE_JSON {
                delete_json_nodes_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                );
            }
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext delete for {key}: {err}");
                        return false;
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext JSON delete for {key}: {err}");
                        return false;
                    }
                }
                _ => {}
            }
        }
        self.write_batch_if_not_empty(&batch);
        if let Some(header) = decode_meta_header(&raw) {
            let refresh = match header.type_tag {
                TYPE_HASH => self.fulltext_request_refresh(key),
                TYPE_JSON => self.fulltext_request_json_refresh(key),
                _ => Ok(()),
            };
            if let Err(err) = refresh {
                log::error!("failed to refresh fulltext delete for {key}: {err}");
            }
        }
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    pub(in crate::store::db) async fn delete_key_internal_async(
        &self,
        key: &str,
        count_change: bool,
    ) -> bool {
        let key_bytes = self.mk(key);
        for _ in 0..64 {
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let Some(raw) = observed.value() else {
                return false;
            };
            let Some(header) = decode_meta_header(raw) else {
                return false;
            };
            let mut batch = WriteBatch::new();
            batch.delete(&key_bytes);
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            if header.type_tag == TYPE_JSON {
                delete_json_nodes_to_batch_async(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                )
                .await;
            }
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext delete for {key}: {err}");
                        return false;
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext JSON delete for {key}: {err}");
                        return false;
                    }
                }
                _ => {}
            }
            match self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await
            {
                Ok(true) => {
                    let refresh = match header.type_tag {
                        TYPE_HASH => self.fulltext_request_refresh(key),
                        TYPE_JSON => self.fulltext_request_json_refresh(key),
                        _ => Ok(()),
                    };
                    if let Err(err) = refresh {
                        log::error!("failed to refresh fulltext delete for {key}: {err}");
                    }
                    if count_change {
                        self.changes.fetch_add(1, Ordering::Relaxed);
                    }
                    return true;
                }
                Ok(false) => continue,
                Err(error) => {
                    log::error!("failed to delete key {key}: {error}");
                    return false;
                }
            }
        }
        log::warn!("gave up deleting repeatedly modified key {key}");
        false
    }
}
