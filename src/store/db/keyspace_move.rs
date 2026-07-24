use super::*;

impl Db {
    pub(crate) fn move_key_between_dbs(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let source_store = store.for_db_index(source_db_index);
        let target_store = store.for_db_index(target_db_index);

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&source_store, source_db_index, source_key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(&target_store, target_db_index, target_key)
            .is_some()
        {
            return Ok(false);
        }

        if source_db_index == target_db_index {
            let mut batch = WriteBatch::new();
            Self::copy_structure_between_dbs_to_batch(
                &mut batch,
                StructureCopyContext::new(
                    &source_store,
                    &target_store,
                    DbKeyRef::new(source_db_index, source_key),
                    DbKeyRef::new(target_db_index, target_key),
                    &source_raw,
                    version_counter,
                ),
            );
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                source_db_index,
                source_key,
                &source_raw,
            );
            if let (Some(ttl_manager), Some(header)) =
                (ttl_manager, decode_meta_header(&source_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
            target_store.write_batch(&batch);
            return Ok(true);
        }

        let mut target_batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch(
            &mut target_batch,
            StructureCopyContext::new(
                &source_store,
                &target_store,
                DbKeyRef::new(source_db_index, source_key),
                DbKeyRef::new(target_db_index, target_key),
                &source_raw,
                version_counter,
            ),
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(
                &mut target_batch,
                header.expire_ms,
                target_db_index,
                target_key,
            );
        }
        target_store.write_batch(&target_batch);

        let mut source_batch = WriteBatch::new();
        Self::delete_structure_for_db_to_batch(
            &mut source_batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.remove_known_to_batch(
                &mut source_batch,
                header.expire_ms,
                source_db_index,
                source_key,
            );
        }
        source_store.write_batch(&source_batch);
        Ok(true)
    }

    #[allow(dead_code)]
    pub(crate) async fn move_key_between_dbs_async(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        if source_db_index == target_db_index && source_key == target_key {
            return Ok(false);
        }

        let source_store = store.for_db_index(source_db_index);
        let target_store = store.for_db_index(target_db_index);

        let Some(source_raw) = Self::load_live_raw_for_db_with_backend_async(
            &source_store,
            source_db_index,
            source_key,
        )
        .await
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend_async(&target_store, target_db_index, target_key)
            .await
            .is_some()
        {
            return Ok(false);
        }

        if source_db_index == target_db_index {
            let mut batch = WriteBatch::new();
            Self::copy_structure_between_dbs_to_batch_async(
                &mut batch,
                StructureCopyContext::new(
                    &source_store,
                    &target_store,
                    DbKeyRef::new(source_db_index, source_key),
                    DbKeyRef::new(target_db_index, target_key),
                    &source_raw,
                    version_counter,
                ),
            )
            .await;
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                source_db_index,
                source_key,
                &source_raw,
            );
            if let (Some(ttl_manager), Some(header)) =
                (ttl_manager, decode_meta_header(&source_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
            target_store.write_batch_async(&batch).await;
            return Ok(true);
        }

        let mut target_batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch_async(
            &mut target_batch,
            StructureCopyContext::new(
                &source_store,
                &target_store,
                DbKeyRef::new(source_db_index, source_key),
                DbKeyRef::new(target_db_index, target_key),
                &source_raw,
                version_counter,
            ),
        )
        .await;
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(
                &mut target_batch,
                header.expire_ms,
                target_db_index,
                target_key,
            );
        }
        target_store.write_batch_async(&target_batch).await;

        let mut source_batch = WriteBatch::new();
        Self::delete_structure_for_db_to_batch(
            &mut source_batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.remove_known_to_batch(
                &mut source_batch,
                header.expire_ms,
                source_db_index,
                source_key,
            );
        }
        source_store.write_batch_async(&source_batch).await;
        Ok(true)
    }

    pub fn move_key_to_db(&self, target_db_index: u16, key: &str) -> Result<bool, Error> {
        let moved = Self::move_key_between_dbs(
            &self.store,
            self.db_index,
            key,
            target_db_index,
            key,
            &self.version_counter,
            Some(&self.ttl_manager),
        )?;
        if moved {
            let source_raw_key = self.key_layout.main_key(self.db_index, key);
            let target_raw_key = self.key_layout.main_key(target_db_index, key);
            self.record_external_key_mutation(self.db_index, source_raw_key.clone());
            self.record_external_key_mutation(target_db_index, target_raw_key.clone());
            if !self.store.is_transactional() {
                self.non_transactional_view()
                    .fulltext_reconcile_committed_keys(&[source_raw_key], true)?;
                self.non_transactional_view_for_db(target_db_index)
                    .fulltext_reconcile_committed_keys(&[target_raw_key], false)?;
            }
        }
        Ok(moved)
    }

    pub async fn move_key_to_db_async(
        &self,
        target_db_index: u16,
        key: &str,
    ) -> Result<bool, Error> {
        let source_shard = set_write_lock_shard(self.db_index, key);
        let target_shard = set_write_lock_shard(target_db_index, key);
        if source_shard == target_shard {
            let _guard = self.set_write_locks[source_shard].lock().await;
            self.move_key_to_db_async_unlocked(target_db_index, key)
                .await
        } else if source_shard < target_shard {
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            let _target_guard = self.set_write_locks[target_shard].lock().await;
            self.move_key_to_db_async_unlocked(target_db_index, key)
                .await
        } else {
            let _target_guard = self.set_write_locks[target_shard].lock().await;
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            self.move_key_to_db_async_unlocked(target_db_index, key)
                .await
        }
    }

    async fn move_key_to_db_async_unlocked(
        &self,
        target_db_index: u16,
        key: &str,
    ) -> Result<bool, Error> {
        let moved = Self::move_key_between_dbs_async(
            &self.store,
            self.db_index,
            key,
            target_db_index,
            key,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
        .await?;
        if moved {
            let source_raw_key = self.key_layout.main_key(self.db_index, key);
            let target_raw_key = self.key_layout.main_key(target_db_index, key);
            self.record_external_key_mutation(self.db_index, source_raw_key.clone());
            self.record_external_key_mutation(target_db_index, target_raw_key.clone());
            if !self.store.is_transactional() {
                self.run_blocking_store_task(move |db| {
                    db.non_transactional_view()
                        .fulltext_reconcile_committed_keys(&[source_raw_key], true)?;
                    db.non_transactional_view_for_db(target_db_index)
                        .fulltext_reconcile_committed_keys(&[target_raw_key], false)
                })
                .await?;
            }
        }
        Ok(moved)
    }
}
