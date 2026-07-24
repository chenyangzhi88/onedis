use super::*;

impl Db {
    pub(crate) fn copy_key_between_dbs(
        store: &KvStore,
        source: DbKeyRef<'_>,
        target: DbKeyRef<'_>,
        replace: bool,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        let DbKeyRef {
            db_index: source_db_index,
            key: source_key,
        } = source;
        let DbKeyRef {
            db_index: target_db_index,
            key: target_key,
        } = target;
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
        let target_raw =
            Self::load_live_raw_for_db_with_backend(&target_store, target_db_index, target_key);
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                target_db_index,
                target_key,
                target_raw,
            );
            if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(target_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    target_db_index,
                    target_key,
                );
            }
        }
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
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
        }
        target_store.write_batch(&batch);
        Ok(true)
    }

    pub(crate) async fn copy_key_between_dbs_async(
        store: &KvStore,
        source: DbKeyRef<'_>,
        target: DbKeyRef<'_>,
        replace: bool,
        version_counter: &VersionCounter,
        ttl_manager: Option<&TtlManager>,
    ) -> Result<bool, Error> {
        let DbKeyRef {
            db_index: source_db_index,
            key: source_key,
        } = source;
        let DbKeyRef {
            db_index: target_db_index,
            key: target_key,
        } = target;
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
        let target_raw = Self::load_live_raw_for_db_with_backend_async(
            &target_store,
            target_db_index,
            target_key,
        )
        .await;
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(
                &mut batch,
                target_db_index,
                target_key,
                target_raw,
            );
            if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(target_raw))
                && header.expire_ms > 0
            {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    target_db_index,
                    target_key,
                );
            }
        }
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
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
        }
        target_store.write_batch_async(&batch).await;
        Ok(true)
    }

    pub fn copy_key_to_db(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        let copied = Self::copy_key_between_dbs(
            &self.store,
            DbKeyRef::new(self.db_index, source_key),
            DbKeyRef::new(target_db_index, target_key),
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )?;
        if copied {
            let raw_key = self.key_layout.main_key(target_db_index, target_key);
            self.record_external_key_mutation(target_db_index, raw_key.clone());
            if !self.store.is_transactional() {
                self.non_transactional_view_for_db(target_db_index)
                    .fulltext_reconcile_committed_keys(&[raw_key], false)?;
            }
        }
        Ok(copied)
    }

    pub async fn copy_key_to_db_async(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        let source_shard = set_write_lock_shard(self.db_index, source_key);
        let target_shard = set_write_lock_shard(target_db_index, target_key);
        if source_shard == target_shard {
            let _guard = self.set_write_locks[source_shard].lock().await;
            self.copy_key_to_db_async_unlocked(target_db_index, source_key, target_key, replace)
                .await
        } else if source_shard < target_shard {
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            let _target_guard = self.set_write_locks[target_shard].lock().await;
            self.copy_key_to_db_async_unlocked(target_db_index, source_key, target_key, replace)
                .await
        } else {
            let _target_guard = self.set_write_locks[target_shard].lock().await;
            let _source_guard = self.set_write_locks[source_shard].lock().await;
            self.copy_key_to_db_async_unlocked(target_db_index, source_key, target_key, replace)
                .await
        }
    }

    async fn copy_key_to_db_async_unlocked(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        let copied = Self::copy_key_between_dbs_async(
            &self.store,
            DbKeyRef::new(self.db_index, source_key),
            DbKeyRef::new(target_db_index, target_key),
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
        .await?;
        if copied {
            let raw_key = self.key_layout.main_key(target_db_index, target_key);
            self.record_external_key_mutation(target_db_index, raw_key.clone());
            if !self.store.is_transactional() {
                self.run_blocking_store_task(move |db| {
                    db.non_transactional_view_for_db(target_db_index)
                        .fulltext_reconcile_committed_keys(&[raw_key], false)
                })
                .await?;
            }
        }
        Ok(copied)
    }
}
