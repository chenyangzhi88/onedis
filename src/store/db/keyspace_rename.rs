use super::*;

impl Db {
    pub async fn rename_key_async(
        &self,
        old_key: &str,
        new_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        let old_shard = set_write_lock_shard(self.db_index, old_key);
        let new_shard = set_write_lock_shard(self.db_index, new_key);
        if old_shard == new_shard {
            let _guard = self.set_write_locks[old_shard].lock().await;
            return self
                .rename_key_async_unlocked(old_key, new_key, replace)
                .await;
        }
        if old_shard < new_shard {
            let _old_guard = self.set_write_locks[old_shard].lock().await;
            let _new_guard = self.set_write_locks[new_shard].lock().await;
            self.rename_key_async_unlocked(old_key, new_key, replace)
                .await
        } else {
            let _new_guard = self.set_write_locks[new_shard].lock().await;
            let _old_guard = self.set_write_locks[old_shard].lock().await;
            self.rename_key_async_unlocked(old_key, new_key, replace)
                .await
        }
    }

    async fn rename_key_async_unlocked(
        &self,
        old_key: &str,
        new_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        if old_key == new_key {
            if Self::load_live_raw_for_db_with_backend_async(&self.store, self.db_index, old_key)
                .await
                .is_some()
            {
                return Ok(true);
            }
            return Err(Error::msg("ERR no such key"));
        }

        let old_key_bytes = self.mk(old_key);
        let new_key_bytes = self.mk(new_key);
        for _ in 0..64 {
            self.expire_if_needed_async(old_key).await;
            self.expire_if_needed_async(new_key).await;

            let source_observed = self.store.get_raw_observed_async(&old_key_bytes).await;
            let Some(source_raw) = source_observed.value() else {
                return Err(Error::msg("ERR no such key"));
            };
            let target_observed = self.store.get_raw_observed_async(&new_key_bytes).await;
            if target_observed.value().is_some() && !replace {
                return Ok(false);
            }

            let mut batch = WriteBatch::new();
            if let Some(target_raw) = target_observed.value() {
                Self::delete_structure_for_db_to_batch(
                    &mut batch,
                    self.db_index,
                    new_key,
                    target_raw,
                );
                if let Some(header) = decode_meta_header(target_raw)
                    && header.expire_ms > 0
                {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        header.expire_ms,
                        self.db_index,
                        new_key,
                    );
                }
            }
            Self::copy_structure_between_dbs_to_batch_async(
                &mut batch,
                StructureCopyContext::new(
                    &self.store,
                    &self.store,
                    DbKeyRef::new(self.db_index, old_key),
                    DbKeyRef::new(self.db_index, new_key),
                    source_raw,
                    &self.version_counter,
                ),
            )
            .await;
            Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, old_key, source_raw);
            if let Some(header) = decode_meta_header(source_raw)
                && header.expire_ms > 0
            {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    old_key,
                );
                self.ttl_manager
                    .add_to_batch(&mut batch, header.expire_ms, self.db_index, new_key);
            }

            let conditions = [
                CompareCondition::from_observed(&source_observed),
                CompareCondition::from_observed(&target_observed),
            ];
            if self
                .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
                .await?
            {
                self.changes.fetch_add(1, Ordering::Relaxed);
                if !self.store.is_transactional() {
                    let old_raw_key = old_key_bytes.clone();
                    let new_raw_key = new_key_bytes.clone();
                    self.run_blocking_store_task(move |db| {
                        db.non_transactional_view()
                            .fulltext_reconcile_committed_keys(&[old_raw_key, new_raw_key], true)
                    })
                    .await?;
                }
                return Ok(true);
            }
        }
        Err(Error::msg("ERR rename write conflict"))
    }
}
