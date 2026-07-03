impl Db {
    pub async fn rename_key_async(
        &self,
        old_key: &str,
        new_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        if old_key == new_key {
            if Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, old_key)
                .is_some()
            {
                return Ok(true);
            }
            return Err(Error::msg("ERR no such key"));
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, old_key)
        else {
            return Err(Error::msg("ERR no such key"));
        };
        let target_raw =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, new_key);
        if target_raw.is_some() && !replace {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if let Some(target_raw) = target_raw.as_deref() {
            Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, new_key, target_raw);
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
            &self.store,
            &self.store,
            &mut batch,
            self.db_index,
            old_key,
            self.db_index,
            new_key,
            &source_raw,
            &self.version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, old_key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
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
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}
