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

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        );
        Self::delete_structure_for_db_to_batch(
            &mut batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw)) {
            if header.expire_ms > 0 {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
        }
        store.write_batch(&batch);
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

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(store, source_db_index, source_key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(store, target_db_index, target_key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch_async(
            store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(
            &mut batch,
            source_db_index,
            source_key,
            &source_raw,
        );
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw)) {
            if header.expire_ms > 0 {
                ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    source_db_index,
                    source_key,
                );
                ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
            }
        }
        store.write_batch(&batch);
        Ok(true)
    }

    pub fn move_key_to_db(&self, target_db_index: u16, key: &str) -> Result<bool, Error> {
        if self.db_index == target_db_index {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(&self.store, target_db_index, key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            target_db_index,
            key,
            &source_raw,
            &self.version_counter,
        );
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
            && header.expire_ms > 0
        {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            self.ttl_manager
                .add_to_batch(&mut batch, header.expire_ms, target_db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }

    pub async fn move_key_to_db_async(
        &self,
        target_db_index: u16,
        key: &str,
    ) -> Result<bool, Error> {
        if self.db_index == target_db_index {
            return Ok(false);
        }

        let Some(source_raw) =
            Self::load_live_raw_for_db_with_backend(&self.store, self.db_index, key)
        else {
            return Ok(false);
        };

        if Self::load_live_raw_for_db_with_backend(&self.store, target_db_index, key).is_some() {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        Self::copy_structure_between_dbs_to_batch_async(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            target_db_index,
            key,
            &source_raw,
            &self.version_counter,
        )
        .await;
        Self::delete_structure_for_db_to_batch(&mut batch, self.db_index, key, &source_raw);
        if let Some(header) = decode_meta_header(&source_raw)
            && header.expire_ms > 0
        {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            self.ttl_manager
                .add_to_batch(&mut batch, header.expire_ms, target_db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }
}
