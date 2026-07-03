use super::*;

impl Db {
    pub(crate) fn copy_key_between_dbs(
        store: &KvStore,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        replace: bool,
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
            &source_store,
            &target_store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
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
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        replace: bool,
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
        Self::copy_structure_between_dbs_to_batch_async(
            &source_store,
            &target_store,
            &mut batch,
            source_db_index,
            source_key,
            target_db_index,
            target_key,
            &source_raw,
            version_counter,
        )
        .await;
        if let (Some(ttl_manager), Some(header)) = (ttl_manager, decode_meta_header(&source_raw))
            && header.expire_ms > 0
        {
            ttl_manager.add_to_batch(&mut batch, header.expire_ms, target_db_index, target_key);
        }
        target_store.write_batch(&batch);
        Ok(true)
    }

    pub fn copy_key_to_db(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        Self::copy_key_between_dbs(
            &self.store,
            self.db_index,
            source_key,
            target_db_index,
            target_key,
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
    }

    pub async fn copy_key_to_db_async(
        &self,
        target_db_index: u16,
        source_key: &str,
        target_key: &str,
        replace: bool,
    ) -> Result<bool, Error> {
        Self::copy_key_between_dbs_async(
            &self.store,
            self.db_index,
            source_key,
            target_db_index,
            target_key,
            replace,
            &self.version_counter,
            Some(&self.ttl_manager),
        )
        .await
    }
}
