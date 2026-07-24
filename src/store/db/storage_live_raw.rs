use super::*;

impl Db {
    pub(in crate::store::db) fn load_live_raw_for_db_with_backend(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Option<Vec<u8>> {
        let key_bytes = main_key(db_index, key);
        if let Some(raw) = store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 && now_ms() >= expire_ms {
                let mut batch = WriteBatch::new();
                Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, &raw);
                store.write_batch(&batch);
                return None;
            }
            return Some(raw);
        }
        None
    }

    pub(in crate::store::db) async fn load_live_raw_for_db_with_backend_async(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Option<Vec<u8>> {
        let key_bytes = main_key(db_index, key);
        for _ in 0..64 {
            let observed = store.get_raw_observed_async(&key_bytes).await;
            let raw = observed.value()?;
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 || now_ms() < expire_ms {
                return Some(raw.to_vec());
            }

            let mut batch = WriteBatch::new();
            Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, raw);
            match store
                .compare_and_write_batch_async(&[observed.condition()], &batch)
                .await
            {
                Ok(()) => return None,
                Err(Status::ConditionFailed(_)) => continue,
                Err(error) => {
                    log::error!("failed to lazily expire key {key} in DB {db_index}: {error}");
                    return None;
                }
            }
        }
        log::warn!("gave up lazily expiring repeatedly modified key {key} in DB {db_index}");
        None
    }
}
