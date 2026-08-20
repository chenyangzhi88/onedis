use super::*;

impl Db {
    pub(in crate::store::db) fn load_live_raw_for_db_with_backend(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key_bytes = main_key(db_index, key);
        for _ in 0..64 {
            let observed = store.get_raw_observed(&key_bytes)?;
            let Some(raw) = observed.value() else {
                return Ok(None);
            };
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 || now_ms() < expire_ms {
                return Ok(Some(raw.to_vec()));
            }

            let mut batch = WriteBatch::new();
            Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, raw);
            match store.compare_and_write_batch(&[observed.condition()], &batch) {
                Ok(()) => return Ok(None),
                Err(Status::ConditionFailed(_)) => continue,
                Err(error) => return Err(Error::msg(error.to_string())),
            }
        }
        Err(Error::msg(format!(
            "ERR key {key} was modified too often while expiring it"
        )))
    }

    pub(in crate::store::db) async fn load_live_raw_for_db_with_backend_async(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key_bytes = main_key(db_index, key);
        for _ in 0..64 {
            let observed = store.get_raw_observed_async(&key_bytes).await?;
            let Some(raw) = observed.value() else {
                return Ok(None);
            };
            let expire_ms = decode_expire_ms(raw);
            if expire_ms == 0 || now_ms() < expire_ms {
                return Ok(Some(raw.to_vec()));
            }

            let mut batch = WriteBatch::new();
            Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, raw);
            match store
                .compare_and_write_batch_async(&[observed.condition()], &batch)
                .await
            {
                Ok(()) => return Ok(None),
                Err(Status::ConditionFailed(_)) => continue,
                Err(error) => return Err(Error::msg(error.to_string())),
            }
        }
        Err(Error::msg(format!(
            "ERR key {key} was modified too often while expiring it"
        )))
    }
}
