use super::*;

impl Db {
    pub async fn insert_string_bytes_refs_async(&self, key_vals: &[(&str, &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.cleanup_old_complex_subkeys_for_string_overwrite_range_to_batch(
                &mut batch,
                key,
                old_raw.as_deref(),
            );
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub async fn insert_string_bytes_refs_without_watch_publish_async(
        &self,
        key_vals: &[(&str, &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.cleanup_old_complex_subkeys_for_string_overwrite_range_to_batch(
                &mut batch,
                key,
                old_raw.as_deref(),
            );
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_without_watch_publish_async(&batch)
            .await;
    }

    pub async fn insert_string_byte_keys_async(&self, key_vals: &[(&[u8], &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| main_key_bytes(self.db_index, key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.cleanup_old_complex_subkeys_for_string_byte_key_overwrite(
                &mut batch,
                key,
                old_raw.as_deref(),
            );
            self.write_string_byte_key_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_owned_if_not_empty_async(batch)
            .await;
    }

    pub async fn insert_string_byte_keys_without_watch_publish_async(
        &self,
        key_vals: &[(&[u8], &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| main_key_bytes(self.db_index, key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.iter().zip(old_values) {
            self.cleanup_old_complex_subkeys_for_string_byte_key_overwrite(
                &mut batch,
                key,
                old_raw.as_deref(),
            );
            self.write_string_byte_key_to_batch_with_deferred_old_raw(
                &mut batch,
                key,
                value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub fn insert_strings(&self, key_vals: Vec<(String, String)>) {
        self.insert_string_bytes_many(
            key_vals
                .into_iter()
                .map(|(key, value)| (key, value.into_bytes()))
                .collect(),
        );
    }

    pub fn insert_string_bytes_many(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_old_raw(&mut batch, &key, &value, 0, None);
        }
        self.write_plain_string_batch_if_not_empty(&batch);
    }

    pub async fn insert_string_bytes_many_async(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&keys).await;
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.into_iter().zip(old_values) {
            self.cleanup_old_complex_subkeys_for_string_overwrite_range_to_batch(
                &mut batch,
                &key,
                old_raw.as_deref(),
            );
            self.write_string_to_batch_with_deferred_old_raw(
                &mut batch,
                &key,
                &value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_plain_string_batch_if_not_empty_async(&batch)
            .await;
    }

    pub fn insert_string_bytes_many_nx(&self, key_vals: Vec<(String, Vec<u8>)>) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        for (key, _) in &key_vals {
            self.expire_if_needed(key);
            if self.store.contains_key(&self.mk(key)) {
                return false;
            }
        }

        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch(&mut batch, &key, &value, 0);
        }
        self.write_batch_if_not_empty(&batch);
        true
    }

    pub async fn insert_string_bytes_many_nx_async(
        &self,
        key_vals: Vec<(String, Vec<u8>)>,
    ) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        for (key, _) in &key_vals {
            self.expire_if_needed_async(key).await;
        }
        let mut observations = Vec::with_capacity(key_vals.len());
        for (key, _) in &key_vals {
            let observed = self.store.get_raw_observed_async(&self.mk(key)).await;
            if observed.exists() {
                return false;
            }
            observations.push(observed);
        }

        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_deferred_old_raw(&mut batch, &key, &value, 0, None);
        }
        let conditions = observations
            .iter()
            .map(CompareCondition::from_observed)
            .collect::<Vec<_>>();
        match self
            .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
            .await
        {
            Ok(true) => {
                self.changes
                    .fetch_add(observations.len() as u64, Ordering::Relaxed);
                true
            }
            Ok(false) => false,
            Err(error) => {
                log::error!("failed to apply MSETNX batch: {error}");
                false
            }
        }
    }
}
