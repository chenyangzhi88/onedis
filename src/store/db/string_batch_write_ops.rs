use super::*;

impl Db {
    pub async fn insert_string_bytes_refs_async(&self, key_vals: &[(&str, &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_old_raw(&mut batch, key, value, 0, None);
        }
        self.write_plain_string_batch_if_not_empty_without_watch_publish_async(&batch)
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
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_old_raw(&mut batch, key, value, 0, None);
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
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_byte_key_to_batch_with_old_raw(&mut batch, key, value, 0, None);
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
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_byte_key_to_batch_with_old_raw(&mut batch, key, value, 0, None);
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
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch_with_old_raw(&mut batch, &key, &value, 0, None);
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
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        if self
            .store
            .multi_get_raw_async(&keys)
            .await
            .iter()
            .any(Option::is_some)
        {
            return false;
        }

        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch(&mut batch, &key, &value, 0);
        }
        self.write_batch_if_not_empty_async(&batch).await;
        true
    }
}
