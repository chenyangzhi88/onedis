use super::*;

impl Db {
    pub(in crate::store::db) fn set_slot_index_is_present(
        &self,
        key: &str,
        version: u64,
        len: usize,
    ) -> bool {
        if len == 0 {
            return true;
        }
        self.store
            .contains_key(&set_slot_key(self.db_index, key, version, 0))
            && self
                .store
                .contains_key(&set_slot_key(self.db_index, key, version, (len - 1) as u64))
    }

    pub(in crate::store::db) async fn set_slot_index_is_present_async(
        &self,
        key: &str,
        version: u64,
        len: usize,
    ) -> bool {
        if len == 0 {
            return true;
        }
        self.store
            .contains_key_async(&set_slot_key(self.db_index, key, version, 0))
            .await
            && self
                .store
                .contains_key_async(&set_slot_key(self.db_index, key, version, (len - 1) as u64))
                .await
    }

    pub(in crate::store::db) fn rebuild_set_slot_index_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        version: u64,
    ) -> usize {
        let members = self.set_members_raw(key, version);
        batch.delete_range(
            &set_slot_prefix(self.db_index, key, version),
            &sub_key_range_end_bytes(self.db_index, &SET_SLOT_NAMESPACE, key.as_bytes(), version),
        );
        batch.delete_range(
            &set_member_slot_prefix(self.db_index, key, version),
            &sub_key_range_end_bytes(
                self.db_index,
                &SET_MEMBER_SLOT_NAMESPACE,
                key.as_bytes(),
                version,
            ),
        );
        for (slot, member) in members.iter().enumerate() {
            let slot = slot as u64;
            batch.put(&set_slot_key(self.db_index, key, version, slot), member);
            batch.put(
                &set_member_slot_key(self.db_index, key, version, member),
                &slot.to_be_bytes(),
            );
        }
        members.len()
    }

    pub(in crate::store::db) async fn rebuild_set_slot_index_to_batch_async(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        version: u64,
    ) -> usize {
        let members = self.set_members_raw_async(key, version).await;
        batch.delete_range(
            &set_slot_prefix(self.db_index, key, version),
            &sub_key_range_end_bytes(self.db_index, &SET_SLOT_NAMESPACE, key.as_bytes(), version),
        );
        batch.delete_range(
            &set_member_slot_prefix(self.db_index, key, version),
            &sub_key_range_end_bytes(
                self.db_index,
                &SET_MEMBER_SLOT_NAMESPACE,
                key.as_bytes(),
                version,
            ),
        );
        for (slot, member) in members.iter().enumerate() {
            let slot = slot as u64;
            batch.put(&set_slot_key(self.db_index, key, version, slot), member);
            batch.put(
                &set_member_slot_key(self.db_index, key, version, member),
                &slot.to_be_bytes(),
            );
        }
        members.len()
    }

    pub(in crate::store::db) fn ensure_set_slot_index(&self, key: &str, meta: SetMeta) -> SetMeta {
        if self.set_slot_index_is_present(key, meta.version, meta.len) {
            return meta;
        }
        let mut batch = WriteBatch::new();
        let rebuilt_len = self.rebuild_set_slot_index_to_batch(&mut batch, key, meta.version);
        if rebuilt_len != meta.len {
            batch.put(
                &self.mk(key),
                &encode_set_meta(meta.expire_ms, meta.version, rebuilt_len),
            );
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        SetMeta {
            len: rebuilt_len,
            ..meta
        }
    }

    pub(in crate::store::db) fn rebuild_set_slot_index(&self, key: &str, meta: SetMeta) -> SetMeta {
        let mut batch = WriteBatch::new();
        let rebuilt_len = self.rebuild_set_slot_index_to_batch(&mut batch, key, meta.version);
        batch.put(
            &self.mk(key),
            &encode_set_meta(meta.expire_ms, meta.version, rebuilt_len),
        );
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        SetMeta {
            len: rebuilt_len,
            ..meta
        }
    }

    pub(in crate::store::db) async fn ensure_set_slot_index_async(
        &self,
        key: &str,
        meta: SetMeta,
    ) -> SetMeta {
        if self
            .set_slot_index_is_present_async(key, meta.version, meta.len)
            .await
        {
            return meta;
        }
        let mut batch = WriteBatch::new();
        let rebuilt_len = self
            .rebuild_set_slot_index_to_batch_async(&mut batch, key, meta.version)
            .await;
        if rebuilt_len != meta.len {
            batch.put(
                &self.mk(key),
                &encode_set_meta(meta.expire_ms, meta.version, rebuilt_len),
            );
        }
        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
        }
        SetMeta {
            len: rebuilt_len,
            ..meta
        }
    }

    pub(in crate::store::db) async fn rebuild_set_slot_index_async(
        &self,
        key: &str,
        meta: SetMeta,
    ) -> SetMeta {
        let mut batch = WriteBatch::new();
        let rebuilt_len = self
            .rebuild_set_slot_index_to_batch_async(&mut batch, key, meta.version)
            .await;
        batch.put(
            &self.mk(key),
            &encode_set_meta(meta.expire_ms, meta.version, rebuilt_len),
        );
        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
        }
        SetMeta {
            len: rebuilt_len,
            ..meta
        }
    }
}
