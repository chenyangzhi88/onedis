use super::*;

impl Db {
    pub(in crate::store::db) fn set_slot_add_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        version: u64,
        slot: u64,
        member: &[u8],
    ) {
        batch.put(&set_slot_key(self.db_index, key, version, slot), member);
        batch.put(
            &set_member_slot_key(self.db_index, key, version, member),
            &slot.to_be_bytes(),
        );
    }

    pub(in crate::store::db) fn set_slot_remove_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        version: u64,
        current_len: usize,
        member: &[u8],
    ) -> bool {
        if current_len == 0 {
            return false;
        }
        let Some(slot_raw) =
            self.store
                .get_raw(&set_member_slot_key(self.db_index, key, version, member))
        else {
            return false;
        };
        let Some(slot) = decode_u64_be(&slot_raw) else {
            return false;
        };
        let last_slot = (current_len - 1) as u64;
        if slot > last_slot {
            return false;
        }

        if slot != last_slot {
            let Some(last_member) =
                self.store
                    .get_raw(&set_slot_key(self.db_index, key, version, last_slot))
            else {
                return false;
            };
            batch.put(
                &set_slot_key(self.db_index, key, version, slot),
                &last_member,
            );
            batch.put(
                &set_member_slot_key(self.db_index, key, version, &last_member),
                &slot.to_be_bytes(),
            );
        }
        batch.delete(&set_slot_key(self.db_index, key, version, last_slot));
        batch.delete(&set_member_slot_key(self.db_index, key, version, member));
        batch.delete(&set_member_key_bytes(self.db_index, key, version, member));
        true
    }

    pub(in crate::store::db) async fn set_slot_remove_to_batch_async(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        version: u64,
        current_len: usize,
        member: &[u8],
    ) -> bool {
        if current_len == 0 {
            return false;
        }
        let Some(slot_raw) = self
            .store
            .get_raw_async(&set_member_slot_key(self.db_index, key, version, member))
            .await
        else {
            return false;
        };
        let Some(slot) = decode_u64_be(&slot_raw) else {
            return false;
        };
        let last_slot = (current_len - 1) as u64;
        if slot > last_slot {
            return false;
        }

        if slot != last_slot {
            let Some(last_member) = self
                .store
                .get_raw_async(&set_slot_key(self.db_index, key, version, last_slot))
                .await
            else {
                return false;
            };
            batch.put(
                &set_slot_key(self.db_index, key, version, slot),
                &last_member,
            );
            batch.put(
                &set_member_slot_key(self.db_index, key, version, &last_member),
                &slot.to_be_bytes(),
            );
        }
        batch.delete(&set_slot_key(self.db_index, key, version, last_slot));
        batch.delete(&set_member_slot_key(self.db_index, key, version, member));
        batch.delete(&set_member_key_bytes(self.db_index, key, version, member));
        true
    }
}
