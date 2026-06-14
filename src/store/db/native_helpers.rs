impl Db {
    fn get_expire_ms(&self, key: &str) -> u64 {
        if let Some(raw) = self.store.get_raw(&main_key(self.db_index, key)) {
            decode_expire_ms(&raw)
        } else {
            0
        }
    }

    fn get_expire_and_version(&self, key: &str) -> (u64, u64) {
        if let Some(raw) = self.store.get_raw(&main_key(self.db_index, key)) {
            if let Some(header) = decode_meta_header(&raw) {
                return (header.expire_ms, header.version);
            }
        }
        (0, 0)
    }

    fn hash_expire_ms(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return Ok(None);
        };

        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    async fn hash_expire_ms_async(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        let key_bytes = self.mk(key);

        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&key_bytes).await else {
            return Ok(None);
        };
        let header = decode_hash_meta_checked(&raw)?;
        Ok(Some((header.expire_ms, header.version)))
    }

    fn hash_entries_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = hash_field_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect()
    }

    fn hash_live_entries_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.hash_entries_raw(key, version)
            .into_iter()
            .filter_map(|(field, value)| {
                let field_text = String::from_utf8_lossy(&field);
                self.hash_field_is_live(key, version, &field_text)
                    .then_some((field, value))
            })
            .collect()
    }

    async fn hash_live_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        for (field, value) in self.hash_entries_raw_async(key, version).await {
            let field_text = String::from_utf8_lossy(&field);
            if self
                .hash_field_is_live_async(key, version, &field_text)
                .await
            {
                entries.push((field, value));
            }
        }
        entries
    }

    fn hash_field_is_live(&self, key: &str, version: u64, field: &str) -> bool {
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let Some(raw) = self.store.get_raw(&expire_key) else {
            return true;
        };
        let Some(expire_ms) = decode_u64_be(&raw) else {
            return true;
        };
        if expire_ms == 0 || now_ms() < expire_ms {
            return true;
        }

        let mut batch = WriteBatch::new();
        batch.delete(&hash_field_key(self.db_index, key, version, field));
        batch.delete(&expire_key);
        self.write_batch_if_not_empty(&batch);
        false
    }

    async fn hash_field_is_live_async(&self, key: &str, version: u64, field: &str) -> bool {
        let expire_key = hash_field_expire_key(self.db_index, key, version, field);
        let Some(raw) = self.store.get_raw_async(&expire_key).await else {
            return true;
        };
        let Some(expire_ms) = decode_u64_be(&raw) else {
            return true;
        };
        if expire_ms == 0 || now_ms() < expire_ms {
            return true;
        }

        let mut batch = WriteBatch::new();
        batch.delete(&hash_field_key(self.db_index, key, version, field));
        batch.delete(&expire_key);
        self.write_batch_if_not_empty_async(&batch).await;
        false
    }

    fn hash_live_field_value(&self, key: &str, version: u64, field: &str) -> Option<Vec<u8>> {
        if !self.hash_field_is_live(key, version, field) {
            return None;
        }
        self.store
            .get_raw(&hash_field_key(self.db_index, key, version, field))
    }

    async fn hash_live_field_value_async(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> Option<Vec<u8>> {
        if !self.hash_field_is_live_async(key, version, field).await {
            return None;
        }
        self.store
            .get_raw_async(&hash_field_key(self.db_index, key, version, field))
            .await
    }

    async fn hash_live_field_observed_async(
        &self,
        key: &str,
        version: u64,
        field: &str,
    ) -> kv_engine::db::ObservedKvValue {
        let _ = self.hash_field_is_live_async(key, version, field).await;
        self.store
            .get_raw_observed_async(&hash_field_key(self.db_index, key, version, field))
            .await
    }

    async fn hash_entries_raw_async(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = hash_field_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(field_key, value)| {
                field_key
                    .strip_prefix(prefix.as_slice())
                    .map(|field| (field.to_vec(), value))
            })
            .collect()
    }

    fn set_meta(&self, key: &str) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }

    async fn set_meta_async(&self, key: &str) -> Result<Option<SetMeta>, Error> {
        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        if let Some(header) = decode_meta_header(&raw)
            && header.type_tag != TYPE_SET
        {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let Some(meta) = decode_set_meta(&raw) else {
            return Err(Error::msg("Failed to decode set metadata"));
        };

        Ok(Some(meta))
    }

    fn set_slot_index_is_present(&self, key: &str, version: u64, len: usize) -> bool {
        if len == 0 {
            return true;
        }
        self.store
            .contains_key(&set_slot_key(self.db_index, key, version, 0))
            && self
                .store
                .contains_key(&set_slot_key(self.db_index, key, version, (len - 1) as u64))
    }

    async fn set_slot_index_is_present_async(&self, key: &str, version: u64, len: usize) -> bool {
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

    fn rebuild_set_slot_index_to_batch(
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

    async fn rebuild_set_slot_index_to_batch_async(
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

    fn ensure_set_slot_index(&self, key: &str, meta: SetMeta) -> SetMeta {
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

    fn rebuild_set_slot_index(&self, key: &str, meta: SetMeta) -> SetMeta {
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

    async fn ensure_set_slot_index_async(&self, key: &str, meta: SetMeta) -> SetMeta {
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

    async fn rebuild_set_slot_index_async(&self, key: &str, meta: SetMeta) -> SetMeta {
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

    fn set_slot_add_to_batch(
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

    fn set_slot_remove_to_batch(
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

    async fn set_slot_remove_to_batch_async(
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

    fn zset_expire_ms(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        self.expire_if_needed(key);

        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode sorted set metadata"));
        };

        if header.type_tag != TYPE_SORTED_SET {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Ok(Some((header.expire_ms, header.version)))
    }

    async fn zset_expire_ms_async(&self, key: &str) -> Result<Option<(u64, u64)>, Error> {
        self.expire_if_needed_async(key).await;

        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode sorted set metadata"));
        };

        if header.type_tag != TYPE_SORTED_SET {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Ok(Some((header.expire_ms, header.version)))
    }

    fn set_members_raw(&self, key: &str, version: u64) -> Vec<Vec<u8>> {
        let prefix = set_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(member_key, _)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| member.to_vec())
            })
            .collect()
    }

    async fn set_members_raw_async(&self, key: &str, version: u64) -> Vec<Vec<u8>> {
        let prefix = set_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(member_key, _)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| member.to_vec())
            })
            .collect()
    }

    fn set_random_seek_members(&self, key: &str, version: u64, count: usize) -> Vec<Vec<u8>> {
        if count == 0 {
            return Vec::new();
        }

        let prefix = set_member_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut members = Vec::with_capacity(count);
        let mut seen = HashSet::with_capacity(count);
        let attempts = count.saturating_mul(2).max(1);

        for _ in 0..attempts {
            if members.len() >= count {
                break;
            }
            let mut lower = prefix.clone();
            lower.extend_from_slice(&random_u64().to_be_bytes());

            let mut hit = self.store.scan_range_raw_limited(&lower, upper.clone(), 1);
            if hit.is_empty() {
                hit = self.store.scan_range_raw_limited(&prefix, upper.clone(), 1);
            }
            if let Some((member_key, _)) = hit.into_iter().next()
                && let Some(member) = member_key.strip_prefix(prefix.as_slice())
            {
                let member = member.to_vec();
                if seen.insert(member.clone()) {
                    members.push(member);
                }
            }
        }

        if members.len() < count {
            for (member_key, _) in
                self.store
                    .scan_range_raw_limited(&prefix, upper, count.saturating_mul(2))
            {
                if let Some(member) = member_key.strip_prefix(prefix.as_slice()) {
                    let member = member.to_vec();
                    if seen.insert(member.clone()) {
                        members.push(member);
                        if members.len() >= count {
                            break;
                        }
                    }
                }
            }
        }

        members
    }

    async fn set_random_seek_members_async(
        &self,
        key: &str,
        version: u64,
        count: usize,
    ) -> Vec<Vec<u8>> {
        if count == 0 {
            return Vec::new();
        }

        let prefix = set_member_prefix(self.db_index, key, version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let mut members = Vec::with_capacity(count);
        let mut seen = HashSet::with_capacity(count);
        let attempts = count.saturating_mul(2).max(1);

        for _ in 0..attempts {
            if members.len() >= count {
                break;
            }
            let mut lower = prefix.clone();
            lower.extend_from_slice(&random_u64().to_be_bytes());

            let mut hit = self
                .store
                .scan_range_raw_limited_async(&lower, upper.clone(), 1)
                .await;
            if hit.is_empty() {
                hit = self
                    .store
                    .scan_range_raw_limited_async(&prefix, upper.clone(), 1)
                    .await;
            }
            if let Some((member_key, _)) = hit.into_iter().next()
                && let Some(member) = member_key.strip_prefix(prefix.as_slice())
            {
                let member = member.to_vec();
                if seen.insert(member.clone()) {
                    members.push(member);
                }
            }
        }

        if members.len() < count {
            for (member_key, _) in self
                .store
                .scan_range_raw_limited_async(&prefix, upper, count.saturating_mul(2))
                .await
            {
                if let Some(member) = member_key.strip_prefix(prefix.as_slice()) {
                    let member = member.to_vec();
                    if seen.insert(member.clone()) {
                        members.push(member);
                        if members.len() >= count {
                            break;
                        }
                    }
                }
            }
        }

        members
    }

    fn zset_members_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = zset_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(member_key, value)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| (member.to_vec(), value))
            })
            .collect()
    }

    async fn zset_members_raw_async(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = zset_member_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(member_key, value)| {
                member_key
                    .strip_prefix(prefix.as_slice())
                    .map(|member| (member.to_vec(), value))
            })
            .collect()
    }

    fn zset_rank_entries_raw(&self, key: &str, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan_prefix_raw(&zset_rank_prefix(self.db_index, key, version))
    }

    async fn zset_rank_entries_raw_async(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan_prefix_raw_async(&zset_rank_prefix(self.db_index, key, version))
            .await
    }

    fn zset_ranked_members(&self, key: &str, version: u64) -> Vec<(String, f64)> {
        self.zset_rank_entries_raw(key, version)
            .into_iter()
            .filter_map(|(rank_key, _)| {
                let score = self.decode_rank_score(key, version, &rank_key)?;
                let member = self.decode_rank_member(key, version, &rank_key)?;
                Some((member, score))
            })
            .collect()
    }

    async fn zset_ranked_members_async(&self, key: &str, version: u64) -> Vec<(String, f64)> {
        self.zset_rank_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(rank_key, _)| {
                let score = self.decode_rank_score(key, version, &rank_key)?;
                let member = self.decode_rank_member(key, version, &rank_key)?;
                Some((member, score))
            })
            .collect()
    }

    fn stream_meta(&self, key: &str) -> Result<Option<StreamMeta>, Error> {
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };

        if let Some(meta) = decode_stream_meta(&raw) {
            return Ok(Some(meta));
        }

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode stream metadata"));
        };
        if header.type_tag != TYPE_STREAM {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Err(Error::msg("Failed to decode stream metadata"))
    }

    async fn stream_meta_async(&self, key: &str) -> Result<Option<StreamMeta>, Error> {
        self.expire_if_needed_async(key).await;
        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };

        if let Some(meta) = decode_stream_meta(&raw) {
            return Ok(Some(meta));
        }

        let Some(header) = decode_meta_header(&raw) else {
            return Err(Error::msg("Failed to decode stream metadata"));
        };
        if header.type_tag != TYPE_STREAM {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        Err(Error::msg("Failed to decode stream metadata"))
    }

    fn next_stream_id(&self, last_id: StreamId) -> StreamId {
        let now = now_ms();
        if now > last_id.ms {
            StreamId { ms: now, seq: 0 }
        } else {
            StreamId {
                ms: last_id.ms,
                seq: last_id.seq.saturating_add(1),
            }
        }
    }

    fn stream_entries_raw(&self, key: &str, version: u64) -> Vec<(StreamId, Vec<u8>)> {
        let prefix = stream_entry_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(entry_key, value)| {
                decode_stream_entry_id(&prefix, &entry_key).map(|id| (id, value))
            })
            .collect()
    }

    async fn stream_entries_raw_async(&self, key: &str, version: u64) -> Vec<(StreamId, Vec<u8>)> {
        let prefix = stream_entry_prefix(self.db_index, key, version);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(entry_key, value)| {
                decode_stream_entry_id(&prefix, &entry_key).map(|id| (id, value))
            })
            .collect()
    }

    fn stream_entries_between(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
    ) -> Vec<StreamEntry> {
        self.stream_entries_raw(key, version)
            .into_iter()
            .filter(|(id, _)| *id >= start && *id <= end)
            .filter_map(|(id, value)| {
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    async fn stream_entries_between_async(
        &self,
        key: &str,
        version: u64,
        start: StreamId,
        end: StreamId,
    ) -> Vec<StreamEntry> {
        self.stream_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter(|(id, _)| *id >= start && *id <= end)
            .filter_map(|(id, value)| {
                Some(StreamEntry {
                    id: id.to_redis_id(),
                    fields: decode_stream_entry(&value)?,
                })
            })
            .collect()
    }

    fn stream_entry_by_id(&self, key: &str, version: u64, id: StreamId) -> Option<StreamEntry> {
        let raw = self
            .store
            .get_raw(&stream_entry_key(self.db_index, key, version, id))?;
        Some(StreamEntry {
            id: id.to_redis_id(),
            fields: decode_stream_entry(&raw)?,
        })
    }

    async fn stream_entry_by_id_async(
        &self,
        key: &str,
        version: u64,
        id: StreamId,
    ) -> Option<StreamEntry> {
        let raw = self
            .store
            .get_raw_async(&stream_entry_key(self.db_index, key, version, id))
            .await?;
        Some(StreamEntry {
            id: id.to_redis_id(),
            fields: decode_stream_entry(&raw)?,
        })
    }

    fn stream_group_state(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Option<StreamGroupState>, Error> {
        let Some(meta) = self.stream_meta(key)? else {
            return Ok(None);
        };
        Ok(self
            .store
            .get_raw(&stream_group_key(self.db_index, key, meta.version, group))
            .and_then(|raw| decode_stream_group_state(&raw)))
    }

    async fn stream_group_state_async(
        &self,
        key: &str,
        group: &str,
    ) -> Result<Option<StreamGroupState>, Error> {
        let Some(meta) = self.stream_meta_async(key).await? else {
            return Ok(None);
        };
        Ok(self
            .store
            .get_raw_async(&stream_group_key(self.db_index, key, meta.version, group))
            .await
            .and_then(|raw| decode_stream_group_state(&raw)))
    }

    fn stream_pending_raw(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> Vec<(StreamId, StreamPelState)> {
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }

    fn stream_consumers_raw(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> BTreeMap<String, StreamConsumerState> {
        let prefix = stream_consumer_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(consumer_key, raw)| {
                let suffix = consumer_key.strip_prefix(prefix.as_slice())?;
                let name = String::from_utf8(suffix.to_vec()).ok()?;
                let state = decode_stream_consumer_state(&raw)?;
                Some((name, state))
            })
            .collect()
    }

    async fn stream_consumers_raw_async(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> BTreeMap<String, StreamConsumerState> {
        let prefix = stream_consumer_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(consumer_key, raw)| {
                let suffix = consumer_key.strip_prefix(prefix.as_slice())?;
                let name = String::from_utf8(suffix.to_vec()).ok()?;
                let state = decode_stream_consumer_state(&raw)?;
                Some((name, state))
            })
            .collect()
    }

    async fn stream_pending_raw_async(
        &self,
        key: &str,
        version: u64,
        group: &str,
    ) -> Vec<(StreamId, StreamPelState)> {
        let prefix = stream_pel_group_prefix(self.db_index, key, version, group);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(pel_key, raw)| {
                Some((
                    decode_stream_pel_id(&prefix, &pel_key)?,
                    decode_stream_pel_state(&raw)?,
                ))
            })
            .collect()
    }

    fn list_meta(&self, key: &str) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional() {
            if let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry) {
                if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                    return Ok(Some(meta));
                }
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    meta.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty(&batch);
                self.list_meta_cache.remove(&key_bytes);
                return Ok(None);
            }
        }
        let Some(raw) = self.store.get_raw(&key_bytes) else {
            return Ok(None);
        };
        if let Some(header) = decode_meta_header(&raw) {
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                    header.type_tag,
                );
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty(&batch);
                return Ok(None);
            }
        }

        if let Some(meta) = decode_list_meta(&raw) {
            self.cache_list_meta_if_non_transactional(key, meta);
            return Ok(Some(meta));
        }

        let Some((_, version, structure)) = decode_entry(&raw) else {
            return Err(Error::msg("Failed to decode list metadata"));
        };
        match structure {
            Structure::List(list) => {
                let meta = ListMeta {
                    expire_ms: decode_expire_ms(&raw),
                    version,
                    head: 0,
                    tail: list.len() as i64,
                };
                self.cache_list_meta_if_non_transactional(key, meta);
                Ok(Some(meta))
            }
            _ => Err(Error::msg(WRONG_TYPE_ERROR)),
        }
    }

    async fn list_meta_async(&self, key: &str) -> Result<Option<ListMeta>, Error> {
        let key_bytes = self.mk(key);
        if !self.store.is_transactional() {
            if let Some(meta) = self.list_meta_cache.get(&key_bytes).map(|entry| *entry) {
                if meta.expire_ms == 0 || now_ms() < meta.expire_ms {
                    return Ok(Some(meta));
                }
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    meta.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty_async(&batch).await;
                self.list_meta_cache.remove(&key_bytes);
                return Ok(None);
            }
        }
        let Some(raw) = self.store.get_raw_async(&key_bytes).await else {
            return Ok(None);
        };
        if let Some(header) = decode_meta_header(&raw) {
            if header.expire_ms > 0 && now_ms() >= header.expire_ms {
                let mut batch = WriteBatch::new();
                batch.delete(&key_bytes);
                delete_sub_keys_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                    header.type_tag,
                );
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    header.expire_ms,
                    self.db_index,
                    key,
                );
                self.write_batch_if_not_empty_async(&batch).await;
                return Ok(None);
            }
        }

        if let Some(meta) = decode_list_meta(&raw) {
            self.cache_list_meta_if_non_transactional(key, meta);
            return Ok(Some(meta));
        }

        let Some((_, version, structure)) = decode_entry(&raw) else {
            return Err(Error::msg("Failed to decode list metadata"));
        };
        match structure {
            Structure::List(list) => {
                let meta = ListMeta {
                    expire_ms: decode_expire_ms(&raw),
                    version,
                    head: 0,
                    tail: list.len() as i64,
                };
                self.cache_list_meta_if_non_transactional(key, meta);
                Ok(Some(meta))
            }
            _ => Err(Error::msg(WRONG_TYPE_ERROR)),
        }
    }

    fn resolve_list_index(&self, meta: ListMeta, index: i64) -> Option<i64> {
        let len = meta.tail - meta.head;
        if len <= 0 {
            return None;
        }

        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            return None;
        }

        Some(meta.head + normalized)
    }

    fn resolve_list_range(&self, meta: ListMeta, start: i64, stop: i64) -> Option<(i64, i64)> {
        let len = meta.tail - meta.head;
        if len <= 0 {
            return None;
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };

        normalized_start = normalized_start.max(0);
        normalized_stop = normalized_stop.min(len - 1);

        if normalized_start > normalized_stop || normalized_start >= len || normalized_stop < 0 {
            return None;
        }

        Some((meta.head + normalized_start, meta.head + normalized_stop))
    }

    fn list_range_raw_values(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
    ) -> Vec<Vec<u8>> {
        let len = (storage_end - storage_start + 1) as usize;
        let mut values = Vec::with_capacity(len);
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            self.append_list_range_raw_values(
                key,
                version,
                storage_start,
                negative_end,
                len.saturating_sub(values.len()),
                &mut values,
            );
        }
        if storage_end >= 0 {
            let positive_start = storage_start.max(0);
            self.append_list_range_raw_values(
                key,
                version,
                positive_start,
                storage_end,
                len.saturating_sub(values.len()),
                &mut values,
            );
        }
        values
    }

    async fn list_range_raw_values_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
    ) -> Vec<Vec<u8>> {
        let len = (storage_end - storage_start + 1) as usize;
        let mut values = Vec::with_capacity(len);
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            self.append_list_range_raw_values_async(
                key,
                version,
                storage_start,
                negative_end,
                len.saturating_sub(values.len()),
                &mut values,
            )
            .await;
        }
        if storage_end >= 0 {
            let positive_start = storage_start.max(0);
            self.append_list_range_raw_values_async(
                key,
                version,
                positive_start,
                storage_end,
                len.saturating_sub(values.len()),
                &mut values,
            )
            .await;
        }
        values
    }

    async fn list_range_raw_values_visit_async<F>(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8]) -> bool + Send,
    {
        let len = (storage_end - storage_start + 1) as usize;
        let mut seen = 0usize;
        let mut visitor = visitor;
        if storage_start < 0 {
            let negative_end = storage_end.min(-1);
            seen += self
                .append_list_range_raw_values_visit_async(
                    key,
                    version,
                    storage_start,
                    negative_end,
                    len.saturating_sub(seen),
                    &mut visitor,
                )
                .await;
        }
        if storage_end >= 0 && seen < len {
            let positive_start = storage_start.max(0);
            seen += self
                .append_list_range_raw_values_visit_async(
                    key,
                    version,
                    positive_start,
                    storage_end,
                    len.saturating_sub(seen),
                    &mut visitor,
                )
                .await;
        }
        seen
    }

    fn append_list_range_raw_values(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        values: &mut Vec<Vec<u8>>,
    ) {
        if storage_start > storage_end || limit == 0 {
            return;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        values.extend(
            self.store
                .scan_range_raw_limited(&lower_bound, upper_bound, limit)
                .into_iter()
                .map(|(_, value)| value),
        );
    }

    async fn append_list_range_raw_values_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        values: &mut Vec<Vec<u8>>,
    ) {
        if storage_start > storage_end || limit == 0 {
            return;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        values.extend(
            self.store
                .scan_range_raw_limited_async(&lower_bound, upper_bound, limit)
                .await
                .into_iter()
                .map(|(_, value)| value),
        );
    }

    async fn append_list_range_raw_values_visit_async(
        &self,
        key: &str,
        version: u64,
        storage_start: i64,
        storage_end: i64,
        limit: usize,
        visitor: &mut (dyn FnMut(&[u8]) -> bool + Send),
    ) -> usize {
        if storage_start > storage_end || limit == 0 {
            return 0;
        }

        let lower_bound = list_item_key(self.db_index, key, version, storage_start);
        let upper_bound = if storage_end < -1 {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        } else if storage_end < 0 {
            prefix_exclusive_upper_bound(&list_item_prefix(self.db_index, key, version))
        } else if storage_end == i64::MAX {
            return 0;
        } else {
            Some(list_item_key(self.db_index, key, version, storage_end + 1))
        };

        self.store
            .scan_range_raw_visit_async(&lower_bound, upper_bound, limit, |_, value| visitor(value))
            .await
    }


}
