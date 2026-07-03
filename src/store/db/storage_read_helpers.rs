use super::*;

impl Db {
    pub(in crate::store::db) fn logical_keys(&self) -> Vec<String> {
        self.store
            .scan_range_raw_limited(&[], None, usize::MAX)
            .into_iter()
            .filter_map(|(k, _)| {
                let key = logical_main_key_from_raw_key(self.key_layout, self.db_index, &k)?;
                String::from_utf8(key).ok()
            })
            .collect()
    }

    pub(in crate::store::db) async fn logical_keys_async(&self) -> Vec<String> {
        self.store
            .scan_range_raw_limited_async(&[], None, usize::MAX)
            .await
            .into_iter()
            .filter_map(|(k, _)| {
                let key = logical_main_key_from_raw_key(self.key_layout, self.db_index, &k)?;
                String::from_utf8(key).ok()
            })
            .collect()
    }

    pub async fn scan_string_prefix_async(
        &self,
        key_prefix: &str,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let prefix = main_key(self.db_index, key_prefix);
        let mut rows = Vec::new();
        for (raw_key, _) in self.store.scan_prefix_raw_async(&prefix).await {
            if rows.len() >= limit {
                break;
            }
            let Some(key_bytes) =
                logical_main_key_from_raw_key(self.key_layout, self.db_index, &raw_key)
            else {
                continue;
            };
            let Ok(key) = String::from_utf8(key_bytes) else {
                continue;
            };
            if let Ok(Some(value)) = self.get_string_bytes_async(&key).await {
                rows.push((key, value));
            }
        }
        rows
    }

    pub(in crate::store::db) fn read_hash_fields(
        &self,
        key: &str,
        version: u64,
    ) -> HashMap<String, String> {
        let mut hash = HashMap::new();

        for (field, value) in self.hash_entries_raw(key, version) {
            if let (Ok(field), Ok(value)) = (String::from_utf8(field), String::from_utf8(value)) {
                hash.insert(field, value);
            }
        }

        hash
    }

    pub(in crate::store::db) fn read_set_members(
        &self,
        key: &str,
        version: u64,
    ) -> HashSet<String> {
        self.set_members_raw(key, version)
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect()
    }

    pub(in crate::store::db) fn read_zset_members(
        &self,
        key: &str,
        version: u64,
    ) -> BTreeMap<String, f64> {
        self.zset_members_raw(key, version)
            .into_iter()
            .filter_map(|(member, value)| {
                match (String::from_utf8(member), decode_zset_score(&value)) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .collect()
    }

    pub(in crate::store::db) fn decode_rank_score(
        &self,
        key: &str,
        version: u64,
        rank_key: &[u8],
    ) -> Option<f64> {
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let suffix = rank_key.strip_prefix(prefix.as_slice())?;
        if suffix.len() < 9 {
            return None;
        }
        let score_bytes: [u8; 8] = suffix[0..8].try_into().ok()?;
        Some(decode_sorted_f64(score_bytes))
    }

    pub(in crate::store::db) fn decode_rank_member(
        &self,
        key: &str,
        version: u64,
        rank_key: &[u8],
    ) -> Option<String> {
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let suffix = rank_key.strip_prefix(prefix.as_slice())?;
        if suffix.len() < 9 || suffix[8] != 0x00 {
            return None;
        }
        String::from_utf8(suffix[9..].to_vec()).ok()
    }

    pub(in crate::store::db) fn read_list_items(&self, key: &str, version: u64) -> Vec<String> {
        let prefix = list_item_prefix(self.db_index, key, version);
        let mut items: Vec<(i64, String)> = Vec::new();

        for (key_bytes, value_bytes) in self.store.scan_prefix_raw(&prefix) {
            let index_bytes = &key_bytes[prefix.len()..];
            if index_bytes.len() != 8 {
                continue;
            }

            let index = match <[u8; 8]>::try_from(index_bytes) {
                Ok(bytes) => i64::from_be_bytes(bytes),
                Err(_) => continue,
            };

            if let Ok(value) = String::from_utf8(value_bytes) {
                items.push((index, value));
            }
        }

        items.sort_by_key(|(index, _)| *index);
        items.into_iter().map(|(_, value)| value).collect()
    }

    pub(in crate::store::db) fn read_stream_entries(
        &self,
        key: &str,
        version: u64,
    ) -> Vec<StreamEntry> {
        self.stream_entries_between(
            key,
            version,
            StreamId { ms: 0, seq: 0 },
            StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            },
        )
    }
}
