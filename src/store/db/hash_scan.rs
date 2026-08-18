use super::*;

pub type HashScanEntries = Vec<(String, String)>;
pub type HashScanEntriesBytes = Vec<(String, Vec<u8>)>;

impl Db {
    pub fn hash_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, HashScanEntries), Error> {
        let (cursor, entries) = self.hash_scan_bytes(key, cursor, pattern_str, count)?;
        Ok((
            cursor,
            entries
                .into_iter()
                .filter_map(|(field, value)| {
                    String::from_utf8(value).ok().map(|value| (field, value))
                })
                .collect(),
        ))
    }

    pub fn hash_scan_bytes(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, HashScanEntriesBytes), Error> {
        let mut entries = self.hash_get_all_bytes(key)?;
        if pattern_str != "*" {
            let matcher = pattern::Matcher::new(pattern_str);
            entries.retain(|(field, _)| matcher.is_match(field));
        }

        let start_index = usize::try_from(cursor).map_err(|_| Error::msg("ERR invalid cursor"))?;
        let end_index = start_index.saturating_add(count).min(entries.len());
        let items = if start_index < entries.len() {
            entries[start_index..end_index].to_vec()
        } else {
            Vec::new()
        };
        let next_cursor = if end_index >= entries.len() {
            0
        } else {
            end_index as u64
        };

        Ok((next_cursor, items))
    }

    pub async fn hash_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, HashScanEntries), Error> {
        let (cursor, entries) = self
            .hash_scan_bytes_async(key, cursor, pattern_str, count)
            .await?;
        Ok((
            cursor,
            entries
                .into_iter()
                .filter_map(|(field, value)| {
                    String::from_utf8(value).ok().map(|value| (field, value))
                })
                .collect(),
        ))
    }

    pub async fn hash_scan_bytes_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, HashScanEntriesBytes), Error> {
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok((0, Vec::new()));
        };
        if pattern_str != "*" || meta.may_have_field_ttl || meta.packed {
            return self
                .hash_scan_bytes_async_filtered(key, meta, cursor, pattern_str, count)
                .await;
        }

        let start_index = usize::try_from(cursor).map_err(|_| Error::msg("ERR invalid cursor"))?;
        let prefix = hash_field_prefix(self.db_index, key, meta.version);
        let upper = prefix_exclusive_upper_bound(&prefix);
        let Some(lower) = self
            .store
            .scan_range_raw_start_at_offset_async(&prefix, upper.clone(), start_index)
            .await
        else {
            return Ok((0, Vec::new()));
        };
        let raw_entries = self
            .store
            .scan_range_raw_limited_async(&lower, upper, count.saturating_add(1))
            .await;
        let has_more = raw_entries.len() > count;
        let items = raw_entries
            .into_iter()
            .take(count)
            .filter_map(|(raw_key, value)| {
                let field = raw_key.strip_prefix(prefix.as_slice())?;
                String::from_utf8(field.to_vec())
                    .ok()
                    .map(|field| (field, value))
            })
            .collect::<Vec<_>>();
        let next_cursor = if has_more {
            cursor.saturating_add(items.len() as u64)
        } else {
            0
        };
        Ok((next_cursor, items))
    }

    async fn hash_scan_bytes_async_filtered(
        &self,
        key: &str,
        meta: HashMeta,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, HashScanEntriesBytes), Error> {
        let mut entries = self.hash_live_entries_for_meta_async(key, meta).await;
        if pattern_str != "*" {
            let matcher = pattern::Matcher::new(pattern_str);
            entries.retain(|(field, _)| {
                std::str::from_utf8(field).is_ok_and(|field| matcher.is_match(field))
            });
        }
        let start_index = usize::try_from(cursor).map_err(|_| Error::msg("ERR invalid cursor"))?;
        let end_index = start_index.saturating_add(count).min(entries.len());
        let items = entries
            .get(start_index..end_index)
            .unwrap_or_default()
            .iter()
            .filter_map(|(field, value)| {
                String::from_utf8(field.clone())
                    .ok()
                    .map(|field| (field, value.clone()))
            })
            .collect();
        Ok((
            if end_index >= entries.len() {
                0
            } else {
                end_index as u64
            },
            items,
        ))
    }
}
