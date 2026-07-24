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
        let mut entries = self.hash_get_all_bytes_async(key).await?;
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
}
