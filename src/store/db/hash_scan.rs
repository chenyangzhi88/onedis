use super::*;

impl Db {
    pub fn hash_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, String)>), Error> {
        let mut entries = self.hash_get_all(key)?;
        if pattern_str != "*" {
            entries.retain(|(field, _)| pattern::is_match(field, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
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
    ) -> Result<(u64, Vec<(String, String)>), Error> {
        let mut entries = self.hash_get_all_async(key).await?;
        if pattern_str != "*" {
            entries.retain(|(field, _)| pattern::is_match(field, pattern_str));
        }

        let start_index = cursor as usize;
        let end_index = std::cmp::min(start_index + count, entries.len());
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
