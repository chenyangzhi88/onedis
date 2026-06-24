impl Db {
    /// 分页扫描 zset members，返回下一个游标和成员/分数。
    pub fn zset_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok((0, Vec::new()));
        };

        let mut entries = self.zset_ranked_members(key, version);
        if pattern_str != "*" {
            entries.retain(|(member, _)| pattern::is_match(member, pattern_str));
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

    pub async fn zset_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok((0, Vec::new()));
        };

        let mut entries = self.zset_ranked_members_async(key, version).await;
        if pattern_str != "*" {
            entries.retain(|(member, _)| pattern::is_match(member, pattern_str));
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
