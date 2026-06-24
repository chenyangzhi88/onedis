impl Db {
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
}
