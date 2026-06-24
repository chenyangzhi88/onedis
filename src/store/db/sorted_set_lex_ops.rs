impl Db {
    pub(crate) fn zset_range_by_lex(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .read_zset_members(key, version)
            .into_iter()
            .filter(|(member, _)| {
                crate::cmds::sorted_set::zrange::lex_member_in_range(member, min, max)
            })
            .collect())
    }

    pub(crate) async fn zset_range_by_lex_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<Vec<(String, f64)>, Error> {
        let meta = self.zset_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .zset_members_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(member, value)| {
                match (String::from_utf8(member), decode_zset_score(&value)) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .filter(|(member, _)| {
                crate::cmds::sorted_set::zrange::lex_member_in_range(member, min, max)
            })
            .collect())
    }

    pub(crate) fn zset_lex_count(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        Ok(self.zset_range_by_lex(key, min, max)?.len())
    }

    pub(crate) async fn zset_lex_count_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        Ok(self.zset_range_by_lex_async(key, min, max).await?.len())
    }

    pub(crate) fn zset_remove_range_by_lex(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let members = self
            .zset_range_by_lex(key, min, max)?
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>();
        self.zset_remove(key, &members)
    }

    pub(crate) async fn zset_remove_range_by_lex_async(
        &self,
        key: &str,
        min: &crate::cmds::sorted_set::zrange::LexBound,
        max: &crate::cmds::sorted_set::zrange::LexBound,
    ) -> Result<usize, Error> {
        let members = self
            .zset_range_by_lex_async(key, min, max)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect::<Vec<_>>();
        self.zset_remove_async(key, &members).await
    }
}
