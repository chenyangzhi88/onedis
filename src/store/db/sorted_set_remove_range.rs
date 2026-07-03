use super::*;

impl Db {
    pub fn zset_remove_range_by_rank(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range(key, start, stop, false)?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove(key, &members)
    }

    pub async fn zset_remove_range_by_rank_async(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_async(key, start, stop, false)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove_async(key, &members).await
    }

    pub fn zset_remove_range_by_score(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_by_score(key, min, max)?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove(key, &members)
    }

    pub async fn zset_remove_range_by_score_async(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, Error> {
        let members: Vec<String> = self
            .zset_range_by_score_async(key, min, max)
            .await?
            .into_iter()
            .map(|(member, _)| member)
            .collect();
        self.zset_remove_async(key, &members).await
    }
}
