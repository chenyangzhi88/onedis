use super::*;

impl Db {
    pub fn zset_pop(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_all_entries(key)?;
        if !min {
            entries.reverse();
        }
        entries.truncate(count);
        let members = entries
            .iter()
            .map(|(member, _)| member.clone())
            .collect::<Vec<_>>();
        self.zset_remove(key, &members)?;
        Ok(entries)
    }

    pub async fn zset_pop_async(
        &self,
        key: &str,
        min: bool,
        count: usize,
    ) -> Result<Vec<(String, f64)>, Error> {
        let mut entries = self.zset_all_entries_async(key).await?;
        if !min {
            entries.reverse();
        }
        entries.truncate(count);
        let members = entries
            .iter()
            .map(|(member, _)| member.clone())
            .collect::<Vec<_>>();
        self.zset_remove_async(key, &members).await?;
        Ok(entries)
    }

    pub fn zset_multi_pop(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(String, f64)>)>, Error> {
        for key in keys {
            if self.zset_card(key)? == 0 {
                continue;
            }
            let entries = self.zset_pop(key, min, count)?;
            if !entries.is_empty() {
                return Ok(Some((key.clone(), entries)));
            }
        }
        Ok(None)
    }

    pub async fn zset_multi_pop_async(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(String, f64)>)>, Error> {
        for key in keys {
            if self.zset_card_async(key).await? == 0 {
                continue;
            }
            let entries = self.zset_pop_async(key, min, count).await?;
            if !entries.is_empty() {
                return Ok(Some((key.clone(), entries)));
            }
        }
        Ok(None)
    }
}
