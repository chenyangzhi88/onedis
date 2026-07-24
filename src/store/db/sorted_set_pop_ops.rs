use super::*;

pub type ZsetEntry = (String, f64);
pub type ZsetMultiPopResult = Option<(String, Vec<ZsetEntry>)>;

impl Db {
    pub fn zset_pop(&self, key: &str, min: bool, count: usize) -> Result<Vec<ZsetEntry>, Error> {
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
    ) -> Result<Vec<ZsetEntry>, Error> {
        let _write_guard = self.set_write_lock(key).lock().await;
        let mut entries = self.zset_all_entries_async(key).await?;
        if !min {
            entries.reverse();
        }
        entries.truncate(count);
        let members = entries
            .iter()
            .map(|(member, _)| member.clone())
            .collect::<Vec<_>>();
        self.zset_remove_async_unlocked(key, &members).await?;
        Ok(entries)
    }

    pub fn zset_multi_pop(
        &self,
        keys: &[String],
        min: bool,
        count: usize,
    ) -> Result<ZsetMultiPopResult, Error> {
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
    ) -> Result<ZsetMultiPopResult, Error> {
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
