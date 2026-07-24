use super::*;

pub type HashRandomField = (String, Option<String>);
pub type HashRandomFields = Vec<HashRandomField>;

impl Db {
    pub fn hash_exists(&self, key: &str, field: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(false);
        };

        Ok(self.hash_live_field_value(key, version, field).is_some())
    }

    pub async fn hash_exists_async(&self, key: &str, field: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(false);
        };

        Ok(self
            .hash_live_field_value_async(key, version, field)
            .await
            .is_some())
    }

    /// 返回 hash field 数量。
    pub fn hash_len(&self, key: &str) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.hash_live_entries_raw(key, version).len())
    }

    pub async fn hash_len_async(&self, key: &str) -> Result<usize, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.hash_live_entries_raw_async(key, version).await.len())
    }

    /// 批量读取 hash fields。
    pub fn hash_multi_get(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        Ok(fields
            .iter()
            .map(|field| {
                self.hash_live_field_value(key, version, field)
                    .and_then(|value| String::from_utf8(value).ok())
            })
            .collect())
    }

    pub async fn hash_multi_get_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        let mut values = Vec::with_capacity(fields.len());
        for field in fields {
            values.push(
                self.hash_live_field_value_async(key, version, field)
                    .await
                    .and_then(|value| String::from_utf8(value).ok()),
            );
        }
        Ok(values)
    }

    /// 返回 hash 所有 field/value。
    pub fn hash_get_all(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw(key, version)
            .into_iter()
            .filter_map(|(field, value)| {
                match (String::from_utf8(field), String::from_utf8(value)) {
                    (Ok(field), Ok(value)) => Some((field, value)),
                    _ => None,
                }
            })
            .collect())
    }

    pub async fn hash_get_all_async(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(field, value)| {
                match (String::from_utf8(field), String::from_utf8(value)) {
                    (Ok(field), Ok(value)) => Some((field, value)),
                    _ => None,
                }
            })
            .collect())
    }

    /// 返回 hash 所有 field。
    pub fn hash_keys(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all(key)?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    pub async fn hash_keys_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_async(key)
            .await?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    /// 返回 hash 所有 value。
    pub fn hash_values(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all(key)?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub async fn hash_values_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_async(key)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub fn hash_random_fields(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<HashRandomFields>, Error> {
        let mut entries = self.hash_get_all(key)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            let (field, value) = entries.remove(0);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            entries
                .into_iter()
                .take((count as usize).min(len))
                .collect::<Vec<_>>()
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|idx| entries[idx % len].clone())
                .collect::<Vec<_>>()
        };
        Ok(Some(
            selected
                .into_iter()
                .map(|(field, value)| (field, with_values.then_some(value)))
                .collect(),
        ))
    }

    pub async fn hash_random_fields_async(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<HashRandomFields>, Error> {
        let mut entries = self.hash_get_all_async(key).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let seed = now_ms() as usize;
        let len = entries.len();
        entries.rotate_left(seed % len);

        let Some(count) = count else {
            let (field, value) = entries.remove(0);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            entries
                .into_iter()
                .take((count as usize).min(len))
                .collect::<Vec<_>>()
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|idx| entries[idx % len].clone())
                .collect::<Vec<_>>()
        };
        Ok(Some(
            selected
                .into_iter()
                .map(|(field, value)| (field, with_values.then_some(value)))
                .collect(),
        ))
    }
}
