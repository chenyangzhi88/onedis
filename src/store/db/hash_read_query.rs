use super::*;
use std::hash::{BuildHasher, Hash, Hasher};

pub type HashRandomField = (String, Option<String>);
pub type HashRandomFields = Vec<HashRandomField>;
pub type HashRandomFieldBytes = (String, Option<Vec<u8>>);
pub type HashRandomFieldsBytes = Vec<HashRandomFieldBytes>;

const MAX_HASH_RANDOM_RESPONSE_ITEMS: u64 = 1_000_000;

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
        Ok(self
            .hash_multi_get_bytes(key, fields)?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub fn hash_multi_get_bytes(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        Ok(fields
            .iter()
            .map(|field| self.hash_live_field_value(key, version, field))
            .collect())
    }

    pub async fn hash_multi_get_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        Ok(self
            .hash_multi_get_bytes_async(key, fields)
            .await?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub async fn hash_multi_get_bytes_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(vec![None; fields.len()]);
        };

        let mut values = Vec::with_capacity(fields.len());
        for field in fields {
            values.push(self.hash_live_field_value_async(key, version, field).await);
        }
        Ok(values)
    }

    /// 返回 hash 所有 field/value。
    pub fn hash_get_all(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        Ok(self
            .hash_get_all_bytes(key)?
            .into_iter()
            .filter_map(|(field, value)| String::from_utf8(value).ok().map(|value| (field, value)))
            .collect())
    }

    pub fn hash_get_all_bytes(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw(key, version)
            .into_iter()
            .filter_map(|(field, value)| String::from_utf8(field).ok().map(|field| (field, value)))
            .collect())
    }

    pub async fn hash_get_all_async(&self, key: &str) -> Result<Vec<(String, String)>, Error> {
        Ok(self
            .hash_get_all_bytes_async(key)
            .await?
            .into_iter()
            .filter_map(|(field, value)| String::from_utf8(value).ok().map(|value| (field, value)))
            .collect())
    }

    pub async fn hash_get_all_bytes_async(
        &self,
        key: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let meta = self.hash_expire_ms_async(key).await?;
        let Some((_, version)) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_raw_async(key, version)
            .await
            .into_iter()
            .filter_map(|(field, value)| String::from_utf8(field).ok().map(|field| (field, value)))
            .collect())
    }

    /// 返回 hash 所有 field。
    pub fn hash_keys(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_bytes(key)?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    pub async fn hash_keys_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_get_all_bytes_async(key)
            .await?
            .into_iter()
            .map(|(field, _)| field)
            .collect())
    }

    /// 返回 hash 所有 value。
    pub fn hash_values(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_values_bytes(key)?
            .into_iter()
            .filter_map(|value| String::from_utf8(value).ok())
            .collect())
    }

    pub fn hash_values_bytes(&self, key: &str) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .hash_get_all_bytes(key)?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub async fn hash_values_async(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .hash_values_bytes_async(key)
            .await?
            .into_iter()
            .filter_map(|value| String::from_utf8(value).ok())
            .collect())
    }

    pub async fn hash_values_bytes_async(&self, key: &str) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .hash_get_all_bytes_async(key)
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
        Ok(self
            .hash_random_fields_bytes(key, count, with_values)?
            .map(|entries| {
                entries
                    .into_iter()
                    .filter_map(|(field, value)| match value {
                        Some(value) => String::from_utf8(value)
                            .ok()
                            .map(|value| (field, Some(value))),
                        None => Some((field, None)),
                    })
                    .collect()
            }))
    }

    pub fn hash_random_fields_bytes(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<HashRandomFieldsBytes>, Error> {
        validate_hash_random_count(count, with_values)?;
        let mut entries = self.hash_get_all_bytes(key)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let len = entries.len();
        let mut random = HashRandom::new();

        let Some(count) = count else {
            let selected = random.index(len);
            let (field, value) = entries.swap_remove(selected);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            let requested = (count as usize).min(len);
            shuffle_prefix(&mut entries, requested, &mut random);
            entries.truncate(requested);
            entries
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|_| entries[random.index(len)].clone())
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
        Ok(self
            .hash_random_fields_bytes_async(key, count, with_values)
            .await?
            .map(|entries| {
                entries
                    .into_iter()
                    .filter_map(|(field, value)| match value {
                        Some(value) => String::from_utf8(value)
                            .ok()
                            .map(|value| (field, Some(value))),
                        None => Some((field, None)),
                    })
                    .collect()
            }))
    }

    pub async fn hash_random_fields_bytes_async(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Option<HashRandomFieldsBytes>, Error> {
        validate_hash_random_count(count, with_values)?;
        let mut entries = self.hash_get_all_bytes_async(key).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let len = entries.len();
        let mut random = HashRandom::new();

        let Some(count) = count else {
            let selected = random.index(len);
            let (field, value) = entries.swap_remove(selected);
            return Ok(Some(vec![(field, with_values.then_some(value))]));
        };
        let selected = if count >= 0 {
            let requested = (count as usize).min(len);
            shuffle_prefix(&mut entries, requested, &mut random);
            entries.truncate(requested);
            entries
        } else {
            let requested = count.unsigned_abs() as usize;
            (0..requested)
                .map(|_| entries[random.index(len)].clone())
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

fn validate_hash_random_count(count: Option<i64>, with_values: bool) -> Result<(), Error> {
    let Some(count) = count else {
        return Ok(());
    };
    let max = if with_values {
        MAX_HASH_RANDOM_RESPONSE_ITEMS / 2
    } else {
        MAX_HASH_RANDOM_RESPONSE_ITEMS
    };
    if count.unsigned_abs() > max {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

struct HashRandom {
    state: u64,
}

impl HashRandom {
    fn new() -> Self {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let random_state = std::collections::hash_map::RandomState::new();
        let mut hasher = random_state.build_hasher();
        NONCE.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
        now_ms().hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        let state = hasher.finish();
        Self {
            state: if state == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                state
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next_u64() % upper as u64) as usize
    }
}

fn shuffle_prefix<T>(values: &mut [T], requested: usize, random: &mut HashRandom) {
    for idx in 0..requested {
        let selected = idx + random.index(values.len() - idx);
        values.swap(idx, selected);
    }
}
