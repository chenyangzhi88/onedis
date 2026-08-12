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
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(false);
        };
        if meta.may_have_field_ttl
            && !self
                .hash_field_is_live_async(key, meta.version, field)
                .await
        {
            return Ok(false);
        }
        Ok(self
            .store
            .get_raw_async(&hash_field_key(self.db_index, key, meta.version, field))
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
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(0);
        };
        if meta.may_have_field_ttl {
            return Ok(self.hash_live_entries_for_meta_async(key, meta).await.len());
        }
        let logical_key = key.as_bytes().to_vec();
        let key_epoch = self
            .counter_cache
            .hash_key_epoch(self.db_index, &logical_key);
        let cache_key = (self.db_index, logical_key);
        if let Some(cached) = self.counter_cache.hash_lengths.get(&cache_key)
            && cached.version == meta.version
            && cached.key_epoch == key_epoch
        {
            return Ok(cached.len);
        }
        let len = self.hash_live_entries_for_meta_async(key, meta).await.len();
        self.counter_cache
            .hash_ever_populated
            .store(true, Ordering::Release);
        self.counter_cache.hash_lengths.insert(
            cache_key,
            HashLenCacheEntry {
                len,
                version: meta.version,
                key_epoch,
            },
        );
        Ok(len)
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
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(vec![None; fields.len()]);
        };
        let field_keys = fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, meta.version, field))
            .collect::<Vec<_>>();
        let mut values = self.store.multi_get_raw_async(&field_keys).await;
        if meta.may_have_field_ttl {
            let expire_keys = fields
                .iter()
                .map(|field| hash_field_expire_key(self.db_index, key, meta.version, field))
                .collect::<Vec<_>>();
            let expires = self.store.multi_get_raw_async(&expire_keys).await;
            let now = now_ms();
            for (value, expire) in values.iter_mut().zip(expires) {
                if expire
                    .as_deref()
                    .and_then(decode_u64_be)
                    .is_some_and(|expire_ms| expire_ms > 0 && now >= expire_ms)
                {
                    *value = None;
                }
            }
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
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(Vec::new());
        };

        Ok(self
            .hash_live_entries_for_meta_async(key, meta)
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
        if count.is_none() || count.is_some_and(|count| count > 0) {
            let Some(meta) = self.hash_meta_async(key).await? else {
                return Ok(None);
            };
            if !meta.may_have_field_ttl {
                let len = self.hash_len_async(key).await?;
                if len == 0 {
                    return Ok(None);
                }
                let requested = count.map_or(1, |count| count as usize).min(len);
                let mut random = HashRandom::new();
                let mut ordinals = HashSet::with_capacity(requested);
                while ordinals.len() < requested {
                    ordinals.insert(random.index(len));
                }
                let mut ordinals = ordinals.into_iter().collect::<Vec<_>>();
                ordinals.sort_unstable();
                let prefix = hash_field_prefix(self.db_index, key, meta.version);
                let raw_keys = self
                    .store
                    .scan_range_raw_keys_at_ordinals_async(
                        &prefix,
                        prefix_exclusive_upper_bound(&prefix),
                        &ordinals,
                    )
                    .await;
                let values = if with_values {
                    self.store.multi_get_raw_async(&raw_keys).await
                } else {
                    vec![None; raw_keys.len()]
                };
                let entries = raw_keys
                    .into_iter()
                    .zip(values)
                    .filter_map(|(raw_key, value)| {
                        let field = raw_key.strip_prefix(prefix.as_slice())?;
                        Some((
                            String::from_utf8(field.to_vec()).ok()?,
                            if with_values { value } else { None },
                        ))
                    })
                    .collect::<Vec<_>>();
                return Ok((!entries.is_empty()).then_some(entries));
            }
        }
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
