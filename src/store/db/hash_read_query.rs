use super::*;
use std::hash::{BuildHasher, Hash, Hasher};

pub type HashRandomField = (String, Option<String>);
pub type HashRandomFields = Vec<HashRandomField>;
pub type HashRandomFieldBytes = (String, Option<Vec<u8>>);
pub type HashRandomFieldsBytes = Vec<HashRandomFieldBytes>;

enum HashMultiGetPlan {
    Missing(usize),
    Error(String),
    Packed(Vec<Option<Vec<u8>>>),
    Fields {
        lookup: usize,
        expire_lookup: usize,
        count: usize,
        may_have_field_ttl: bool,
    },
}

const MAX_HASH_RANDOM_RESPONSE_ITEMS: u64 = 1_000_000;

impl Db {
    pub(crate) async fn hash_len_batch_async(
        &self,
        command_keys: &[&str],
    ) -> Vec<Result<usize, Error>> {
        let mut key_positions = HashMap::with_capacity(command_keys.len());
        let mut keys = Vec::new();
        for key in command_keys {
            if !key_positions.contains_key(key) {
                key_positions.insert(*key, keys.len());
                keys.push(*key);
            }
        }
        let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let metas = match self.store.multi_get_raw_async(&meta_keys).await {
            Ok(values) => values,
            Err(error) => return storage_batch_error(command_keys.len(), error),
        };
        let now = now_ms();
        let mut lengths = Vec::with_capacity(keys.len());
        for (key, raw) in keys.iter().zip(metas) {
            let result = match raw {
                None => Ok(0),
                Some(raw) => match decode_hash_meta_checked(&raw) {
                    Err(error) => Err(error.to_string()),
                    Ok(meta) if meta.expire_ms > 0 && now >= meta.expire_ms => Ok(0),
                    Ok(meta) if meta.packed => decode_packed_hash(&raw)
                        .map(|fields| fields.len())
                        .ok_or_else(|| "Failed to decode packed hash".to_string()),
                    Ok(meta) if meta.may_have_field_ttl => self
                        .hash_live_entries_for_meta_async(key, meta)
                        .await
                        .map(|entries| entries.len())
                        .map_err(|error| error.to_string()),
                    Ok(meta) => {
                        let logical_key = key.as_bytes().to_vec();
                        let key_epoch = self
                            .counter_cache
                            .hash_key_epoch(self.db_index, &logical_key);
                        let cache_key = (self.db_index, logical_key);
                        if let Some(cached) = self.counter_cache.hash_lengths.get(&cache_key)
                            && cached.version == meta.version
                            && cached.key_epoch == key_epoch
                        {
                            Ok(cached.len)
                        } else {
                            let prefix = hash_field_prefix(self.db_index, key, meta.version);
                            let len = match self
                                .store
                                .count_range_raw_keys_async(
                                    &prefix,
                                    prefix_exclusive_upper_bound(&prefix),
                                )
                                .await
                            {
                                Ok(len) => len,
                                Err(error) => {
                                    lengths.push(Err(error.to_string()));
                                    continue;
                                }
                            };
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
                    }
                },
            };
            lengths.push(result);
        }
        command_keys
            .iter()
            .map(|key| match &lengths[key_positions[key]] {
                Ok(len) => Ok(*len),
                Err(message) => Err(Error::msg(message.clone())),
            })
            .collect()
    }

    pub fn hash_exists(&self, key: &str, field: &str) -> Result<bool, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(false);
        };

        Ok(self.hash_live_field_value(key, version, field)?.is_some())
    }

    pub async fn hash_exists_async(&self, key: &str, field: &str) -> Result<bool, Error> {
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(false);
        };
        if meta.packed {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await?
                .and_then(|raw| decode_packed_hash(&raw))
                .is_some_and(|fields| fields.contains_key(field)));
        }
        if meta.may_have_field_ttl
            && !self
                .hash_field_is_live_async(key, meta.version, field)
                .await?
        {
            return Ok(false);
        }
        Ok(self
            .store
            .get_raw_async(&hash_field_key(self.db_index, key, meta.version, field))
            .await?
            .is_some())
    }

    /// 返回 hash field 数量。
    pub fn hash_len(&self, key: &str) -> Result<usize, Error> {
        let meta = self.hash_expire_ms(key)?;
        let Some((_, version)) = meta else {
            return Ok(0);
        };

        Ok(self.hash_live_entries_raw(key, version)?.len())
    }

    pub async fn hash_len_async(&self, key: &str) -> Result<usize, Error> {
        let Some(meta) = self.hash_meta_async(key).await? else {
            return Ok(0);
        };
        if meta.packed {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await?
                .and_then(|raw| decode_packed_hash(&raw))
                .map_or(0, |fields| fields.len()));
        }
        if meta.may_have_field_ttl {
            return Ok(self
                .hash_live_entries_for_meta_async(key, meta)
                .await?
                .len());
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
        let prefix = hash_field_prefix(self.db_index, key, meta.version);
        let len = self
            .store
            .count_range_raw_keys_async(&prefix, prefix_exclusive_upper_bound(&prefix))
            .await?;
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

        fields
            .iter()
            .map(|field| self.hash_live_field_value(key, version, field))
            .collect()
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
        if meta.packed {
            let packed = self
                .store
                .get_raw_async(&self.mk(key))
                .await?
                .and_then(|raw| decode_packed_hash(&raw))
                .unwrap_or_default();
            return Ok(fields
                .iter()
                .map(|field| packed.get(field).cloned())
                .collect());
        }
        let field_keys = fields
            .iter()
            .map(|field| hash_field_key(self.db_index, key, meta.version, field))
            .collect::<Vec<_>>();
        let mut values = self.store.multi_get_raw_async(&field_keys).await?;
        if meta.may_have_field_ttl {
            let expire_keys = fields
                .iter()
                .map(|field| hash_field_expire_key(self.db_index, key, meta.version, field))
                .collect::<Vec<_>>();
            let expires = self.store.multi_get_raw_async(&expire_keys).await?;
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

    /// Batch independent Hash reads across a client pipeline. All metadata, field values and
    /// optional field-expiry records are fetched in at most three storage multi-gets.
    pub(crate) async fn hash_multi_get_bytes_batch_async(
        &self,
        commands: &[(&str, Vec<String>)],
    ) -> Vec<Result<Vec<Option<Vec<u8>>>, Error>> {
        let meta_keys = commands
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let metas = match self.store.multi_get_raw_async(&meta_keys).await {
            Ok(values) => values,
            Err(error) => return storage_batch_error(commands.len(), error),
        };
        let now = now_ms();
        let mut field_keys = Vec::new();
        let mut expire_keys = Vec::new();
        let mut plans = Vec::with_capacity(commands.len());

        for ((key, fields), raw) in commands.iter().zip(metas) {
            let Some(raw) = raw else {
                plans.push(HashMultiGetPlan::Missing(fields.len()));
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                plans.push(HashMultiGetPlan::Error(
                    "Failed to decode hash metadata".to_string(),
                ));
                continue;
            };
            if header.expire_ms > 0 && now >= header.expire_ms {
                plans.push(HashMultiGetPlan::Missing(fields.len()));
                continue;
            }
            if header.type_tag != TYPE_HASH {
                plans.push(HashMultiGetPlan::Error(WRONG_TYPE_ERROR.to_string()));
                continue;
            }
            let Some(meta) = decode_hash_meta(&raw) else {
                plans.push(HashMultiGetPlan::Error(
                    "Failed to decode hash metadata".to_string(),
                ));
                continue;
            };
            if meta.packed {
                match decode_packed_hash(&raw) {
                    Some(packed) => plans.push(HashMultiGetPlan::Packed(
                        fields
                            .iter()
                            .map(|field| packed.get(field).cloned())
                            .collect(),
                    )),
                    None => plans.push(HashMultiGetPlan::Error(
                        "Failed to decode packed hash".to_string(),
                    )),
                }
                continue;
            }
            let lookup = field_keys.len();
            field_keys.extend(
                fields
                    .iter()
                    .map(|field| hash_field_key(self.db_index, key, meta.version, field)),
            );
            let expire_lookup = expire_keys.len();
            if meta.may_have_field_ttl {
                expire_keys.extend(
                    fields.iter().map(|field| {
                        hash_field_expire_key(self.db_index, key, meta.version, field)
                    }),
                );
            }
            plans.push(HashMultiGetPlan::Fields {
                lookup,
                expire_lookup,
                count: fields.len(),
                may_have_field_ttl: meta.may_have_field_ttl,
            });
        }

        let values = match self.store.multi_get_raw_async(&field_keys).await {
            Ok(values) => values,
            Err(error) => return storage_batch_error(commands.len(), error),
        };
        let expires = match self.store.multi_get_raw_async(&expire_keys).await {
            Ok(values) => values,
            Err(error) => return storage_batch_error(commands.len(), error),
        };
        plans
            .into_iter()
            .map(|plan| match plan {
                HashMultiGetPlan::Missing(count) => Ok(vec![None; count]),
                HashMultiGetPlan::Error(message) => Err(Error::msg(message)),
                HashMultiGetPlan::Packed(values) => Ok(values),
                HashMultiGetPlan::Fields {
                    lookup,
                    expire_lookup,
                    count,
                    may_have_field_ttl,
                } => {
                    let mut reply = values[lookup..lookup.saturating_add(count)].to_vec();
                    if may_have_field_ttl {
                        for (value, expire) in reply
                            .iter_mut()
                            .zip(&expires[expire_lookup..expire_lookup.saturating_add(count)])
                        {
                            if expire
                                .as_deref()
                                .and_then(decode_u64_be)
                                .is_some_and(|expire_ms| expire_ms > 0 && now >= expire_ms)
                            {
                                *value = None;
                            }
                        }
                    }
                    Ok(reply)
                }
            })
            .collect()
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
            .hash_live_entries_raw(key, version)?
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
            .await?
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
            if !meta.may_have_field_ttl && !meta.packed {
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
                    .await?;
                let values = if with_values {
                    self.store.multi_get_raw_async(&raw_keys).await?
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
