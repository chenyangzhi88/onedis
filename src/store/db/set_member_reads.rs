use super::*;

impl Db {
    /// 检查 member 是否属于 set。
    pub fn set_contains(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        if meta.packed {
            return Ok(self
                .store
                .get_raw(&self.mk(key))
                .as_deref()
                .and_then(decode_packed_set)
                .is_some_and(|members| members.contains(member)));
        }

        Ok(self
            .store
            .contains_key(&set_member_key(self.db_index, key, meta.version, member)))
    }

    pub async fn set_contains_async(&self, key: &str, member: &str) -> Result<bool, Error> {
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(false);
        };

        if meta.packed {
            return Ok(self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_set)
                .is_some_and(|members| members.contains(member)));
        }

        Ok(self
            .store
            .contains_key_async(&set_member_key(self.db_index, key, meta.version, member))
            .await)
    }

    /// Check several members with one metadata lookup and one storage multi-get.
    pub async fn set_multi_contains_async(
        &self,
        key: &str,
        members: &[String],
    ) -> Result<Vec<bool>, Error> {
        let Some(meta) = self.set_meta_async(key).await? else {
            return Ok(vec![false; members.len()]);
        };
        if meta.packed {
            let packed = self
                .store
                .get_raw_async(&self.mk(key))
                .await
                .as_deref()
                .and_then(decode_packed_set)
                .ok_or_else(|| Error::msg("Failed to decode packed set"))?;
            return Ok(members
                .iter()
                .map(|member| packed.contains(member))
                .collect());
        }
        let member_keys = members
            .iter()
            .map(|member| set_member_key(self.db_index, key, meta.version, member))
            .collect::<Vec<_>>();
        Ok(self
            .store
            .multi_get_raw_async(&member_keys)
            .await
            .into_iter()
            .map(|value| value.is_some())
            .collect())
    }

    /// Batch several SMISMEMBER commands across a client pipeline. Metadata and member
    /// probes are each collapsed into one storage multi-get while replies retain order.
    pub(crate) async fn set_multi_contains_batch_async(
        &self,
        commands: &[(&str, Vec<String>)],
    ) -> Vec<Result<Vec<bool>, Error>> {
        let meta_keys = commands
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        let metas = self.store.multi_get_raw_async(&meta_keys).await;
        let now = now_ms();
        let mut member_keys = Vec::new();
        let mut plans = Vec::with_capacity(commands.len());
        for ((key, members), raw) in commands.iter().zip(metas) {
            let Some(raw) = raw else {
                plans.push(SetMultiContainsPlan::Missing(members.len()));
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                plans.push(SetMultiContainsPlan::Error(
                    "Failed to decode set metadata".to_string(),
                ));
                continue;
            };
            if header.expire_ms > 0 && now >= header.expire_ms {
                plans.push(SetMultiContainsPlan::Missing(members.len()));
                continue;
            }
            if header.type_tag != TYPE_SET {
                plans.push(SetMultiContainsPlan::Error(WRONG_TYPE_ERROR.to_string()));
                continue;
            }
            let Some(meta) = decode_set_meta(&raw) else {
                plans.push(SetMultiContainsPlan::Error(
                    "Failed to decode set metadata".to_string(),
                ));
                continue;
            };
            if meta.packed {
                let Some(packed) = decode_packed_set(&raw) else {
                    plans.push(SetMultiContainsPlan::Error(
                        "Failed to decode packed set".to_string(),
                    ));
                    continue;
                };
                plans.push(SetMultiContainsPlan::Packed(
                    members
                        .iter()
                        .map(|member| packed.contains(member))
                        .collect(),
                ));
                continue;
            }
            let lookup = member_keys.len();
            member_keys.extend(
                members
                    .iter()
                    .map(|member| set_member_key(self.db_index, key, meta.version, member)),
            );
            plans.push(SetMultiContainsPlan::Members {
                lookup,
                count: members.len(),
            });
        }
        let values = self.store.multi_get_raw_async(&member_keys).await;
        plans
            .into_iter()
            .map(|plan| match plan {
                SetMultiContainsPlan::Missing(count) => Ok(vec![false; count]),
                SetMultiContainsPlan::Error(message) => Err(Error::msg(message)),
                SetMultiContainsPlan::Packed(values) => Ok(values),
                SetMultiContainsPlan::Members { lookup, count } => Ok(values
                    [lookup..lookup.saturating_add(count)]
                    .iter()
                    .map(Option::is_some)
                    .collect()),
            })
            .collect()
    }

    /// 返回 set 成员数量。
    pub fn set_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self.set_meta(key)?.map_or(0, |meta| meta.len))
    }

    pub async fn set_len_async(&self, key: &str) -> Result<usize, Error> {
        Ok(self.set_meta_async(key).await?.map_or(0, |meta| meta.len))
    }

    /// 返回 set 所有成员。
    pub fn set_members(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw(key, meta.version)
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    /// Returns set members without allowing a single response to materialize an
    /// unbounded amount of data first.
    pub fn set_members_bounded(
        &self,
        key: &str,
        max_members: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<String>, Error> {
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        if meta.len > max_members {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }

        if meta.packed {
            let members = self
                .set_members_raw(key, meta.version)
                .into_iter()
                .map(|member| {
                    String::from_utf8(member)
                        .map_err(|_| Error::msg("ERR invalid UTF-8 set member"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let encoded_bytes = members.iter().try_fold(32usize, |bytes, member| {
                bytes.checked_add(member.len().saturating_add(32))
            });
            if encoded_bytes.is_none_or(|bytes| bytes > max_encoded_bytes) {
                return Err(Error::msg("ERR response exceeds configured limit"));
            }
            return Ok(members);
        }

        let prefix = set_member_prefix(self.db_index, key, meta.version);
        let mut members = Vec::with_capacity(meta.len.min(max_members));
        let mut encoded_bytes = 32usize;
        let mut exceeded = false;
        self.store.scan_range_raw_visit(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            max_members.saturating_add(1),
            |member_key, _| {
                let Some(raw_member) = member_key.strip_prefix(prefix.as_slice()) else {
                    return true;
                };
                let Ok(member) = String::from_utf8(raw_member.to_vec()) else {
                    return true;
                };
                let Some(next_bytes) = encoded_bytes.checked_add(member.len().saturating_add(32))
                else {
                    exceeded = true;
                    return false;
                };
                if members.len() >= max_members || next_bytes > max_encoded_bytes {
                    exceeded = true;
                    return false;
                }
                encoded_bytes = next_bytes;
                members.push(member);
                true
            },
        );

        if exceeded {
            Err(Error::msg("ERR response exceeds configured limit"))
        } else {
            Ok(members)
        }
    }

    pub async fn set_members_async(&self, key: &str) -> Result<Vec<String>, Error> {
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };

        Ok(self
            .set_members_raw_async(key, meta.version)
            .await
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect())
    }

    pub async fn set_members_bounded_async(
        &self,
        key: &str,
        max_members: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<String>, Error> {
        let key = key.to_owned();
        self.run_blocking_store_task(move |db| {
            db.set_members_bounded(&key, max_members, max_encoded_bytes)
        })
        .await
    }

    pub(in crate::store::db) async fn set_member_set_async(
        &self,
        key: &str,
    ) -> Result<Option<HashSet<String>>, Error> {
        match self.set_meta_async(key).await? {
            Some(meta) => Ok(Some(
                self.set_members_raw_async(key, meta.version)
                    .await
                    .into_iter()
                    .filter_map(|member| String::from_utf8(member).ok())
                    .collect(),
            )),
            None => Ok(None),
        }
    }
}

enum SetMultiContainsPlan {
    Missing(usize),
    Error(String),
    Packed(Vec<bool>),
    Members { lookup: usize, count: usize },
}
