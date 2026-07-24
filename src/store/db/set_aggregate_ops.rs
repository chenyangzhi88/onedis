use super::*;

impl Db {
    pub fn set_intersection_card(&self, keys: &[String], limit: usize) -> Result<usize, Error> {
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sintercard' command",
            ));
        }
        let mut metas = Vec::with_capacity(keys.len());
        for key in keys {
            metas.push(self.set_meta(key)?);
        }
        let Some(metas) = metas.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(0);
        };
        let smallest = metas
            .iter()
            .enumerate()
            .min_by_key(|(_, meta)| meta.len)
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let prefix = set_member_prefix(self.db_index, &keys[smallest], metas[smallest].version);
        let mut count = 0usize;
        self.store.scan_range_raw_visit(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            usize::MAX,
            |member_key, _| {
                let Some(member) = member_key.strip_prefix(prefix.as_slice()) else {
                    return true;
                };
                let present_everywhere = keys.iter().enumerate().all(|(idx, key)| {
                    idx == smallest
                        || self.store.contains_key(&set_member_key_bytes(
                            self.db_index,
                            key,
                            metas[idx].version,
                            member,
                        ))
                });
                if present_everywhere {
                    count = count.saturating_add(1);
                }
                limit == 0 || count < limit
            },
        );
        Ok(count)
    }

    pub async fn set_intersection_card_async(
        &self,
        keys: &[String],
        limit: usize,
    ) -> Result<usize, Error> {
        let keys = keys.to_vec();
        self.run_blocking_store_task(move |db| db.set_intersection_card(&keys, limit))
            .await
    }
    /// 计算多个 set 的差集。不存在的 key 视为空 set。
    pub fn set_diff(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        };

        let mut difference = match self.get(first_key) {
            Some(Structure::Set(set)) => set,
            Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
            None => HashSet::new(),
        };

        for key in rest {
            match self.get(key) {
                Some(Structure::Set(set)) => {
                    for member in set {
                        difference.remove(&member);
                    }
                }
                Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
                None => {}
            }
        }

        Ok(difference)
    }

    pub async fn set_diff_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        };

        let mut difference = self
            .set_member_set_async(first_key)
            .await?
            .unwrap_or_default();
        for key in rest {
            if let Some(set) = self.set_member_set_async(key).await? {
                for member in set {
                    difference.remove(&member);
                }
            }
        }

        Ok(difference)
    }

    /// 计算 set 差集并写入目标 key，返回写入成员数量。
    pub fn set_diff_store(&self, destination: &str, keys: &[String]) -> Result<usize, Error> {
        let difference = self.set_diff(keys)?;
        let len = difference.len();
        if len == 0 {
            self.remove(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(difference));
        }
        Ok(len)
    }

    pub async fn set_diff_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let difference = self.set_diff_async(keys).await?;
        self.set_store_members_async(destination, difference).await
    }

    pub fn set_intersection(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        };

        let mut intersection = match self.get(first_key) {
            Some(Structure::Set(set)) => set,
            Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
            None => return Ok(HashSet::new()),
        };

        for key in rest {
            match self.get(key) {
                Some(Structure::Set(set)) => {
                    intersection = intersection.intersection(&set).cloned().collect();
                }
                Some(_) => return Err(Error::msg(WRONG_TYPE_ERROR)),
                None => return Ok(HashSet::new()),
            }
        }

        Ok(intersection)
    }

    pub async fn set_intersection_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let Some((first_key, rest)) = keys.split_first() else {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        };

        let Some(mut intersection) = self.set_member_set_async(first_key).await? else {
            return Ok(HashSet::new());
        };
        for key in rest {
            let Some(set) = self.set_member_set_async(key).await? else {
                return Ok(HashSet::new());
            };
            intersection = intersection.intersection(&set).cloned().collect();
        }

        Ok(intersection)
    }

    pub fn set_intersection_store(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let intersection = self.set_intersection(keys)?;
        let len = intersection.len();
        if len == 0 {
            self.delete_key(destination);
        } else {
            self.insert(destination.to_string(), Structure::Set(intersection));
        }
        Ok(len)
    }

    pub async fn set_intersection_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let intersection = self.set_intersection_async(keys).await?;
        self.set_store_members_async(destination, intersection)
            .await
    }

    pub async fn set_union_async(&self, keys: &[String]) -> Result<HashSet<String>, Error> {
        let mut result = HashSet::new();
        for key in keys {
            if let Some(set) = self.set_member_set_async(key).await? {
                result.extend(set);
            }
        }
        Ok(result)
    }

    pub async fn set_union_store_async(
        &self,
        destination: &str,
        keys: &[String],
    ) -> Result<usize, Error> {
        let union = self.set_union_async(keys).await?;
        self.set_store_members_async(destination, union).await
    }

    async fn set_store_members_async(
        &self,
        destination: &str,
        members: HashSet<String>,
    ) -> Result<usize, Error> {
        let _write_guard = self.set_write_lock(destination).lock().await;
        self.delete_key_internal_async(destination, true).await;
        let len = members.len();
        if len == 0 {
            return Ok(0);
        }
        let members = members.into_iter().collect::<Vec<_>>();
        self.set_add_async_unlocked(destination, &members).await?;
        Ok(len)
    }
}
