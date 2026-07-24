use super::*;

#[derive(Clone, Copy)]
enum ResolvedHashExpiration {
    Persist,
    At(u64),
}

fn resolve_hash_expiration(
    expiration: StringExpireUpdate,
) -> Result<ResolvedHashExpiration, Error> {
    let resolved = match expiration {
        StringExpireUpdate::Persist => ResolvedHashExpiration::Persist,
        StringExpireUpdate::RelativeMs(ttl_ms) => ResolvedHashExpiration::At(
            now_ms()
                .checked_add(ttl_ms)
                .ok_or_else(|| Error::msg("ERR invalid expire time in hash command"))?,
        ),
        StringExpireUpdate::AbsoluteMs(expire_ms) => ResolvedHashExpiration::At(expire_ms),
    };
    if let ResolvedHashExpiration::At(expire_ms) = resolved
        && expire_ms > HASH_FIELD_MAX_EXPIRE_MS
    {
        return Err(Error::msg("ERR invalid expire time in hash command"));
    }
    Ok(resolved)
}

impl Db {
    pub fn hash_multi_set(&self, key: &str, fields: &HashMap<String, String>) -> Result<(), Error> {
        let items = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect::<Vec<_>>();
        self.hash_set_many(key, &items)?;
        Ok(())
    }

    pub async fn hash_multi_set_async(
        &self,
        key: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Error> {
        let items = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect::<Vec<_>>();
        self.hash_set_many_async(key, &items).await?;
        Ok(())
    }

    pub fn hash_get_del(&self, key: &str, fields: &[String]) -> Result<Vec<Option<String>>, Error> {
        Ok(self
            .hash_get_del_bytes(key, fields)?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub fn hash_get_del_bytes(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut seen = HashSet::new();
        let values = fields
            .iter()
            .map(|field| {
                if seen.insert(field) {
                    self.hash_get_bytes(key, field)
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.hash_delete(key, fields)?;
        Ok(values)
    }

    pub async fn hash_get_del_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        Ok(self
            .hash_get_del_bytes_async(key, fields)
            .await?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub async fn hash_get_del_bytes_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let _hash_write_guard = self.set_write_lock(key).lock().await;
        let mut seen = HashSet::new();
        let mut values = Vec::with_capacity(fields.len());
        for field in fields {
            values.push(if seen.insert(field) {
                self.hash_get_bytes_async(key, field).await?
            } else {
                None
            });
        }
        self.hash_delete_async_unlocked(key, fields).await?;
        Ok(values)
    }

    pub fn hash_get_ex(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<String>>, Error> {
        Ok(self
            .hash_get_ex_bytes(key, fields, expiration)?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub fn hash_get_ex_bytes(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let resolved = expiration.map(resolve_hash_expiration).transpose()?;
        let delete_immediately = matches!(resolved, Some(ResolvedHashExpiration::At(expire_ms)) if expire_ms <= now_ms());
        let values = if delete_immediately {
            let mut seen = HashSet::new();
            fields
                .iter()
                .map(|field| {
                    if seen.insert(field) {
                        self.hash_get_bytes(key, field)
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>, Error>>()?
        } else {
            self.hash_multi_get_bytes(key, fields)?
        };
        let Some(expiration) = resolved else {
            return Ok(values);
        };
        match expiration {
            ResolvedHashExpiration::Persist => {
                self.hash_persist_fields(key, fields)?;
            }
            ResolvedHashExpiration::At(expire_ms) => {
                self.hash_expire_fields_at_ms(key, expire_ms, fields, ExpireCondition::Always)?;
            }
        }
        Ok(values)
    }

    pub async fn hash_get_ex_async(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<String>>, Error> {
        Ok(self
            .hash_get_ex_bytes_async(key, fields, expiration)
            .await?
            .into_iter()
            .map(|value| value.and_then(|value| String::from_utf8(value).ok()))
            .collect())
    }

    pub async fn hash_get_ex_bytes_async(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let _hash_write_guard = self.set_write_lock(key).lock().await;
        let resolved = expiration.map(resolve_hash_expiration).transpose()?;
        let delete_immediately = matches!(resolved, Some(ResolvedHashExpiration::At(expire_ms)) if expire_ms <= now_ms());
        let values = if delete_immediately {
            let mut seen = HashSet::new();
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                values.push(if seen.insert(field) {
                    self.hash_get_bytes_async(key, field).await?
                } else {
                    None
                });
            }
            values
        } else {
            self.hash_multi_get_bytes_async(key, fields).await?
        };
        let Some(expiration) = resolved else {
            return Ok(values);
        };
        match expiration {
            ResolvedHashExpiration::Persist => {
                self.hash_persist_fields_async_unlocked(key, fields).await?;
            }
            ResolvedHashExpiration::At(expire_ms) => {
                self.hash_expire_fields_at_ms_async_unlocked(
                    key,
                    expire_ms,
                    fields,
                    ExpireCondition::Always,
                )
                .await?;
            }
        }
        Ok(values)
    }

    pub fn hash_set_ex(
        &self,
        key: &str,
        fields: &[(String, String)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let fields = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        self.hash_set_ex_bytes(key, &fields, expiration, keep_ttl, fnx, fxx)
    }

    pub fn hash_set_ex_bytes(
        &self,
        key: &str,
        fields: &[(String, Vec<u8>)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let expiration = expiration.map(resolve_hash_expiration).transpose()?;
        let meta = self.hash_expire_ms(key)?;
        if fields.is_empty() {
            return Ok(true);
        }
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        if fnx
            && fields
                .iter()
                .any(|(field, _)| self.hash_live_field_value(key, version, field).is_some())
        {
            return Ok(false);
        }
        if fxx
            && fields
                .iter()
                .any(|(field, _)| self.hash_live_field_value(key, version, field).is_none())
        {
            return Ok(false);
        }

        let delete_immediately = matches!(expiration, Some(ResolvedHashExpiration::At(expire_ms)) if expire_ms <= now_ms());
        let field_ttl_requested =
            matches!(expiration, Some(ResolvedHashExpiration::At(_))) && !delete_immediately;
        let mut batch = WriteBatch::new();
        if delete_immediately {
            let Some((hash_expire_ms, _)) = meta else {
                return Ok(true);
            };
            let existing_fields = self.hash_live_entries_raw(key, version);
            let existing_names = existing_fields
                .iter()
                .filter_map(|(field, _)| String::from_utf8(field.clone()).ok())
                .collect::<HashSet<_>>();
            let deleted_names = fields
                .iter()
                .map(|(field, _)| field)
                .filter(|field| existing_names.contains(*field))
                .collect::<HashSet<_>>();
            let delete_hash = deleted_names.len() == existing_fields.len();
            for (field, _) in fields {
                batch.delete(&hash_field_key(self.db_index, key, version, field));
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            if delete_hash {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, hash_expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, TYPE_HASH);
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
            return Ok(true);
        }

        if let Some((hash_expire_ms, _)) = meta {
            if field_ttl_requested {
                batch.put(
                    &self.mk(key),
                    &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
                );
            }
        } else {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        }
        for (field, value) in fields {
            batch.put(&hash_field_key(self.db_index, key, version, field), value);
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            match expiration {
                Some(ResolvedHashExpiration::At(expire_ms)) => {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                }
                Some(ResolvedHashExpiration::Persist) => {
                    batch.delete(&expire_key);
                }
                None if !keep_ttl => {
                    batch.delete(&expire_key);
                }
                None => {}
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(true)
    }

    pub async fn hash_set_ex_async(
        &self,
        key: &str,
        fields: &[(String, String)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let fields = fields
            .iter()
            .map(|(field, value)| (field.clone(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        self.hash_set_ex_bytes_async(key, &fields, expiration, keep_ttl, fnx, fxx)
            .await
    }

    pub async fn hash_set_ex_bytes_async(
        &self,
        key: &str,
        fields: &[(String, Vec<u8>)],
        expiration: Option<StringExpireUpdate>,
        keep_ttl: bool,
        fnx: bool,
        fxx: bool,
    ) -> Result<bool, Error> {
        let expiration = expiration.map(resolve_hash_expiration).transpose()?;
        let _hash_write_guard = self.set_write_lock(key).lock().await;
        let meta = self.hash_expire_ms_async(key).await?;
        if fields.is_empty() {
            return Ok(true);
        }
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version_async().await,
        };
        if fnx || fxx {
            for (field, _) in fields {
                let exists = self
                    .hash_live_field_value_async(key, version, field)
                    .await
                    .is_some();
                if (fnx && exists) || (fxx && !exists) {
                    return Ok(false);
                }
            }
        }

        let delete_immediately = matches!(expiration, Some(ResolvedHashExpiration::At(expire_ms)) if expire_ms <= now_ms());
        let field_ttl_requested =
            matches!(expiration, Some(ResolvedHashExpiration::At(_))) && !delete_immediately;
        let mut batch = WriteBatch::new();
        if delete_immediately {
            let Some((hash_expire_ms, _)) = meta else {
                return Ok(true);
            };
            let existing_fields = self.hash_live_entries_raw_async(key, version).await;
            let existing_names = existing_fields
                .iter()
                .filter_map(|(field, _)| String::from_utf8(field.clone()).ok())
                .collect::<HashSet<_>>();
            let deleted_names = fields
                .iter()
                .map(|(field, _)| field)
                .filter(|field| existing_names.contains(*field))
                .collect::<HashSet<_>>();
            let delete_hash = deleted_names.len() == existing_fields.len();
            for (field, _) in fields {
                batch.delete(&hash_field_key(self.db_index, key, version, field));
                batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
            }
            if delete_hash {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, hash_expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, TYPE_HASH);
                self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)?;
            } else {
                self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            }
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
            return Ok(true);
        }

        if let Some((hash_expire_ms, _)) = meta {
            if field_ttl_requested {
                batch.put(
                    &self.mk(key),
                    &encode_hash_meta_with_field_ttl_flag(hash_expire_ms, version, true),
                );
            }
        } else {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        }
        for (field, value) in fields {
            batch.put(&hash_field_key(self.db_index, key, version, field), value);
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            match expiration {
                Some(ResolvedHashExpiration::At(expire_ms)) => {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                }
                Some(ResolvedHashExpiration::Persist) => {
                    batch.delete(&expire_key);
                }
                None if !keep_ttl => {
                    batch.delete(&expire_key);
                }
                None => {}
            }
        }
        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
        Ok(true)
    }
}
