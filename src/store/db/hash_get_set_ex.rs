use super::*;

impl Db {
    pub fn hash_multi_set(&self, key: &str, fields: &HashMap<String, String>) -> Result<(), Error> {
        let meta = self.hash_expire_ms(key)?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
        };
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(&self.mk(key), &encode_hash_meta(0, version));
        }

        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            batch.delete(&hash_field_expire_key(self.db_index, key, version, field));
        }

        if batch.count() > 0 {
            self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, key)?;
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_refresh(key)?;
        }
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
        let values = self.hash_multi_get(key, fields)?;
        self.hash_delete(key, fields)?;
        Ok(values)
    }

    pub async fn hash_get_del_async(
        &self,
        key: &str,
        fields: &[String],
    ) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        self.hash_delete_async(key, fields).await?;
        Ok(values)
    }

    pub fn hash_get_ex(
        &self,
        key: &str,
        fields: &[String],
        expiration: Option<StringExpireUpdate>,
    ) -> Result<Vec<Option<String>>, Error> {
        let values = self.hash_multi_get(key, fields)?;
        let Some(expiration) = expiration else {
            return Ok(values);
        };
        match expiration {
            StringExpireUpdate::Persist => {
                self.hash_persist_fields(key, fields)?;
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                let expire_ms = now_ms().saturating_add(ttl_ms);
                self.hash_expire_fields_at_ms(key, expire_ms, fields, ExpireCondition::Always)?;
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
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
        let values = self.hash_multi_get(key, fields)?;
        let Some(expiration) = expiration else {
            return Ok(values);
        };
        match expiration {
            StringExpireUpdate::Persist => {
                self.hash_persist_fields_async(key, fields).await?;
            }
            StringExpireUpdate::RelativeMs(ttl_ms) => {
                let expire_ms = now_ms().saturating_add(ttl_ms);
                self.hash_expire_fields_at_ms_async(
                    key,
                    expire_ms,
                    fields,
                    ExpireCondition::Always,
                )
                .await?;
            }
            StringExpireUpdate::AbsoluteMs(expire_ms) => {
                self.hash_expire_fields_at_ms_async(
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
        let meta = self.hash_expire_ms(key)?;
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

        let expire_ms = match expiration {
            Some(StringExpireUpdate::RelativeMs(ttl_ms)) => Some(now_ms().saturating_add(ttl_ms)),
            Some(StringExpireUpdate::AbsoluteMs(expire_ms)) => Some(expire_ms),
            Some(StringExpireUpdate::Persist) => Some(0),
            None => None,
        };
        let field_ttl_requested = expire_ms.is_some_and(|expire_ms| expire_ms > 0);
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        } else if field_ttl_requested {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(meta.unwrap().0, version, true),
            );
        }
        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if let Some(expire_ms) = expire_ms {
                if expire_ms > 0 {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                } else {
                    batch.delete(&expire_key);
                }
            } else if !keep_ttl {
                batch.delete(&expire_key);
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
        let meta = self.hash_expire_ms_async(key).await?;
        let version = match meta {
            Some((_, v)) => v,
            None => self.next_persisted_version(),
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

        let expire_ms = match expiration {
            Some(StringExpireUpdate::RelativeMs(ttl_ms)) => Some(now_ms().saturating_add(ttl_ms)),
            Some(StringExpireUpdate::AbsoluteMs(expire_ms)) => Some(expire_ms),
            Some(StringExpireUpdate::Persist) => Some(0),
            None => None,
        };
        let field_ttl_requested = expire_ms.is_some_and(|expire_ms| expire_ms > 0);
        let mut batch = WriteBatch::new();
        if meta.is_none() {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(0, version, field_ttl_requested),
            );
        } else if field_ttl_requested {
            batch.put(
                &self.mk(key),
                &encode_hash_meta_with_field_ttl_flag(meta.unwrap().0, version, true),
            );
        }
        for (field, value) in fields {
            batch.put(
                &hash_field_key(self.db_index, key, version, field),
                value.as_bytes(),
            );
            let expire_key = hash_field_expire_key(self.db_index, key, version, field);
            if let Some(expire_ms) = expire_ms {
                if expire_ms > 0 {
                    batch.put(&expire_key, &expire_ms.to_be_bytes());
                } else {
                    batch.delete(&expire_key);
                }
            } else if !keep_ttl {
                batch.delete(&expire_key);
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
