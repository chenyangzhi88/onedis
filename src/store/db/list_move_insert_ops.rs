use super::*;

impl Db {
    pub fn list_move(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
        let value = if source_left {
            self.list_pop_left(source)?
        } else {
            self.list_pop_right(source)?
        };
        let Some(value) = value else {
            return Ok(None);
        };

        let moved = std::slice::from_ref(&value);
        if destination_left {
            self.list_push_left(destination, moved, false)?;
        } else {
            self.list_push_right(destination, moved, false)?;
        }
        Ok(Some(value))
    }

    pub async fn list_move_async(
        &self,
        source: &str,
        destination: &str,
        source_left: bool,
        destination_left: bool,
    ) -> Result<Option<String>, Error> {
        let value = if source_left {
            self.list_pop_left_async(source).await?
        } else {
            self.list_pop_right_async(source).await?
        };
        let Some(value) = value else {
            return Ok(None);
        };

        let moved = std::slice::from_ref(&value);
        if destination_left {
            self.list_push_left_async(destination, moved, false).await?;
        } else {
            self.list_push_right_async(destination, moved, false)
                .await?;
        }
        Ok(Some(value))
    }

    pub fn list_insert(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        element: &str,
    ) -> Result<i64, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let mut items = self.list_range(key, 0, -1)?;
        let Some(pivot_index) = items.iter().position(|value| value == pivot) else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        items.insert(insert_index, element.to_string());

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for (index, value) in items.iter().enumerate() {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, index as i64),
                value.as_bytes(),
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, 0, items.len() as i64),
        );
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(items.len() as i64)
    }

    pub async fn list_insert_async(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        element: &str,
    ) -> Result<i64, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let mut items = self.list_range_async(key, 0, -1).await?;
        let Some(pivot_index) = items.iter().position(|value| value == pivot) else {
            return Ok(-1);
        };
        let insert_index = if before {
            pivot_index
        } else {
            pivot_index.saturating_add(1)
        };
        items.insert(insert_index, element.to_string());

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for (index, value) in items.iter().enumerate() {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, index as i64),
                value.as_bytes(),
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, 0, items.len() as i64),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(items.len() as i64)
    }

    pub fn list_multi_pop(
        &self,
        keys: &[String],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        for key in keys {
            if self.list_len(key)? == 0 {
                continue;
            }

            let mut values = Vec::new();
            for _ in 0..count {
                let value = if left {
                    self.list_pop_left(key)?
                } else {
                    self.list_pop_right(key)?
                };
                match value {
                    Some(value) => values.push(value),
                    None => break,
                }
            }
            if !values.is_empty() {
                return Ok(Some((key.clone(), values)));
            }
        }
        Ok(None)
    }

    pub async fn list_multi_pop_async(
        &self,
        keys: &[String],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        for key in keys {
            if self.list_len(key)? == 0 {
                continue;
            }

            let mut values = Vec::new();
            for _ in 0..count {
                let value = if left {
                    self.list_pop_left_async(key).await?
                } else {
                    self.list_pop_right_async(key).await?
                };
                match value {
                    Some(value) => values.push(value),
                    None => break,
                }
            }
            if !values.is_empty() {
                return Ok(Some((key.clone(), values)));
            }
        }
        Ok(None)
    }

    pub fn list_blocking_pop_once(
        &self,
        keys: &[String],
        left: bool,
    ) -> Result<Option<(String, String)>, Error> {
        for key in keys {
            let value = if left {
                self.list_pop_left(key)?
            } else {
                self.list_pop_right(key)?
            };
            if let Some(value) = value {
                return Ok(Some((key.clone(), value)));
            }
        }
        Ok(None)
    }
}
