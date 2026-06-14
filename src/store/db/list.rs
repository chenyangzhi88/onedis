impl Db {
    pub fn list_push_left(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_left_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_left_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_left_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_persisted_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            meta.head -= 1;
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.head),
                value,
            );
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_left_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let _guard = self.set_write_lock(key).lock().await;
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
            let mut meta = match raw_meta.as_deref() {
                Some(raw) => {
                    if let Some(meta) = decode_list_meta(raw) {
                        meta
                    } else {
                        let Some(header) = decode_meta_header(raw) else {
                            return Err(Error::msg("Failed to decode list metadata"));
                        };
                        if header.type_tag != TYPE_LIST {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        }
                        return Err(Error::msg("Failed to decode list metadata"));
                    }
                }
                None if only_if_exists => return Ok(0),
                None => ListMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version_async().await,
                    head: 0,
                    tail: 0,
                },
            };
            let mut batch = WriteBatch::new();
            for value in values {
                meta.head -= 1;
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, meta.head),
                    value,
                );
            }
            batch.put(
                &key_bytes,
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
            let len = (meta.tail - meta.head) as usize;
            let condition = CompareCondition::from_observed(key_bytes, &observed_meta);
            if self
                .compare_and_write_batch_if_not_empty_async(&[condition], &batch)
                .await?
            {
                self.cache_list_meta_if_non_transactional(key, meta);
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(len);
            }
        }
        Err(Error::msg("ERR list write conflict"))
    }

    /// 右侧批量入队。
    pub fn list_push_right(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes(key, &value_refs, only_if_exists)
    }

    pub async fn list_push_right_async(
        &self,
        key: &str,
        values: &[String],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let value_refs = values.iter().map(String::as_bytes).collect::<Vec<&[u8]>>();
        self.list_push_right_bytes_async(key, &value_refs, only_if_exists)
            .await
    }

    pub fn list_push_right_bytes(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None if only_if_exists => return Ok(0),
            None => ListMeta {
                expire_ms: 0,
                version: self.next_persisted_version(),
                head: 0,
                tail: 0,
            },
        };
        let mut batch = WriteBatch::new();
        for value in values {
            batch.put(
                &list_item_key(self.db_index, key, meta.version, meta.tail),
                value,
            );
            meta.tail += 1;
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
        );
        let len = (meta.tail - meta.head) as usize;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.cache_list_meta_if_non_transactional(key, meta);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(len)
    }

    pub async fn list_push_right_bytes_async(
        &self,
        key: &str,
        values: &[&[u8]],
        only_if_exists: bool,
    ) -> Result<usize, Error> {
        let _guard = self.set_write_lock(key).lock().await;
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed_meta = self.store.get_raw_observed_async(&key_bytes).await;
            let raw_meta = observed_meta.value.as_ref().map(|value| value.to_vec());
            let mut meta = match raw_meta.as_deref() {
                Some(raw) => {
                    if let Some(meta) = decode_list_meta(raw) {
                        meta
                    } else {
                        let Some(header) = decode_meta_header(raw) else {
                            return Err(Error::msg("Failed to decode list metadata"));
                        };
                        if header.type_tag != TYPE_LIST {
                            return Err(Error::msg(WRONG_TYPE_ERROR));
                        }
                        return Err(Error::msg("Failed to decode list metadata"));
                    }
                }
                None if only_if_exists => return Ok(0),
                None => ListMeta {
                    expire_ms: 0,
                    version: self.next_persisted_version_async().await,
                    head: 0,
                    tail: 0,
                },
            };
            let mut batch = WriteBatch::new();
            for value in values {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, meta.tail),
                    value,
                );
                meta.tail += 1;
            }
            batch.put(
                &key_bytes,
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
            let len = (meta.tail - meta.head) as usize;
            let condition = CompareCondition::from_observed(key_bytes, &observed_meta);
            if self
                .compare_and_write_batch_if_not_empty_async(&[condition], &batch)
                .await?
            {
                self.cache_list_meta_if_non_transactional(key, meta);
                self.changes.fetch_add(1, Ordering::Relaxed);
                return Ok(len);
            }
        }
        Err(Error::msg("ERR list write conflict"))
    }

    /// 左侧出队。
    pub fn list_pop_left(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        let item_key = list_item_key(self.db_index, key, meta.version, meta.head);
        let value = self
            .store
            .get_raw(&item_key)
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        meta.head += 1;
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty(&batch);
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    pub async fn list_pop_left_async(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        let item_key = list_item_key(self.db_index, key, meta.version, meta.head);
        let value = self
            .store
            .get_raw_async(&item_key)
            .await
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        meta.head += 1;
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    /// 右侧出队。
    pub fn list_pop_right(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        meta.tail -= 1;
        let item_key = list_item_key(self.db_index, key, meta.version, meta.tail);
        let value = self
            .store
            .get_raw(&item_key)
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty(&batch);
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    pub async fn list_pop_right_async(&self, key: &str) -> Result<Option<String>, Error> {
        let mut meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        if meta.head >= meta.tail {
            let mut batch = WriteBatch::new();
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            self.remove_list_meta_cache_if_non_transactional(key);
            return Ok(None);
        }

        meta.tail -= 1;
        let item_key = list_item_key(self.db_index, key, meta.version, meta.tail);
        let value = self
            .store
            .get_raw_async(&item_key)
            .await
            .and_then(|value| String::from_utf8(value).ok());
        let mut batch = WriteBatch::new();
        batch.delete(&item_key);
        if meta.head >= meta.tail {
            batch.delete(&self.mk(key));
        } else {
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, meta.head, meta.tail),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        if meta.head >= meta.tail {
            self.remove_list_meta_cache_if_non_transactional(key);
        } else {
            self.cache_list_meta_if_non_transactional(key, meta);
        }
        if value.is_some() {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
    }

    /// 返回队列长度。
    pub fn list_len(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .list_meta(key)?
            .map(|meta| (meta.tail - meta.head) as usize)
            .unwrap_or(0))
    }

    pub async fn list_len_async(&self, key: &str) -> Result<usize, Error> {
        Ok(self
            .list_meta_async(key)
            .await?
            .map(|meta| (meta.tail - meta.head) as usize)
            .unwrap_or(0))
    }

    /// 返回指定下标的元素。
    pub fn list_index(&self, key: &str, index: i64) -> Result<Option<String>, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        let Some(storage_index) = self.resolve_list_index(meta, index) else {
            return Ok(None);
        };

        Ok(self
            .store
            .get_raw(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ))
            .and_then(|value| String::from_utf8(value).ok()))
    }

    pub async fn list_index_async(&self, key: &str, index: i64) -> Result<Option<String>, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        let Some(storage_index) = self.resolve_list_index(meta, index) else {
            return Ok(None);
        };

        Ok(self
            .store
            .get_raw_async(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ))
            .await
            .and_then(|value| String::from_utf8(value).ok()))
    }

    /// 返回指定范围的元素。
    pub fn list_range(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        let trace_id = trace_lrange_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let meta_started_at = trace_id.map(|_| Instant::now());
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(Vec::new()),
        };
        let meta_us = meta_started_at.map(|started| started.elapsed().as_micros());

        let resolve_started_at = trace_id.map(|_| Instant::now());
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            return Ok(Vec::new());
        };
        let resolve_us = resolve_started_at.map(|started| started.elapsed().as_micros());

        let raw_started_at = trace_id.map(|_| Instant::now());
        let raw_values = self.list_range_raw_values(key, meta.version, storage_start, storage_end);
        let raw_us = raw_started_at.map(|started| started.elapsed().as_micros());
        let convert_started_at = trace_id.map(|_| Instant::now());
        let mut result = Vec::with_capacity(raw_values.len());
        for value in raw_values {
            let value =
                String::from_utf8(value).map_err(|_| Error::msg("ERR invalid UTF-8 list value"))?;
            result.push(value);
        }
        if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
            let convert_us = convert_started_at
                .map(|started| started.elapsed().as_micros())
                .unwrap_or_default();
            eprintln!(
                "lrange-trace onedis_list_range sample={} key={} req=[{},{}] storage=[{},{}] items={} meta_us={} resolve_us={} raw_us={} convert_us={} total_us={}",
                trace_id,
                key,
                start,
                stop,
                storage_start,
                storage_end,
                result.len(),
                meta_us.unwrap_or_default(),
                resolve_us.unwrap_or_default(),
                raw_us.unwrap_or_default(),
                convert_us,
                total_started_at.elapsed().as_micros(),
            );
        }
        Ok(result)
    }

    pub async fn list_range_async(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<String>, Error> {
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(Vec::new()),
        };

        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            return Ok(Vec::new());
        };

        let raw_values = self
            .list_range_raw_values_async(key, meta.version, storage_start, storage_end)
            .await;
        let mut result = Vec::with_capacity(raw_values.len());
        for value in raw_values {
            let value =
                String::from_utf8(value).map_err(|_| Error::msg("ERR invalid UTF-8 list value"))?;
            result.push(value);
        }
        Ok(result)
    }

    pub async fn list_range_bytes_async(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(Vec::new()),
        };

        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            return Ok(Vec::new());
        };

        Ok(self
            .list_range_raw_values_async(key, meta.version, storage_start, storage_end)
            .await)
    }

    pub async fn list_range_visit_bytes_async<F>(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        visitor: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(&[u8]) -> bool + Send,
    {
        let meta = match self.list_meta_async(key).await? {
            Some(meta) => meta,
            None => return Ok(0),
        };

        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            return Ok(0);
        };

        Ok(self
            .list_range_raw_values_visit_async(
                key,
                meta.version,
                storage_start,
                storage_end,
                visitor,
            )
            .await)
    }

    pub fn list_positions(
        &self,
        key: &str,
        element: &str,
        rank: i64,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<Vec<usize>, Error> {
        let items = self.list_range(key, 0, -1)?;
        if items.is_empty() || maxlen == Some(0) {
            return Ok(Vec::new());
        }

        let limit = maxlen.unwrap_or(usize::MAX);
        let mut matches = Vec::new();
        if rank > 0 {
            let mut seen = 0i64;
            for (idx, value) in items.iter().enumerate().take(limit) {
                if value == element {
                    seen += 1;
                    if seen >= rank {
                        matches.push(idx);
                        if matches_lpos_count(count, matches.len()) {
                            break;
                        }
                    }
                }
            }
        } else {
            let mut seen = 0i64;
            let len = items.len();
            for (scanned, idx) in (0..len).rev().enumerate() {
                if scanned >= limit {
                    break;
                }
                if items[idx] == element {
                    seen -= 1;
                    if seen <= rank {
                        matches.push(idx);
                        if matches_lpos_count(count, matches.len()) {
                            break;
                        }
                    }
                }
            }
        }

        Ok(matches)
    }

    pub async fn list_positions_async(
        &self,
        key: &str,
        element: &str,
        rank: i64,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<Vec<usize>, Error> {
        let items = self.list_range_async(key, 0, -1).await?;
        if items.is_empty() || maxlen == Some(0) {
            return Ok(Vec::new());
        }

        let limit = maxlen.unwrap_or(usize::MAX);
        let mut matches = Vec::new();
        if rank > 0 {
            let mut seen = 0i64;
            for (idx, value) in items.iter().enumerate().take(limit) {
                if value == element {
                    seen += 1;
                    if seen >= rank {
                        matches.push(idx);
                        if matches_lpos_count(count, matches.len()) {
                            break;
                        }
                    }
                }
            }
        } else {
            let mut seen = 0i64;
            let len = items.len();
            for (scanned, idx) in (0..len).rev().enumerate() {
                if scanned >= limit {
                    break;
                }
                if items[idx] == element {
                    seen -= 1;
                    if seen <= rank {
                        matches.push(idx);
                        if matches_lpos_count(count, matches.len()) {
                            break;
                        }
                    }
                }
            }
        }

        Ok(matches)
    }

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

    /// 设置指定下标的元素。
    pub fn list_set(&self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        let meta = self
            .list_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        let storage_index = self
            .resolve_list_index(meta, index)
            .ok_or_else(|| Error::msg("ERR index out of range"))?;

        let mut batch = WriteBatch::new();
        batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        );
        self.write_batch_if_not_empty(&batch);
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn list_set_async(&self, key: &str, index: i64, value: &str) -> Result<(), Error> {
        let meta = self
            .list_meta(key)?
            .ok_or_else(|| Error::msg("ERR no such key"))?;
        let storage_index = self
            .resolve_list_index(meta, index)
            .ok_or_else(|| Error::msg("ERR index out of range"))?;

        let mut batch = WriteBatch::new();
        batch.put(
            &list_item_key(self.db_index, key, meta.version, storage_index),
            value.as_bytes(),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// 保留指定范围，其余元素删除。
    pub fn list_trim(&self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let mut batch = WriteBatch::new();
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            for storage_index in meta.head..meta.tail {
                batch.delete(&list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    storage_index,
                ));
            }
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty(&batch);
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        for storage_index in meta.head..storage_start {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for storage_index in (storage_end + 1)..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        );
        self.write_batch_if_not_empty(&batch);
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn list_trim_async(&self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(()),
        };

        let mut batch = WriteBatch::new();
        let Some((storage_start, storage_end)) = self.resolve_list_range(meta, start, stop) else {
            for storage_index in meta.head..meta.tail {
                batch.delete(&list_item_key(
                    self.db_index,
                    key,
                    meta.version,
                    storage_index,
                ));
            }
            batch.delete(&self.mk(key));
            self.write_batch_if_not_empty_async(&batch).await;
            if batch.count() > 0 {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        };

        for storage_index in meta.head..storage_start {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        for storage_index in (storage_end + 1)..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        batch.put(
            &self.mk(key),
            &encode_list_meta(meta.expire_ms, meta.version, storage_start, storage_end + 1),
        );
        self.write_batch_if_not_empty_async(&batch).await;
        if batch.count() > 0 {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn list_remove(&self, key: &str, count: i64, element: &str) -> Result<usize, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self.list_range(key, 0, -1)?;
        let mut removed = 0usize;
        let mut keep = Vec::with_capacity(items.len());
        if count >= 0 {
            let limit = if count == 0 {
                usize::MAX
            } else {
                count as usize
            };
            for item in items {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    keep.push(item);
                }
            }
        } else {
            let limit = count.unsigned_abs() as usize;
            let mut rev_keep = Vec::with_capacity(items.len());
            for item in items.into_iter().rev() {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    rev_keep.push(item);
                }
            }
            keep = rev_keep.into_iter().rev().collect();
        }
        if removed == 0 {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        if keep.is_empty() {
            batch.delete(&self.mk(key));
        } else {
            for (index, value) in keep.iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, index as i64),
                    value.as_bytes(),
                );
            }
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, 0, keep.len() as i64),
            );
        }
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

    pub async fn list_remove_async(
        &self,
        key: &str,
        count: i64,
        element: &str,
    ) -> Result<usize, Error> {
        let meta = match self.list_meta(key)? {
            Some(meta) => meta,
            None => return Ok(0),
        };
        let items = self.list_range_async(key, 0, -1).await?;
        let mut removed = 0usize;
        let mut keep = Vec::with_capacity(items.len());
        if count >= 0 {
            let limit = if count == 0 {
                usize::MAX
            } else {
                count as usize
            };
            for item in items {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    keep.push(item);
                }
            }
        } else {
            let limit = count.unsigned_abs() as usize;
            let mut rev_keep = Vec::with_capacity(items.len());
            for item in items.into_iter().rev() {
                if item == element && removed < limit {
                    removed += 1;
                } else {
                    rev_keep.push(item);
                }
            }
            keep = rev_keep.into_iter().rev().collect();
        }
        if removed == 0 {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        for storage_index in meta.head..meta.tail {
            batch.delete(&list_item_key(
                self.db_index,
                key,
                meta.version,
                storage_index,
            ));
        }
        if keep.is_empty() {
            batch.delete(&self.mk(key));
        } else {
            for (index, value) in keep.iter().enumerate() {
                batch.put(
                    &list_item_key(self.db_index, key, meta.version, index as i64),
                    value.as_bytes(),
                );
            }
            batch.put(
                &self.mk(key),
                &encode_list_meta(meta.expire_ms, meta.version, 0, keep.len() as i64),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

}
