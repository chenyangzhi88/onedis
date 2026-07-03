use super::*;

impl Db {
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
}
