impl KvStore {
    pub fn scan_prefix_raw(&self, prefix: &[u8]) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_raw_limited(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
    }

    pub async fn scan_prefix_raw_async(
        &self,
        prefix: &[u8],
    ) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_raw_limited_async(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
            .await
    }

    /// Count visible keys in a bounded range without materializing their values.
    pub fn count_range_raw_keys(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
    ) -> KvResult<usize> {
        let storage_started = Instant::now();
        let query = scan_request_with_projection(
            Some(lower_bound.to_vec()),
            upper_bound,
            None,
            KvProjection::KeyOnly,
        );
        let cursor = if self.txn.is_some() {
            self.transaction_access_or(
                self.with_transaction_mut(|txn| txn.scan(query)),
                || {
                    Err(Status::InvalidArgument(
                        "unable to access onedis transaction".to_string(),
                    ))
                },
                "access transaction for key count",
            )
            .unwrap_or_else(|| {
                Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                ))
            })
        } else {
            self.table.scan(query)
        };
        let mut cursor = match cursor {
            Ok(cursor) => cursor,
            Err(error) => {
                self.health
                    .record_failure("create key count cursor", &error);
                global_metrics().record_storage_read(elapsed_us(storage_started));
                return Err(error);
            }
        };
        let mut count = 0usize;
        loop {
            match cursor.next_batch() {
                Ok(Some(batch)) => count = count.saturating_add(batch.len()),
                Ok(None) => break,
                Err(error) => {
                    self.health
                        .record_failure("advance key count cursor", &error);
                    global_metrics().record_storage_read(elapsed_us(storage_started));
                    return Err(error);
                }
            }
        }
        global_metrics().record_storage_read(elapsed_us(storage_started));
        Ok(count)
    }

    pub async fn count_range_raw_keys_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
    ) -> KvResult<usize> {
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.count_range_raw_keys(&lower_bound, upper_bound)
        })
        .await
        {
            Ok(count) => count,
            Err(error) => {
                self.health
                    .record_internal_failure("key count worker", &error);
                Err(Status::InvalidArgument(format!("key count worker failed: {error}")))
            }
        }
    }

    /// Scan a bounded raw range and stop after `limit` entries.
    pub fn scan_range_raw_limited(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let storage_started = Instant::now();
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let upper_len = upper_bound.as_ref().map(Vec::len).unwrap_or_default();
        let query = scan_request(Some(lower_bound.to_vec()), upper_bound, limit);
        let entries = if self.txn.is_some() {
            let new_cursor_started_at = trace_id.map(|_| Instant::now());
            let cursor = self.with_transaction_mut(|txn| txn.scan(query));
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = match cursor {
                Ok(Some(cursor)) => cursor.and_then(|cursor| collect_scan_cursor(cursor, limit)),
                Ok(None) => Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                )),
                Err(error) => Err(error),
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=true limit={} entries={} lower_len={} upper_len={} new_cursor_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.as_ref().map_or(0, Vec::len),
                    lower_bound.len(),
                    upper_len,
                    new_cursor_us.unwrap_or_default(),
                    collect_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            entries
        } else {
            let new_cursor_started_at = trace_id.map(|_| Instant::now());
            let cursor = self.table.scan(query);
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = cursor.and_then(|cursor| collect_scan_cursor(cursor, limit));
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=false limit={} entries={} lower_len={} upper_len={} new_cursor_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.as_ref().map_or(0, Vec::len),
                    lower_bound.len(),
                    upper_len,
                    new_cursor_us.unwrap_or_default(),
                    collect_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            entries
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        if let Err(error) = &entries {
            self.health.record_failure("range scan", error);
        }
        entries
    }

    pub async fn scan_range_raw_limited_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.scan_range_raw_limited(&lower_bound, upper_bound, limit)
        })
        .await
        {
            Ok(entries) => entries,
            Err(error) => {
                self.health.record_internal_failure("scan worker", &error);
                Err(Status::InvalidArgument(format!("scan worker failed: {error}")))
            }
        }
    }

    /// Scan a bounded raw range in descending key order and stop after `limit` entries.
    pub fn scan_range_raw_limited_reverse(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let storage_started = Instant::now();
        let mut query = scan_request(Some(lower_bound.to_vec()), upper_bound, limit);
        query.order = KeyOrder::Desc;
        let entries = if self.txn.is_some() {
            match self.with_transaction_mut(|txn| txn.scan(query)) {
                Ok(Some(cursor)) => cursor.and_then(|cursor| collect_scan_cursor(cursor, limit)),
                Ok(None) => Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                )),
                Err(error) => Err(error),
            }
        } else {
            self.table
                .scan(query)
                .and_then(|cursor| collect_scan_cursor(cursor, limit))
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        if let Err(error) = &entries {
            self.health.record_failure("reverse range scan", error);
        }
        entries
    }

    pub async fn scan_range_raw_limited_reverse_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> KvResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.scan_range_raw_limited_reverse(&lower_bound, upper_bound, limit)
        })
        .await
        {
            Ok(entries) => entries,
            Err(error) => {
                self.health
                    .record_internal_failure("reverse scan worker", &error);
                Err(Status::InvalidArgument(format!(
                    "reverse scan worker failed: {error}"
                )))
            }
        }
    }

    /// Return the keys at the requested zero-based visible-key ranks from one bounded read view.
    ///
    /// `ordinals` must be strictly increasing. Exact clean scan units before a target are skipped
    /// by kv-engine without materializing their keys; dirty units retain normal MVCC merge
    /// semantics.
    pub fn scan_range_raw_keys_at_ordinals(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        ordinals: &[usize],
    ) -> KvResult<Vec<Vec<u8>>> {
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(ordinals.windows(2).all(|pair| pair[0] < pair[1]));

        let storage_started = Instant::now();
        let ranks = ordinals.iter().map(|&rank| rank as u64).collect::<Vec<_>>();
        let query = scan_request_with_projection(
            Some(lower_bound.to_vec()),
            upper_bound,
            None,
            KvProjection::KeyOnly,
        );
        let selected = if self.txn.is_some() {
            match self.with_transaction_mut(|txn| {
                txn.scan(query)
                    .and_then(|cursor| cursor.select_keys_by_rank(&ranks))
            }) {
                Ok(Some(keys)) => keys,
                Ok(None) => Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                )),
                Err(error) => Err(error),
            }
        } else {
            self.table
                .scan(query)
                .and_then(|cursor| cursor.select_keys_by_rank(&ranks))
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        if let Err(error) = &selected {
            self.health.record_failure("ordinal scan", error);
        }
        selected
    }

    pub async fn scan_range_raw_keys_at_ordinals_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        ordinals: &[usize],
    ) -> KvResult<Vec<Vec<u8>>> {
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        let ordinals = ordinals.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.scan_range_raw_keys_at_ordinals(&lower_bound, upper_bound, &ordinals)
        })
        .await
        {
            Ok(keys) => keys,
            Err(error) => {
                self.health
                    .record_internal_failure("ordinal scan worker", &error);
                Err(Status::InvalidArgument(format!(
                    "ordinal scan worker failed: {error}"
                )))
            }
        }
    }

    /// Resolve the inclusive lower bound for a zero-based visible-key offset.
    pub fn scan_range_raw_start_at_offset(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        offset: usize,
    ) -> KvResult<Option<Vec<u8>>> {
        if offset == 0 {
            return Ok(Some(lower_bound.to_vec()));
        }
        Ok(self
            .scan_range_raw_keys_at_ordinals(lower_bound, upper_bound, &[offset])?
            .into_iter()
            .next())
    }

    pub async fn scan_range_raw_start_at_offset_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        offset: usize,
    ) -> KvResult<Option<Vec<u8>>> {
        if offset == 0 {
            return Ok(Some(lower_bound.to_vec()));
        }
        Ok(self
            .scan_range_raw_keys_at_ordinals_async(lower_bound, upper_bound, &[offset])
            .await
            ?
            .into_iter()
            .next())
    }

    pub fn scan_range_raw_visit<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> KvResult<usize>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        if limit == 0 {
            return Ok(0);
        }
        let storage_started = Instant::now();
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let upper_len = upper_bound.as_ref().map(Vec::len).unwrap_or_default();
        let query = scan_request(Some(lower_bound.to_vec()), upper_bound, limit);
        let mut visitor = visitor;
        let seen = if self.txn.is_some() {
            let scan_started_at = trace_id.map(|_| Instant::now());
            let seen = match self.with_transaction_mut(|txn| txn.scan(query)) {
                Ok(Some(Ok(mut cursor))) => {
                    collect_scan_cursor_into(&mut cursor, limit, &mut visitor)
                }
                Ok(Some(Err(error))) => Err(error),
                Ok(None) => Err(Status::InvalidArgument(
                    "missing onedis transaction".to_string(),
                )),
                Err(error) => Err(error),
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_visit sample={} txn=true limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                    trace_id,
                    limit,
                    seen.as_ref().copied().unwrap_or_default(),
                    lower_bound.len(),
                    upper_len,
                    scan_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            seen
        } else {
            let scan_started_at = trace_id.map(|_| Instant::now());
            let seen = match self.table.scan(query) {
                Ok(mut cursor) => collect_scan_cursor_into(&mut cursor, limit, &mut visitor),
                Err(error) => Err(error),
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_visit sample={} txn=false limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                    trace_id,
                    limit,
                    seen.as_ref().copied().unwrap_or_default(),
                    lower_bound.len(),
                    upper_len,
                    scan_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            seen
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        if let Err(error) = &seen {
            self.health.record_failure("visit scan", error);
        }
        seen
    }

    pub async fn scan_range_raw_visit_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> KvResult<usize>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        const VISIT_BATCH_SIZE: usize = 1024;
        self.scan_range_raw_visit_chunked_async(
            lower_bound,
            upper_bound,
            limit,
            VISIT_BATCH_SIZE,
            visitor,
        )
        .await
    }

    pub async fn scan_range_raw_visit_chunked_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        batch_size: usize,
        visitor: F,
    ) -> KvResult<usize>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        if limit == 0 {
            return Ok(0);
        }
        debug_assert!(batch_size > 0);
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut next_lower_bound = lower_bound.to_vec();
        while seen < limit {
            let batch_limit = limit.saturating_sub(seen).min(batch_size.max(1));
            let entries = self
                .scan_range_raw_limited_async(&next_lower_bound, upper_bound.clone(), batch_limit)
                .await?;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            let mut last_key = None;
            for (key, value) in entries {
                last_key = Some(key.clone());
                seen += 1;
                if !visitor(&key, &value) {
                    return Ok(seen);
                }
            }
            if entry_count < batch_limit {
                break;
            }
            let Some(mut last_key) = last_key else {
                break;
            };
            last_key.push(0);
            if upper_bound
                .as_ref()
                .is_some_and(|upper| last_key.as_slice() >= upper.as_slice())
            {
                break;
            }
            next_lower_bound = last_key;
        }
        Ok(seen)
    }

    pub async fn scan_range_raw_visit_reverse_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> KvResult<usize>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        const VISIT_BATCH_SIZE: usize = 1024;
        if limit == 0 {
            return Ok(0);
        }
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut next_upper_bound = upper_bound;
        while seen < limit {
            let batch_limit = limit.saturating_sub(seen).min(VISIT_BATCH_SIZE);
            let entries = self
                .scan_range_raw_limited_reverse_async(
                    lower_bound,
                    next_upper_bound.clone(),
                    batch_limit,
                )
                .await?;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            let mut last_key = None;
            for (key, value) in entries {
                last_key = Some(key.clone());
                seen += 1;
                if !visitor(&key, &value) {
                    return Ok(seen);
                }
            }
            if entry_count < batch_limit {
                break;
            }
            let Some(last_key) = last_key else {
                break;
            };
            if last_key.as_slice() <= lower_bound {
                break;
            }
            next_upper_bound = Some(last_key);
        }
        Ok(seen)
    }

    /// 范围删除 [start, end)，用于批量清理 sub-keys。
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> KvResult<()> {
        let started = Instant::now();
        if let Some(result) = self.transaction_access_or(
            self.with_transaction_mut(|txn| txn.delete_range(start, end)),
            || {
                Err(Status::InvalidArgument(
                    "unable to access onedis transaction".to_string(),
                ))
            },
            "access transaction for delete range",
        ) {
            let failed = result.is_err();
            global_metrics().record_storage_write(elapsed_us(started), failed);
            if let Err(error) = &result {
                self.health
                    .record_failure("transaction delete range", error);
            }
            return result;
        }
        let result = self
            .table
            .delete_range(start, end, self.write_options.clone());
        let failed = result.is_err();
        if let Err(error) = &result {
            self.health.record_failure("delete range", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
        result
    }
}
