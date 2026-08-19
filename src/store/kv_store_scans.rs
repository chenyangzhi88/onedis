impl KvStore {
    pub fn scan_prefix_raw(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_range_raw_limited(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
    }

    pub async fn scan_prefix_raw_async(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_range_raw_limited_async(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
            .await
    }

    /// Count visible keys in a bounded range without materializing their values.
    pub fn count_range_raw_keys(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
    ) -> usize {
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
                crate::store::health::storage_health()
                    .record_failure("create key count cursor", error);
                global_metrics().record_storage_read(elapsed_us(storage_started));
                return 0;
            }
        };
        let mut count = 0usize;
        loop {
            match cursor.next_batch() {
                Ok(Some(batch)) => count = count.saturating_add(batch.len()),
                Ok(None) => break,
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("advance key count cursor", error);
                    break;
                }
            }
        }
        global_metrics().record_storage_read(elapsed_us(storage_started));
        count
    }

    pub async fn count_range_raw_keys_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
    ) -> usize {
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.count_range_raw_keys(&lower_bound, upper_bound)
        })
        .await
        {
            Ok(count) => count,
            Err(error) => {
                crate::store::health::storage_health()
                    .record_failure("key count worker", error);
                0
            }
        }
    }

    /// Scan a bounded raw range and stop after `limit` entries.
    pub fn scan_range_raw_limited(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let storage_started = Instant::now();
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let upper_len = upper_bound.as_ref().map(Vec::len).unwrap_or_default();
        let query = scan_request(Some(lower_bound.to_vec()), upper_bound, limit);
        let entries = if self.txn.is_some() {
            let new_cursor_started_at = trace_id.map(|_| Instant::now());
            let cursor = match self.with_transaction_mut(|txn| txn.scan(query)) {
                Ok(Some(Ok(cursor))) => Some(cursor),
                Ok(Some(Err(error))) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction scan", error);
                    None
                }
                Ok(None) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction scan", "missing transaction");
                    None
                }
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("access transaction for scan", error);
                    None
                }
            };
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = cursor.map_or_else(
                || {
                    crate::store::health::storage_health()
                        .record_failure("transaction scan", "failed to create cursor");
                    Vec::new()
                },
                |cursor| collect_scan_cursor(cursor, limit),
            );
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=true limit={} entries={} lower_len={} upper_len={} new_cursor_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.len(),
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
            let entries = match cursor {
                Ok(cursor) => collect_scan_cursor(cursor, limit),
                Err(error) => {
                    crate::store::health::storage_health().record_failure("scan", error);
                    Vec::new()
                }
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=false limit={} entries={} lower_len={} upper_len={} new_cursor_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.len(),
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
        entries
    }

    pub async fn scan_range_raw_limited_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let store = self.clone();
        let lower_bound = lower_bound.to_vec();
        match tokio::task::spawn_blocking(move || {
            store.scan_range_raw_limited(&lower_bound, upper_bound, limit)
        })
        .await
        {
            Ok(entries) => entries,
            Err(error) => {
                crate::store::health::storage_health().record_failure("scan worker", error);
                Vec::new()
            }
        }
    }

    /// Scan a bounded raw range in descending key order and stop after `limit` entries.
    pub fn scan_range_raw_limited_reverse(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let storage_started = Instant::now();
        let mut query = scan_request(Some(lower_bound.to_vec()), upper_bound, limit);
        query.order = KeyOrder::Desc;
        let entries = if self.txn.is_some() {
            match self.with_transaction_mut(|txn| txn.scan(query)) {
                Ok(Some(Ok(cursor))) => collect_scan_cursor(cursor, limit),
                Ok(Some(Err(error))) => {
                    crate::store::health::storage_health()
                        .record_failure("reverse transaction scan", error);
                    Vec::new()
                }
                Ok(None) => {
                    crate::store::health::storage_health()
                        .record_failure("reverse transaction scan", "missing transaction");
                    Vec::new()
                }
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("access transaction for reverse scan", error);
                    Vec::new()
                }
            }
        } else {
            match self.table.scan(query) {
                Ok(cursor) => collect_scan_cursor(cursor, limit),
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("reverse scan", error);
                    Vec::new()
                }
            }
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        entries
    }

    pub async fn scan_range_raw_limited_reverse_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
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
                crate::store::health::storage_health()
                    .record_failure("reverse scan worker", error);
                Vec::new()
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
    ) -> Vec<Vec<u8>> {
        if ordinals.is_empty() {
            return Vec::new();
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
                Ok(Some(Ok(keys))) => keys,
                Ok(Some(Err(error))) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction ordinal scan", error);
                    Vec::new()
                }
                Ok(None) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction ordinal scan", "missing transaction");
                    Vec::new()
                }
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("access transaction for ordinal scan", error);
                    Vec::new()
                }
            }
        } else {
            match self
                .table
                .scan(query)
                .and_then(|cursor| cursor.select_keys_by_rank(&ranks))
            {
                Ok(keys) => keys,
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("ordinal scan", error);
                    Vec::new()
                }
            }
        };
        global_metrics().record_storage_read(elapsed_us(storage_started));
        selected
    }

    pub async fn scan_range_raw_keys_at_ordinals_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        ordinals: &[usize],
    ) -> Vec<Vec<u8>> {
        if ordinals.is_empty() {
            return Vec::new();
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
                crate::store::health::storage_health()
                    .record_failure("ordinal scan worker", error);
                Vec::new()
            }
        }
    }

    /// Resolve the inclusive lower bound for a zero-based visible-key offset.
    pub fn scan_range_raw_start_at_offset(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        offset: usize,
    ) -> Option<Vec<u8>> {
        if offset == 0 {
            return Some(lower_bound.to_vec());
        }
        self.scan_range_raw_keys_at_ordinals(lower_bound, upper_bound, &[offset])
            .into_iter()
            .next()
    }

    pub async fn scan_range_raw_start_at_offset_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        offset: usize,
    ) -> Option<Vec<u8>> {
        if offset == 0 {
            return Some(lower_bound.to_vec());
        }
        self.scan_range_raw_keys_at_ordinals_async(lower_bound, upper_bound, &[offset])
            .await
            .into_iter()
            .next()
    }

    pub fn scan_range_raw_visit<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        if limit == 0 {
            return 0;
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
                Ok(Some(Err(error))) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction visit scan", error);
                    0
                }
                Ok(None) => {
                    crate::store::health::storage_health()
                        .record_failure("transaction visit scan", "missing transaction");
                    0
                }
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("access transaction for visit scan", error);
                    0
                }
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_visit sample={} txn=true limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                    trace_id,
                    limit,
                    seen,
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
                Err(error) => {
                    crate::store::health::storage_health()
                        .record_failure("visit scan", error);
                    0
                }
            };
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_visit sample={} txn=false limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                    trace_id,
                    limit,
                    seen,
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
        seen
    }

    pub async fn scan_range_raw_visit_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> usize
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
    ) -> usize
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        if limit == 0 {
            return 0;
        }
        debug_assert!(batch_size > 0);
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut next_lower_bound = lower_bound.to_vec();
        while seen < limit {
            let batch_limit = limit.saturating_sub(seen).min(batch_size.max(1));
            let entries = self
                .scan_range_raw_limited_async(&next_lower_bound, upper_bound.clone(), batch_limit)
                .await;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            let mut last_key = None;
            for (key, value) in entries {
                last_key = Some(key.clone());
                seen += 1;
                if !visitor(&key, &value) {
                    return seen;
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
        seen
    }

    pub async fn scan_range_raw_visit_reverse_async<F>(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
        visitor: F,
    ) -> usize
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        const VISIT_BATCH_SIZE: usize = 1024;
        if limit == 0 {
            return 0;
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
                .await;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            let mut last_key = None;
            for (key, value) in entries {
                last_key = Some(key.clone());
                seen += 1;
                if !visitor(&key, &value) {
                    return seen;
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
        seen
    }

    /// 范围删除 [start, end)，用于批量清理 sub-keys。
    pub fn delete_range(&self, start: &[u8], end: &[u8]) {
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
            if let Err(error) = result {
                crate::store::health::storage_health()
                    .record_failure("transaction delete range", error);
            }
            global_metrics().record_storage_write(elapsed_us(started), failed);
            return;
        }
        let result = self
            .table
            .delete_range(start, end, self.write_options.clone());
        let failed = result.is_err();
        if let Err(error) = result {
            crate::store::health::storage_health().record_failure("delete range", error);
        }
        global_metrics().record_storage_write(elapsed_us(started), failed);
    }
}
