impl KvStore {
    pub fn scan_prefix_raw(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_range_raw_limited(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
    }

    pub async fn scan_prefix_raw_async(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_range_raw_limited_async(prefix, prefix_exclusive_upper_bound(prefix), usize::MAX)
            .await
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
            let cursor = self
                .with_transaction_mut(|txn| {
                    txn.scan(query)
                        .expect("failed to create kv_engine transaction scan cursor")
                })
                .expect("missing kv_engine transaction");
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_scan_cursor(cursor, limit);
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
            let cursor = self
                .table
                .scan(query)
                .expect("failed to create kv_engine scan cursor");
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_scan_cursor(cursor, limit);
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
        tokio::task::spawn_blocking(move || {
            store.scan_range_raw_limited(&lower_bound, upper_bound, limit)
        })
        .await
        .expect("kv_engine scan worker panicked")
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
            self.with_transaction_mut(|txn| {
                txn.scan(query)
                    .expect("failed to create kv_engine transaction key scan cursor")
                    .select_keys_by_rank(&ranks)
                    .expect("failed to select ranked keys from kv_engine transaction cursor")
            })
            .expect("missing kv_engine transaction")
        } else {
            self.table
                .scan(query)
                .expect("failed to create kv_engine key scan cursor")
                .select_keys_by_rank(&ranks)
                .expect("failed to select ranked keys from kv_engine cursor")
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
        tokio::task::spawn_blocking(move || {
            store.scan_range_raw_keys_at_ordinals(&lower_bound, upper_bound, &ordinals)
        })
        .await
        .expect("kv_engine ordinal key scan worker panicked")
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
            let cursor = self
                .with_transaction_mut(|txn| {
                    txn.scan(query)
                        .expect("failed to create kv_engine transaction scan cursor")
                })
                .expect("missing kv_engine transaction");
            let scan_started_at = trace_id.map(|_| Instant::now());
            let mut cursor = cursor;
            let seen = collect_scan_cursor_into(&mut cursor, limit, &mut visitor);
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
            let cursor = self
                .table
                .scan(query)
                .expect("failed to create kv_engine scan cursor");
            let scan_started_at = trace_id.map(|_| Instant::now());
            let mut cursor = cursor;
            let seen = collect_scan_cursor_into(&mut cursor, limit, &mut visitor);
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
        if limit == 0 {
            return 0;
        }
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut next_lower_bound = lower_bound.to_vec();
        while seen < limit {
            let batch_limit = limit.saturating_sub(seen).min(VISIT_BATCH_SIZE);
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

    /// 范围删除 [start, end)，用于批量清理 sub-keys。
    pub fn delete_range(&self, start: &[u8], end: &[u8]) {
        let started = Instant::now();
        if let Some(result) = self.with_transaction_mut(|txn| txn.delete_range(start, end)) {
            result.expect("failed to stage delete_range into kv_engine transaction");
            global_metrics().record_storage_write(elapsed_us(started), false);
            return;
        }
        self.table
            .delete_range(start, end, self.write_options.clone())
            .expect("failed to delete_range in kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }
}
