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
        let query = range_query(Some(lower_bound.to_vec()), upper_bound, limit);
        let entries = if self.txn.is_some() {
            let new_cursor_started_at = trace_id.map(|_| Instant::now());
            let cursor = self.with_transaction_mut(|txn| {
                txn.range_query(query)
                    .expect("failed to create kv_engine transaction range cursor")
            }).expect("missing kv_engine transaction");
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_txn_range_cursor(cursor, limit);
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
                .range_query(query)
                .expect("failed to create kv_engine range cursor");
            let new_cursor_us = new_cursor_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_range_cursor(cursor, limit);
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
        self.scan_range_raw_limited(lower_bound, upper_bound, limit)
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
        let query = range_query(Some(lower_bound.to_vec()), upper_bound, limit);
        let mut visitor = visitor;
        let seen = if self.txn.is_some() {
            let cursor = self.with_transaction_mut(|txn| {
                txn.range_query(query)
                    .expect("failed to create kv_engine transaction range cursor")
            }).expect("missing kv_engine transaction");
            let scan_started_at = trace_id.map(|_| Instant::now());
            let mut cursor = cursor;
            let seen = collect_txn_range_cursor_into(&mut cursor, limit, &mut visitor);
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
                .range_query(query)
                .expect("failed to create kv_engine range cursor");
            let scan_started_at = trace_id.map(|_| Instant::now());
            let mut cursor = cursor;
            let seen = collect_range_cursor_into(&mut cursor, limit, &mut visitor);
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
        self.scan_range_raw_visit(lower_bound, upper_bound, limit, visitor)
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
            .delete_range(start, end)
            .expect("failed to delete_range in kv_engine");
        global_metrics().record_storage_write(elapsed_us(started), false);
    }
}
