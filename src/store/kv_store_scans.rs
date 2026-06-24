impl KvStore {
    pub fn scan_prefix_raw(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to scan after transaction completion");
            let opts = IteratorOptions {
                lower_bound: Some(prefix.to_vec()),
                upper_bound: prefix_exclusive_upper_bound(prefix),
                ..IteratorOptions::default()
            };
            let mut iter = txn
                .new_iterator_with_opts(&opts)
                .expect("failed to create kv_engine transaction prefix iterator");
            return collect_iterator(&mut *iter);
        }
        let mut iter = self
            .table
            .scan_prefix(prefix)
            .expect("failed to create kv_engine prefix iterator");
        iter.seek_to_first()
            .expect("failed to seek kv_engine prefix iterator");

        let mut entries = Vec::new();
        while let Some((key, value)) = iter.next_ref() {
            entries.push((key.to_vec(), value.to_vec()));
        }
        entries
    }

    pub async fn scan_prefix_raw_async(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        if let Some(txn_cell) = &self.txn {
            let opts = IteratorOptions {
                lower_bound: Some(prefix.to_vec()),
                upper_bound: prefix_exclusive_upper_bound(prefix),
                ..IteratorOptions::default()
            };
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            let entries = match iter_result {
                Ok(mut iter) => collect_iterator_async(&mut *iter, usize::MAX).await,
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction prefix iterator: {err}");
                }
            };
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return entries;
        }
        let mut iter = self
            .table
            .scan_prefix_async(prefix)
            .await
            .expect("failed to create kv_engine async prefix iterator");
        iter.seek_to_first()
            .expect("failed to seek kv_engine async prefix iterator");
        collect_iterator_async(&mut *iter, usize::MAX).await
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
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to scan after transaction completion");
            let new_iter_started_at = trace_id.map(|_| Instant::now());
            let mut iter = txn
                .new_iterator_with_opts(&opts)
                .expect("failed to create kv_engine transaction range iterator");
            let new_iter_us = new_iter_started_at.map(|started| started.elapsed().as_micros());
            let collect_started_at = trace_id.map(|_| Instant::now());
            let entries = collect_iterator_limited(&mut *iter, limit);
            if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                eprintln!(
                    "lrange-trace kv_scan sample={} txn=true limit={} entries={} lower_len={} upper_len={} new_iter_us={} collect_us={} total_us={}",
                    trace_id,
                    limit,
                    entries.len(),
                    lower_bound.len(),
                    opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                    new_iter_us.unwrap_or_default(),
                    collect_started_at
                        .map(|started| started.elapsed().as_micros())
                        .unwrap_or_default(),
                    total_started_at.elapsed().as_micros(),
                );
            }
            return entries;
        }
        let new_iter_started_at = trace_id.map(|_| Instant::now());
        let mut iter = self
            .table
            .iterator(&opts)
            .expect("failed to create kv_engine range iterator");
        let new_iter_us = new_iter_started_at.map(|started| started.elapsed().as_micros());
        let collect_started_at = trace_id.map(|_| Instant::now());
        let entries = collect_iterator_limited(&mut *iter, limit);
        if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
            eprintln!(
                "lrange-trace kv_scan sample={} txn=false limit={} entries={} lower_len={} upper_len={} new_iter_us={} collect_us={} total_us={}",
                trace_id,
                limit,
                entries.len(),
                lower_bound.len(),
                opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                new_iter_us.unwrap_or_default(),
                collect_started_at
                    .map(|started| started.elapsed().as_micros())
                    .unwrap_or_default(),
                total_started_at.elapsed().as_micros(),
            );
        }
        entries
    }

    pub async fn scan_range_raw_limited_async(
        &self,
        lower_bound: &[u8],
        upper_bound: Option<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        if let Some(txn_cell) = &self.txn {
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            let entries = match iter_result {
                Ok(mut iter) => collect_iterator_async(&mut *iter, limit).await,
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction range iterator: {err}");
                }
            };
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return entries;
        }
        let mut iter = self
            .table
            .iterator(&opts)
            .expect("failed to create kv_engine range iterator");
        collect_iterator_limited(&mut *iter, limit)
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
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut limited_visitor = |key: &[u8], value: &[u8]| {
            if seen >= limit {
                return false;
            }
            seen += 1;
            visitor(key, value) && seen < limit
        };
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to scan after transaction completion");
            let mut iter = txn
                .new_iterator_with_opts(&opts)
                .expect("failed to create kv_engine transaction range iterator");
            iter.seek_to_first()
                .expect("failed to seek kv_engine transaction range iterator");
            iter.scan_ref(&mut limited_visitor)
                .expect("failed to advance kv_engine transaction range iterator");
            return seen;
        }
        let mut iter = self
            .table
            .iterator(&opts)
            .expect("failed to create kv_engine range iterator");
        iter.seek_to_first()
            .expect("failed to seek kv_engine range iterator");
        iter.scan_ref(&mut limited_visitor)
            .expect("failed to advance kv_engine range iterator");
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
        if limit == 0 {
            return 0;
        }
        let trace_id = trace_lrange_scan_sample();
        let total_started_at = trace_id.map(|_| Instant::now());
        let opts = IteratorOptions {
            lower_bound: Some(lower_bound.to_vec()),
            upper_bound,
            ..IteratorOptions::default()
        };
        let mut visitor = visitor;
        let mut seen = 0usize;
        let mut limited_visitor = |key: &[u8], value: &[u8]| {
            if seen >= limit {
                return false;
            }
            seen += 1;
            visitor(key, value) && seen < limit
        };
        if let Some(txn_cell) = &self.txn {
            let mut txn = {
                let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                guard
                    .take()
                    .expect("attempted to scan after transaction completion")
            };
            let iter_result = txn.new_iterator_with_opts_async(&opts).await;
            match iter_result {
                Ok(mut iter) => {
                    let scan_started_at = trace_id.map(|_| Instant::now());
                    iter.seek_to_first()
                        .expect("failed to seek kv_engine async transaction range iterator");
                    iter.scan_ref_async(&mut limited_visitor)
                        .await
                        .expect("failed to advance kv_engine async transaction range iterator");
                    if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
                        eprintln!(
                            "lrange-trace kv_visit sample={} txn=true limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                            trace_id,
                            limit,
                            seen,
                            lower_bound.len(),
                            opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                            scan_started_at
                                .map(|started| started.elapsed().as_micros())
                                .unwrap_or_default(),
                            total_started_at.elapsed().as_micros(),
                        );
                    }
                }
                Err(err) => {
                    let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
                    *guard = Some(txn);
                    panic!("failed to create kv_engine async transaction range iterator: {err}");
                }
            }
            let mut guard = txn_cell.lock().expect("transaction mutex poisoned");
            *guard = Some(txn);
            return seen;
        }
        let mut iter = self
            .table
            .iterator(&opts)
            .expect("failed to create kv_engine range iterator");
        let scan_started_at = trace_id.map(|_| Instant::now());
        iter.seek_to_first()
            .expect("failed to seek kv_engine range iterator");
        iter.scan_ref(&mut limited_visitor)
            .expect("failed to advance kv_engine range iterator");
        if let (Some(trace_id), Some(total_started_at)) = (trace_id, total_started_at) {
            eprintln!(
                "lrange-trace kv_visit sample={} txn=false limit={} entries={} lower_len={} upper_len={} scan_us={} total_us={}",
                trace_id,
                limit,
                seen,
                lower_bound.len(),
                opts.upper_bound.as_ref().map(Vec::len).unwrap_or_default(),
                scan_started_at
                    .map(|started| started.elapsed().as_micros())
                    .unwrap_or_default(),
                total_started_at.elapsed().as_micros(),
            );
        }
        seen
    }

    /// 范围删除 [start, end)，用于批量清理 sub-keys。
    pub fn delete_range(&self, start: &[u8], end: &[u8]) {
        if let Some(txn) = &self.txn {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            let txn = guard
                .as_mut()
                .expect("attempted to delete range after transaction completion");
            txn.delete_range(start, end)
                .expect("failed to stage delete_range into kv_engine transaction");
            return;
        }
        self.table
            .delete_range(start, end)
            .expect("failed to delete_range in kv_engine");
    }
}
