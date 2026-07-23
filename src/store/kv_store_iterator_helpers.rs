fn scan_request(
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    limit: usize,
) -> KvScanRequest {
    KvScanRequest {
        bounds: KeyRange::new(lower_bound, upper_bound),
        projection: KvProjection::KeyValue,
        limit: Some(limit as u64),
        ..KvScanRequest::default()
    }
}

fn collect_scan_cursor(mut cursor: KvScanCursor, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    collect_scan_cursor_into(&mut cursor, limit, |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        true
    });
    entries
}

fn collect_scan_cursor_into<F>(cursor: &mut KvScanCursor, limit: usize, mut visitor: F) -> usize
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    let mut seen = 0usize;
    while let Some(batch) = cursor
            .next_batch()
            .expect("failed to advance kv_engine scan cursor")
    {
        if !visit_scan_batch(&batch, limit, &mut seen, &mut visitor) {
            break;
        }
    }
    seen
}

fn visit_scan_batch<F>(batch: &KvBatch, limit: usize, seen: &mut usize, visitor: &mut F) -> bool
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    for index in 0..batch.len() {
        if *seen >= limit {
            return false;
        }
        let Some(key) = batch.key(index) else {
            continue;
        };
        let Some(value) = batch.value(index) else {
            continue;
        };
        *seen += 1;
        if !visitor(key, value) {
            return false;
        }
    }
    true
}

fn prefix_exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper_bound = prefix.to_vec();
    for idx in (0..upper_bound.len()).rev() {
        if upper_bound[idx] != u8::MAX {
            upper_bound[idx] += 1;
            upper_bound.truncate(idx + 1);
            return Some(upper_bound);
        }
    }
    None
}
