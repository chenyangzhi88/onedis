fn range_query(lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>, limit: usize) -> SchemalessRangeQuery {
    let batch_limit = limit.clamp(1, 8192);
    SchemalessRangeQuery {
        bounds: KeyRange::new(lower_bound, upper_bound),
        projection: RangeProjection::KeyValue,
        budget: ScanBudget {
            max_records_per_batch: batch_limit,
            ..ScanBudget::default()
        },
        ..SchemalessRangeQuery::default()
    }
}

fn collect_range_cursor(mut cursor: kv_engine::api::RangeCursor, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    collect_range_cursor_into(&mut cursor, limit, |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        true
    });
    entries
}

fn collect_txn_range_cursor(mut cursor: kv_engine::api::SchemalessTransactionCursor, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    collect_txn_range_cursor_into(&mut cursor, limit, |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        true
    });
    entries
}

fn collect_range_cursor_into<F>(cursor: &mut kv_engine::api::RangeCursor, limit: usize, mut visitor: F) -> usize
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    let mut seen = 0usize;
    loop {
        let batch = cursor
            .next_batch()
            .expect("failed to advance kv_engine range cursor");
        let exhausted = batch.exhausted;
        if !visit_range_batch(&batch, limit, &mut seen, &mut visitor) || exhausted {
            break;
        }
    }
    seen
}

fn collect_txn_range_cursor_into<F>(cursor: &mut kv_engine::api::SchemalessTransactionCursor, limit: usize, mut visitor: F) -> usize
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    let mut seen = 0usize;
    loop {
        let batch = cursor
            .next_batch()
            .expect("failed to advance kv_engine transaction range cursor");
        let exhausted = batch.exhausted;
        if !visit_range_batch(&batch, limit, &mut seen, &mut visitor) || exhausted {
            break;
        }
    }
    seen
}

fn visit_range_batch<F>(batch: &RangeBatch, limit: usize, seen: &mut usize, visitor: &mut F) -> bool
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    for record in &batch.records {
        if *seen >= limit {
            return false;
        }
        let Some(key) = record.key.as_ref() else {
            continue;
        };
        let Some(value) = record.value.as_ref() else {
            continue;
        };
        *seen += 1;
        if !visitor(key.as_ref(), value.as_ref()) {
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
