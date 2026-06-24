fn collect_iterator(iter: &mut dyn DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.seek_to_first()
        .expect("failed to seek kv_engine iterator");
    let mut entries = Vec::new();
    iter.scan_ref(&mut |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        true
    })
    .expect("failed to advance kv_engine iterator");
    entries
}

fn collect_iterator_limited(iter: &mut dyn DbIterator, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.seek_to_first()
        .expect("failed to seek kv_engine iterator");
    let mut entries = Vec::new();
    iter.scan_ref(&mut |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        entries.len() < limit
    })
    .expect("failed to advance kv_engine iterator");
    entries
}

async fn collect_iterator_async(
    iter: &mut dyn DbIterator,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    if limit == 0 {
        return entries;
    }
    iter.seek_to_first()
        .expect("failed to seek kv_engine async iterator");
    while let Some((key, value)) = iter
        .next_async()
        .await
        .expect("failed to advance kv_engine async iterator")
    {
        entries.push((key.to_vec(), value.to_vec()));
        if entries.len() >= limit {
            break;
        }
    }
    entries
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
