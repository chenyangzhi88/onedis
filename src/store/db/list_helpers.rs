fn matches_lpos_count(count: Option<usize>, matched: usize) -> bool {
    match count {
        Some(0) => false,
        Some(limit) => matched >= limit,
        None => true,
    }
}
