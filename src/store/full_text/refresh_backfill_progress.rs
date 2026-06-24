struct BackfillProgress {
    finished: bool,
    cursor: Option<String>,
    docs: usize,
    bytes: usize,
}
