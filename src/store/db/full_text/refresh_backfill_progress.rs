pub(super) struct BackfillProgress {
    pub(super) finished: bool,
    pub(super) cursor: Option<String>,
    pub(super) docs: usize,
}
