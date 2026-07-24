use super::*;

impl Db {
    /// 分页扫描 zset members，返回下一个游标和成员/分数。
    pub fn zset_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let mut state = ZsetScanState::new(cursor, count);
        self.zset_visit_entries(key, |member, score| {
            state.visit(member, score, matcher.as_ref())
        })?;
        state.finish()
    }

    pub async fn zset_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), Error> {
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let mut state = ZsetScanState::new(cursor, count);
        self.zset_visit_entries_async(key, |member, score| {
            state.visit(member, score, matcher.as_ref())
        })
        .await?;
        state.finish()
    }
}

struct ZsetScanState {
    cursor: u64,
    position: u64,
    count: usize,
    bytes: usize,
    entries: Vec<(String, f64)>,
    stopped: bool,
    error: Option<Error>,
}

impl ZsetScanState {
    fn new(cursor: u64, count: usize) -> Self {
        Self {
            cursor,
            position: 0,
            count,
            bytes: 32,
            entries: Vec::with_capacity(count.min(1024)),
            stopped: false,
            error: None,
        }
    }

    fn visit(&mut self, member: String, score: f64, matcher: Option<&pattern::Matcher>) -> bool {
        self.position = self.position.saturating_add(1);
        if self.position <= self.cursor || matcher.is_some_and(|matcher| !matcher.is_match(&member))
        {
            return true;
        }
        let cost = member
            .len()
            .saturating_add(score.to_string().len())
            .saturating_add(64);
        if self
            .bytes
            .checked_add(cost)
            .is_none_or(|bytes| bytes > crate::frame::MAX_FRAME_BYTES)
        {
            if self.entries.is_empty() {
                self.error = Some(Error::msg("ERR response exceeds configured limit"));
            } else {
                self.position = self.position.saturating_sub(1);
            }
            self.stopped = true;
            return false;
        }
        self.bytes += cost;
        self.entries.push((member, score));
        if self.entries.len() >= self.count {
            self.stopped = true;
            return false;
        }
        true
    }

    fn finish(self) -> Result<(u64, Vec<(String, f64)>), Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok((if self.stopped { self.position } else { 0 }, self.entries))
    }
}
