use super::*;

const SCAN_RESPONSE_BYTE_BUDGET: usize = crate::frame::MAX_FRAME_BYTES - 128;

struct KeyScanFilter<'a> {
    layout: KeyEncodingLayout,
    db_index: u16,
    matcher: Option<&'a pattern::Matcher>,
    type_filter: Option<&'a str>,
    now: u64,
}

struct KeyScanState {
    requested_cursor: u64,
    position: u64,
    count: usize,
    result_bytes: usize,
    keys: Vec<String>,
    stopped: bool,
    error: Option<Error>,
}

impl KeyScanState {
    fn new(cursor: u64, count: usize) -> Self {
        Self {
            requested_cursor: cursor,
            // The storage cursor is pre-positioned at the requested physical offset. Keeping the
            // logical position here lets `visit` include that first key and return Redis's next
            // cursor without rescanning the prefix.
            position: cursor,
            count,
            result_bytes: 0,
            keys: Vec::with_capacity(count.min(1024)),
            stopped: false,
            error: None,
        }
    }

    fn visit(&mut self, filter: &KeyScanFilter<'_>, raw_key: &[u8], raw_value: &[u8]) -> bool {
        let Some(key_bytes) =
            logical_main_key_from_raw_key(filter.layout, filter.db_index, raw_key)
        else {
            return true;
        };
        let Ok(key) = String::from_utf8(key_bytes) else {
            return true;
        };
        self.position = self.position.saturating_add(1);
        if self.position <= self.requested_cursor {
            return true;
        }

        let Some(header) = decode_meta_header(raw_value) else {
            return true;
        };
        if header.expire_ms > 0 && filter.now >= header.expire_ms {
            return true;
        }
        if filter
            .matcher
            .is_some_and(|matcher| !matcher.is_match(&key))
            || filter
                .type_filter
                .is_some_and(|expected| type_name_for_tag(header.type_tag) != expected)
        {
            return true;
        }

        let encoded_cost = key.len().saturating_add(32);
        if self
            .result_bytes
            .checked_add(encoded_cost)
            .is_none_or(|bytes| bytes > SCAN_RESPONSE_BYTE_BUDGET)
        {
            if self.keys.is_empty() {
                self.error = Some(Error::msg("ERR response exceeds configured limit"));
            } else {
                // This key was not returned. Keep the cursor immediately before it
                // so the next call can consider it again.
                self.position = self.position.saturating_sub(1);
            }
            self.stopped = true;
            return false;
        }
        self.result_bytes += encoded_cost;
        self.keys.push(key);
        if self.keys.len() >= self.count {
            self.stopped = true;
            return false;
        }
        true
    }

    fn finish(self) -> Result<(u64, Vec<String>), Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok((if self.stopped { self.position } else { 0 }, self.keys))
    }
}

fn type_name_for_tag(type_tag: u8) -> &'static str {
    match type_tag {
        TYPE_STRING => "string",
        TYPE_HASH => "hash",
        TYPE_SET => "set",
        TYPE_SORTED_SET => "zset",
        TYPE_LIST => "list",
        TYPE_STREAM => "stream",
        TYPE_VECTOR => "vector",
        TYPE_JSON => "json",
        _ => "none",
    }
}

impl Db {
    pub fn scan_keys_page(
        &self,
        cursor: u64,
        pattern_str: &str,
        count: usize,
        type_filter: Option<&str>,
    ) -> Result<(u64, Vec<String>), Error> {
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let filter = KeyScanFilter {
            layout: self.key_layout,
            db_index: self.db_index,
            matcher: matcher.as_ref(),
            type_filter,
            now: now_ms(),
        };
        let mut state = KeyScanState::new(cursor, count);
        let mut remaining_cursor = cursor;
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            let scan_lower = if remaining_cursor == 0 {
                lower
            } else {
                let Ok(offset) = usize::try_from(remaining_cursor) else {
                    continue;
                };
                match self
                    .store
                    .scan_range_raw_start_at_offset(&lower, upper.clone(), offset)
                {
                    Some(start) => {
                        remaining_cursor = 0;
                        start
                    }
                    None => {
                        remaining_cursor = remaining_cursor.saturating_sub(
                            self.store.count_range_raw_keys(&lower, upper.clone()) as u64,
                        );
                        continue;
                    }
                }
            };
            let mut next_lower = scan_lower;
            loop {
                let remaining = state.count.saturating_sub(state.keys.len());
                let batch_limit = if filter.matcher.is_none() && filter.type_filter.is_none() {
                    remaining.max(1)
                } else {
                    remaining.saturating_mul(4).clamp(64, 1024)
                };
                let entries =
                    self.store
                        .scan_range_raw_limited(&next_lower, upper.clone(), batch_limit);
                if entries.is_empty() {
                    break;
                }
                let entry_count = entries.len();
                let mut last_key = None;
                for (raw_key, raw_value) in entries {
                    last_key = Some(raw_key.clone());
                    if !state.visit(&filter, &raw_key, &raw_value) {
                        break;
                    }
                }
                if state.stopped || entry_count < batch_limit {
                    break;
                }
                let Some(mut last_key) = last_key else {
                    break;
                };
                last_key.push(0);
                if upper
                    .as_ref()
                    .is_some_and(|upper| last_key.as_slice() >= upper.as_slice())
                {
                    break;
                }
                next_lower = last_key;
            }
            if state.stopped {
                break;
            }
        }
        state.finish()
    }

    pub async fn scan_keys_page_async(
        &self,
        cursor: u64,
        pattern_str: &str,
        count: usize,
        type_filter: Option<&str>,
    ) -> Result<(u64, Vec<String>), Error> {
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let filter = KeyScanFilter {
            layout: self.key_layout,
            db_index: self.db_index,
            matcher: matcher.as_ref(),
            type_filter,
            now: now_ms(),
        };
        let mut state = KeyScanState::new(cursor, count);
        let mut remaining_cursor = cursor;
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            let scan_lower = if remaining_cursor == 0 {
                lower
            } else {
                let Ok(offset) = usize::try_from(remaining_cursor) else {
                    continue;
                };
                match self
                    .store
                    .scan_range_raw_start_at_offset_async(&lower, upper.clone(), offset)
                    .await
                {
                    Some(start) => {
                        remaining_cursor = 0;
                        start
                    }
                    None => {
                        remaining_cursor = remaining_cursor.saturating_sub(
                            self.store
                                .count_range_raw_keys_async(&lower, upper.clone())
                                .await as u64,
                        );
                        continue;
                    }
                }
            };
            let mut next_lower = scan_lower;
            loop {
                let remaining = state.count.saturating_sub(state.keys.len());
                let batch_limit = if filter.matcher.is_none() && filter.type_filter.is_none() {
                    remaining.max(1)
                } else {
                    remaining.saturating_mul(4).clamp(64, 1024)
                };
                let entries = self
                    .store
                    .scan_range_raw_limited_async(&next_lower, upper.clone(), batch_limit)
                    .await;
                if entries.is_empty() {
                    break;
                }
                let entry_count = entries.len();
                let mut last_key = None;
                for (raw_key, raw_value) in entries {
                    last_key = Some(raw_key.clone());
                    if !state.visit(&filter, &raw_key, &raw_value) {
                        break;
                    }
                }
                if state.stopped || entry_count < batch_limit {
                    break;
                }
                let Some(mut last_key) = last_key else {
                    break;
                };
                last_key.push(0);
                if upper
                    .as_ref()
                    .is_some_and(|upper| last_key.as_slice() >= upper.as_slice())
                {
                    break;
                }
                next_lower = last_key;
            }
            if state.stopped {
                break;
            }
        }
        state.finish()
    }

    pub fn keys(&self, pattern_str: &str) -> Vec<String> {
        let now = now_ms();
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        self.logical_keys()
            .into_iter()
            .filter(|key| {
                // skip expired keys lazily
                if let Some(raw) = self.store.get_raw(&self.mk(key)) {
                    let expire_ms = decode_expire_ms(&raw);
                    if expire_ms > 0 && now >= expire_ms {
                        return false;
                    }
                }
                matcher.as_ref().is_none_or(|matcher| matcher.is_match(key))
            })
            .collect()
    }

    pub async fn keys_async(&self, pattern_str: &str) -> Vec<String> {
        let now = now_ms();
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let keys = self.logical_keys_async().await;
        let mut result = Vec::new();
        for key in keys {
            if let Some(raw) = self.store.get_raw_async(&self.mk(&key)).await {
                let expire_ms = decode_expire_ms(&raw);
                if expire_ms > 0 && now >= expire_ms {
                    continue;
                }
            }
            if matcher
                .as_ref()
                .is_none_or(|matcher| matcher.is_match(&key))
            {
                result.push(key);
            }
        }
        result
    }

    /**
     * 随机返回一个键
     */
    pub fn random_key(&self) -> Option<String> {
        let now = now_ms();
        let mut seen = 0u64;
        let mut selected = None;
        let mut random = random_seed();
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            self.store
                .scan_range_raw_visit(&lower, upper, usize::MAX, |raw_key, raw_value| {
                    let Some(key) =
                        live_logical_key(self.key_layout, self.db_index, raw_key, raw_value, now)
                    else {
                        return true;
                    };
                    seen = seen.saturating_add(1);
                    random = splitmix64(random);
                    if random.is_multiple_of(seen) {
                        selected = Some(key);
                    }
                    true
                });
        }
        selected
    }

    pub async fn random_key_async(&self) -> Option<String> {
        let now = now_ms();
        let mut seen = 0u64;
        let mut selected = None;
        let mut random = random_seed();
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            self.store
                .scan_range_raw_visit_async(&lower, upper, usize::MAX, |raw_key, raw_value| {
                    let Some(key) =
                        live_logical_key(self.key_layout, self.db_index, raw_key, raw_value, now)
                    else {
                        return true;
                    };
                    seen = seen.saturating_add(1);
                    random = splitmix64(random);
                    if random.is_multiple_of(seen) {
                        selected = Some(key);
                    }
                    true
                })
                .await;
        }
        selected
    }

    /**
     * 获取键值对数量
     */
    pub fn len(&self) -> usize {
        let now = now_ms();
        let mut count = 0usize;
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            self.store
                .scan_range_raw_visit(&lower, upper, usize::MAX, |raw_key, raw_value| {
                    if live_logical_key(self.key_layout, self.db_index, raw_key, raw_value, now)
                        .is_some()
                    {
                        count = count.saturating_add(1);
                    }
                    true
                });
        }
        count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn len_async(&self) -> usize {
        let now = now_ms();
        let mut count = 0usize;
        for (lower, upper) in self.key_layout.logical_main_key_ranges(self.db_index) {
            self.store
                .scan_range_raw_visit_async(&lower, upper, usize::MAX, |raw_key, raw_value| {
                    if live_logical_key(self.key_layout, self.db_index, raw_key, raw_value, now)
                        .is_some()
                    {
                        count = count.saturating_add(1);
                    }
                    true
                })
                .await;
        }
        count
    }

    /**
     * 清空所有数据
     */
    pub fn clear(&self) {
        self.flushdb();
    }

    pub async fn clear_async(&self) {
        self.flushdb_async().await;
    }
}

fn live_logical_key(
    layout: KeyEncodingLayout,
    db_index: u16,
    raw_key: &[u8],
    raw_value: &[u8],
    now: u64,
) -> Option<String> {
    let key = String::from_utf8(logical_main_key_from_raw_key(layout, db_index, raw_key)?).ok()?;
    let header = decode_meta_header(raw_value)?;
    (header.expire_ms == 0 || now < header.expire_ms).then_some(key)
}

fn random_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    (nanos as u64) ^ ((nanos >> 64) as u64)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
