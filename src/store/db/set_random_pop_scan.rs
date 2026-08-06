use super::*;

fn random_index(upper: usize) -> usize {
    debug_assert!(upper > 0);
    let upper = upper as u64;
    let threshold = upper.wrapping_neg() % upper;
    loop {
        let value = random_u64();
        if value >= threshold {
            return (value % upper) as usize;
        }
    }
}

fn shuffle_prefix<T>(items: &mut [T], count: usize) {
    let target = count.min(items.len());
    for index in 0..target {
        let selected = index + random_index(items.len() - index);
        items.swap(index, selected);
    }
}

struct SetPopRandom {
    state: u64,
}

impl SetPopRandom {
    fn new() -> Self {
        Self {
            state: random_u64(),
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        let upper = upper as u64;
        let threshold = upper.wrapping_neg() % upper;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return (value % upper) as usize;
            }
        }
    }
}

fn sample_set_pop_ordinals(len: usize, count: usize) -> Vec<usize> {
    let count = count.min(len);
    if count == 0 {
        return Vec::new();
    }
    if count == len {
        return (0..len).collect();
    }

    // Floyd's algorithm samples `count` distinct positions without allocating `len` slots.
    let mut random = SetPopRandom::new();
    let mut selected = HashSet::with_capacity(count);
    for candidate in len - count..len {
        let ordinal = random.index(candidate + 1);
        if !selected.insert(ordinal) {
            selected.insert(candidate);
        }
    }
    let mut ordinals = selected.into_iter().collect::<Vec<_>>();
    ordinals.sort_unstable();
    ordinals
}

struct SelectedSetMember {
    raw_key: Vec<u8>,
    member: String,
}

fn decode_selected_set_members(
    prefix: &[u8],
    raw_keys: Vec<Vec<u8>>,
    expected_count: usize,
) -> Result<Vec<SelectedSetMember>, Error> {
    if raw_keys.len() != expected_count {
        return Err(Error::msg(
            "ERR set metadata length does not match visible member entries",
        ));
    }

    raw_keys
        .into_iter()
        .map(|raw_key| {
            let member = raw_key
                .strip_prefix(prefix)
                .ok_or_else(|| Error::msg("ERR invalid set member key found while popping"))?;
            let member = String::from_utf8(member.to_vec()).map_err(|_| {
                Error::msg("ERR invalid UTF-8 set member found while popping from set")
            })?;
            Ok(SelectedSetMember { raw_key, member })
        })
        .collect()
}

impl Db {
    fn select_set_members_for_pop(
        &self,
        key: &str,
        version: u64,
        len: usize,
        count: usize,
    ) -> Result<Vec<SelectedSetMember>, Error> {
        let prefix = set_member_prefix(self.db_index, key, version);
        let ordinals = sample_set_pop_ordinals(len, count);
        let raw_keys = self.store.scan_range_raw_keys_at_ordinals(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            &ordinals,
        );
        decode_selected_set_members(&prefix, raw_keys, ordinals.len())
    }

    async fn select_set_members_for_pop_async(
        &self,
        key: &str,
        version: u64,
        len: usize,
        count: usize,
    ) -> Result<Vec<SelectedSetMember>, Error> {
        let prefix = set_member_prefix(self.db_index, key, version);
        let ordinals = sample_set_pop_ordinals(len, count);
        let raw_keys = self
            .store
            .scan_range_raw_keys_at_ordinals_async(
                &prefix,
                prefix_exclusive_upper_bound(&prefix),
                &ordinals,
            )
            .await;
        decode_selected_set_members(&prefix, raw_keys, ordinals.len())
    }

    pub fn set_random_members(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<String>>, Error> {
        let mut members = self.set_members(key)?;
        if members.is_empty() {
            return Ok(None);
        }
        let Some(count) = count else {
            shuffle_prefix(&mut members, 1);
            members.truncate(1);
            return Ok(Some(members));
        };
        if count >= 0 {
            let target = (count as usize).min(members.len());
            shuffle_prefix(&mut members, target);
            members.truncate(target);
            return Ok(Some(members));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for _ in 0..requested {
            result.push(members[random_index(members.len())].clone());
        }
        Ok(Some(result))
    }

    pub async fn set_random_members_async(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Option<Vec<String>>, Error> {
        let mut members = self.set_members_async(key).await?;
        if members.is_empty() {
            return Ok(None);
        }
        let Some(count) = count else {
            shuffle_prefix(&mut members, 1);
            members.truncate(1);
            return Ok(Some(members));
        };
        if count >= 0 {
            let target = (count as usize).min(members.len());
            shuffle_prefix(&mut members, target);
            members.truncate(target);
            return Ok(Some(members));
        }

        let requested = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(requested);
        for _ in 0..requested {
            result.push(members[random_index(members.len())].clone());
        }
        Ok(Some(result))
    }

    /// 弹出 count 个成员。
    pub fn set_pop(&self, key: &str, count: usize) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let meta = self.set_meta(key)?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        let target_count = count.min(meta.len);
        if target_count == 0 {
            return Ok(Vec::new());
        }

        let popped = self.select_set_members_for_pop(key, meta.version, meta.len, target_count)?;

        let mut batch = WriteBatch::new();
        if !popped.is_empty() {
            let len = meta.len.saturating_sub(popped.len());
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                for selected in &popped {
                    batch.delete(&selected.raw_key)?;
                }
                batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                )?;
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(popped.into_iter().map(|selected| selected.member).collect())
    }

    pub async fn set_pop_async(&self, key: &str, count: usize) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _set_write_guard = self.set_write_lock(key).lock().await;
        let meta = self.set_meta_async(key).await?;
        let Some(meta) = meta else {
            return Ok(Vec::new());
        };
        let target_count = count.min(meta.len);
        if target_count == 0 {
            return Ok(Vec::new());
        }

        let popped = self
            .select_set_members_for_pop_async(key, meta.version, meta.len, target_count)
            .await?;

        let mut batch = WriteBatch::new();
        if !popped.is_empty() {
            let len = meta.len.saturating_sub(popped.len());
            if len == 0 {
                self.delete_main_key_with_ttl_to_batch(&mut batch, key, meta.expire_ms);
                delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_SET);
            } else {
                for selected in &popped {
                    batch.delete(&selected.raw_key)?;
                }
                batch.put(
                    &self.mk(key),
                    &encode_set_meta(meta.expire_ms, meta.version, len),
                )?;
            }
        }

        if batch.count() > 0 {
            self.write_batch_if_not_empty_async(&batch).await;
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(popped.into_iter().map(|selected| selected.member).collect())
    }

    /// 扫描 set members，返回下一个游标和成员。
    pub fn set_scan(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<String>), Error> {
        let Some(meta) = self.set_meta(key)? else {
            return Ok((0, Vec::new()));
        };
        let prefix = set_member_prefix(self.db_index, key, meta.version);
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let mut state = SetScanState::new(cursor, count);
        self.store.scan_range_raw_visit(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            usize::MAX,
            |member_key, _| {
                let Some(member) = member_key.strip_prefix(prefix.as_slice()) else {
                    return true;
                };
                state.visit(member, matcher.as_ref())
            },
        );
        state.finish()
    }

    pub async fn set_scan_async(
        &self,
        key: &str,
        cursor: u64,
        pattern_str: &str,
        count: usize,
    ) -> Result<(u64, Vec<String>), Error> {
        let Some(meta) = self.set_meta_async(key).await? else {
            return Ok((0, Vec::new()));
        };
        let prefix = set_member_prefix(self.db_index, key, meta.version);
        let matcher = (pattern_str != "*").then(|| pattern::Matcher::new(pattern_str));
        let mut state = SetScanState::new(cursor, count);
        self.store
            .scan_range_raw_visit_async(
                &prefix,
                prefix_exclusive_upper_bound(&prefix),
                usize::MAX,
                |member_key, _| {
                    let Some(member) = member_key.strip_prefix(prefix.as_slice()) else {
                        return true;
                    };
                    state.visit(member, matcher.as_ref())
                },
            )
            .await;
        state.finish()
    }
}

struct SetScanState {
    cursor: u64,
    position: u64,
    count: usize,
    bytes: usize,
    members: Vec<String>,
    stopped: bool,
    error: Option<Error>,
}

impl SetScanState {
    fn new(cursor: u64, count: usize) -> Self {
        Self {
            cursor,
            position: 0,
            count,
            bytes: 32,
            members: Vec::with_capacity(count.min(1024)),
            stopped: false,
            error: None,
        }
    }

    fn visit(&mut self, raw_member: &[u8], matcher: Option<&pattern::Matcher>) -> bool {
        self.position = self.position.saturating_add(1);
        if self.position <= self.cursor {
            return true;
        }
        let Ok(member) = std::str::from_utf8(raw_member) else {
            return true;
        };
        if matcher.is_some_and(|matcher| !matcher.is_match(member)) {
            return true;
        }
        let cost = member.len().saturating_add(32);
        if self
            .bytes
            .checked_add(cost)
            .is_none_or(|bytes| bytes > crate::frame::MAX_FRAME_BYTES)
        {
            if self.members.is_empty() {
                self.error = Some(Error::msg("ERR response exceeds configured limit"));
            } else {
                self.position = self.position.saturating_sub(1);
            }
            self.stopped = true;
            return false;
        }
        self.bytes += cost;
        self.members.push(member.to_string());
        if self.members.len() >= self.count {
            self.stopped = true;
            return false;
        }
        true
    }

    fn finish(self) -> Result<(u64, Vec<String>), Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok((if self.stopped { self.position } else { 0 }, self.members))
    }
}
