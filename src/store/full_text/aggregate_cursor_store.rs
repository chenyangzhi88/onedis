struct FullTextAggregateCursor {
    db_index: u16,
    index: String,
    rows: VecDeque<FullTextAggregateRow>,
    last_access: Instant,
    max_idle: Duration,
    estimated_bytes: usize,
}

static FULLTEXT_AGGREGATE_CURSOR_ID: AtomicU64 = AtomicU64::new(1);
static FULLTEXT_AGGREGATE_CURSORS: OnceLock<Mutex<HashMap<u64, FullTextAggregateCursor>>> =
    OnceLock::new();

fn fulltext_aggregate_cursors() -> &'static Mutex<HashMap<u64, FullTextAggregateCursor>> {
    FULLTEXT_AGGREGATE_CURSORS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_fulltext_aggregate_cursor(
    db_index: u16,
    index: &str,
    rows: Vec<FullTextAggregateRow>,
    max_idle_ms: u64,
    memory_budget_bytes: usize,
) -> Result<u64, Error> {
    let now = Instant::now();
    let estimated_bytes = index
        .len()
        .saturating_add(rows.iter().map(estimate_fulltext_aggregate_row_bytes).sum());
    let id = FULLTEXT_AGGREGATE_CURSOR_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let cursor = FullTextAggregateCursor {
        db_index,
        index: index.to_string(),
        rows: rows.into(),
        last_access: now,
        max_idle: Duration::from_millis(max_idle_ms),
        estimated_bytes,
    };
    let mut cursors = fulltext_aggregate_cursors()
        .lock()
        .map_err(|_| Error::msg("ERR fulltext cursor lock poisoned"))?;
    remove_expired_fulltext_aggregate_cursors(&mut cursors, now);
    let used = cursors
        .values()
        .filter(|cursor| cursor.db_index == db_index)
        .fold(0usize, |used, cursor| {
            used.saturating_add(cursor.estimated_bytes)
        });
    if estimated_bytes > memory_budget_bytes.saturating_sub(used) {
        return Err(Error::msg(
            "ERR aggregate cursor memory limit exceeded",
        ));
    }
    cursors.insert(id, cursor);
    Ok(id)
}

fn read_fulltext_aggregate_cursor(
    db_index: u16,
    index: &str,
    cursor_id: u64,
    count: usize,
) -> Result<(Vec<FullTextAggregateRow>, usize), Error> {
    let mut cursors = fulltext_aggregate_cursors()
        .lock()
        .map_err(|_| Error::msg("ERR fulltext cursor lock poisoned"))?;
    let now = Instant::now();
    remove_expired_fulltext_aggregate_cursors(&mut cursors, now);
    let cursor = cursors
        .get_mut(&cursor_id)
        .ok_or_else(|| Error::msg("ERR cursor does not exist"))?;
    if cursor.db_index != db_index || cursor.index != index {
        return Err(Error::msg("ERR cursor does not exist"));
    }
    cursor.last_access = now;
    let mut rows = Vec::new();
    for _ in 0..count {
        let Some(row) = cursor.rows.pop_front() else {
            break;
        };
        rows.push(row);
    }
    let remaining = cursor.rows.len();
    if remaining == 0 {
        cursors.remove(&cursor_id);
    }
    Ok((rows, remaining))
}

fn delete_fulltext_aggregate_cursor(
    db_index: u16,
    index: &str,
    cursor_id: u64,
) -> Result<(), Error> {
    let mut cursors = fulltext_aggregate_cursors()
        .lock()
        .map_err(|_| Error::msg("ERR fulltext cursor lock poisoned"))?;
    remove_expired_fulltext_aggregate_cursors(&mut cursors, Instant::now());
    let Some(cursor) = cursors.get(&cursor_id) else {
        return Err(Error::msg("ERR cursor does not exist"));
    };
    if cursor.db_index != db_index || cursor.index != index {
        return Err(Error::msg("ERR cursor does not exist"));
    }
    cursors.remove(&cursor_id);
    Ok(())
}

fn delete_fulltext_aggregate_cursors_for_index(
    db_index: u16,
    index: &str,
) -> Result<(), Error> {
    let mut cursors = fulltext_aggregate_cursors()
        .lock()
        .map_err(|_| Error::msg("ERR fulltext cursor lock poisoned"))?;
    cursors.retain(|_, cursor| cursor.db_index != db_index || cursor.index != index);
    Ok(())
}

fn delete_fulltext_aggregate_cursors_for_db(db_index: u16) -> Result<(), Error> {
    let mut cursors = fulltext_aggregate_cursors()
        .lock()
        .map_err(|_| Error::msg("ERR fulltext cursor lock poisoned"))?;
    cursors.retain(|_, cursor| cursor.db_index != db_index);
    Ok(())
}

fn remove_expired_fulltext_aggregate_cursors(
    cursors: &mut HashMap<u64, FullTextAggregateCursor>,
    now: Instant,
) {
    cursors.retain(|_, cursor| now.duration_since(cursor.last_access) < cursor.max_idle);
}

fn estimate_fulltext_aggregate_row_bytes(row: &FullTextAggregateRow) -> usize {
    let values_bytes = row.values.iter().fold(0usize, |size, (key, value)| {
        size.saturating_add(key.len())
            .saturating_add(estimate_fulltext_aggregate_value_bytes(value))
    });
    let output_bytes = row.output.iter().fold(0usize, |size, (key, value)| {
        size.saturating_add(key.len())
            .saturating_add(estimate_fulltext_aggregate_value_bytes(value))
    });
    std::mem::size_of::<FullTextAggregateRow>()
        .saturating_add(values_bytes)
        .saturating_add(output_bytes)
}

fn estimate_fulltext_aggregate_value_bytes(value: &FullTextAggregateValue) -> usize {
    match value {
        FullTextAggregateValue::Null | FullTextAggregateValue::Number(_) => {
            std::mem::size_of::<FullTextAggregateValue>()
        }
        FullTextAggregateValue::String(value) => std::mem::size_of::<FullTextAggregateValue>()
            .saturating_add(value.len()),
        FullTextAggregateValue::List(values) => values.iter().fold(
            std::mem::size_of::<FullTextAggregateValue>(),
            |size, value| size.saturating_add(estimate_fulltext_aggregate_value_bytes(value)),
        ),
    }
}
