struct FullTextAggregateCursor {
    db_index: u16,
    index: String,
    rows: VecDeque<FullTextAggregateRow>,
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
) -> u64 {
    let id = FULLTEXT_AGGREGATE_CURSOR_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let cursor = FullTextAggregateCursor {
        db_index,
        index: index.to_string(),
        rows: rows.into(),
    };
    if let Ok(mut cursors) = fulltext_aggregate_cursors().lock() {
        cursors.insert(id, cursor);
    }
    id
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
    let cursor = cursors
        .get_mut(&cursor_id)
        .ok_or_else(|| Error::msg("ERR cursor does not exist"))?;
    if cursor.db_index != db_index || cursor.index != index {
        return Err(Error::msg("ERR cursor does not exist"));
    }
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
    let Some(cursor) = cursors.get(&cursor_id) else {
        return Err(Error::msg("ERR cursor does not exist"));
    };
    if cursor.db_index != db_index || cursor.index != index {
        return Err(Error::msg("ERR cursor does not exist"));
    }
    cursors.remove(&cursor_id);
    Ok(())
}

