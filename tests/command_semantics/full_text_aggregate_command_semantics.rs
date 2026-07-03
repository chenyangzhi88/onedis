use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use onedis_server::{
    command::Command,
    frame::Frame,
    store::{
        db::Db,
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    },
};
use tempfile::TempDir;

fn make_db() -> (TempDir, Db) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    (dir, Db::new(0, store, version_counter, ttl_manager))
}

fn command(args: &[&str]) -> Command {
    Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    ))
    .expect("failed to parse command")
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command(db, command(args)).expect("command failed")
}

fn seed_products(db: &Db) {
    assert!(matches!(
        apply(
            db,
            &[
                "FT.CREATE",
                "products",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "product:",
                "SCHEMA",
                "title",
                "TEXT",
                "category",
                "TAG",
                "price",
                "NUMERIC",
                "SORTABLE"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(
            db,
            &[
                "HSET",
                "product:1",
                "title",
                "winter jacket",
                "category",
                "jacket",
                "price",
                "20"
            ],
        )
        .to_string(),
        "3"
    );
    assert_eq!(
        apply(
            db,
            &[
                "HSET",
                "product:2",
                "title",
                "rain jacket",
                "category",
                "jacket",
                "price",
                "20"
            ],
        )
        .to_string(),
        "3"
    );
    assert_eq!(
        apply(
            db,
            &[
                "HSET",
                "product:3",
                "title",
                "work pants",
                "category",
                "pants",
                "price",
                "25"
            ],
        )
        .to_string(),
        "3"
    );
    wait_for_aggregate_total(db, &["FT.AGGREGATE", "products", "*"], 3);
}

fn wait_for_aggregate_total(db: &Db, args: &[&str], expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = None;
    while Instant::now() < deadline {
        let frame = apply(db, args);
        last = Some(frame.clone());
        if aggregate_total(&frame) == Some(expected) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "aggregate total did not become {expected}: {:?}",
        last.map(|f| f.to_string())
    );
}

fn aggregate_total(frame: &Frame) -> Option<i64> {
    let Frame::Array(items) = frame else {
        return None;
    };
    let Some(Frame::Integer(total)) = items.first() else {
        return None;
    };
    Some(*total)
}

fn aggregate_rows(frame: Frame) -> (i64, Vec<BTreeMap<String, String>>) {
    let Frame::Array(items) = frame else {
        panic!("expected aggregate array");
    };
    let Some(Frame::Integer(total)) = items.first() else {
        panic!("expected aggregate total");
    };
    let total = *total;
    let mut rows = Vec::new();
    for item in items.into_iter().skip(1) {
        let Frame::Array(fields) = item else {
            panic!("expected aggregate row");
        };
        let mut row = BTreeMap::new();
        let mut iter = fields.into_iter();
        while let (Some(field), Some(value)) = (iter.next(), iter.next()) {
            row.insert(field.to_string(), value.to_string());
        }
        rows.push(row);
    }
    (total, rows)
}

#[test]
fn ft_aggregate_load_sort_and_limit() {
    let (_dir, db) = make_db();
    seed_products(&db);

    let (total, rows) = aggregate_rows(apply(
        &db,
        &[
            "FT.AGGREGATE",
            "products",
            "*",
            "LOAD",
            "3",
            "@title",
            "@category",
            "@price",
            "SORTBY",
            "2",
            "@price",
            "DESC",
            "LIMIT",
            "0",
            "2",
        ],
    ));

    assert_eq!(total, 3);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("title").map(String::as_str), Some("work pants"));
    assert_eq!(rows[0].get("price").map(String::as_str), Some("25"));
    assert_eq!(rows[1].get("price").map(String::as_str), Some("20"));
}

#[test]
fn ft_aggregate_groupby_reducers() {
    let (_dir, db) = make_db();
    seed_products(&db);

    let (total, rows) = aggregate_rows(apply(
        &db,
        &[
            "FT.AGGREGATE",
            "products",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "count",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
            "SORTBY",
            "2",
            "@total",
            "DESC",
        ],
    ));

    assert_eq!(total, 2);
    assert_eq!(rows[0].get("category").map(String::as_str), Some("jacket"));
    assert_eq!(rows[0].get("count").map(String::as_str), Some("2"));
    assert_eq!(rows[0].get("total").map(String::as_str), Some("40"));
    assert_eq!(rows[1].get("category").map(String::as_str), Some("pants"));
    assert_eq!(rows[1].get("total").map(String::as_str), Some("25"));
}

#[test]
fn ft_aggregate_apply_and_filter() {
    let (_dir, db) = make_db();
    seed_products(&db);

    let (total, rows) = aggregate_rows(apply(
        &db,
        &[
            "FT.AGGREGATE",
            "products",
            "*",
            "LOAD",
            "2",
            "@title",
            "@price",
            "APPLY",
            "@price * 2",
            "AS",
            "double",
            "FILTER",
            "@double >= 40",
            "SORTBY",
            "2",
            "@double",
            "ASC",
        ],
    ));

    assert_eq!(total, 3);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("double").map(String::as_str), Some("40"));
    assert_eq!(rows[2].get("double").map(String::as_str), Some("50"));
}

#[test]
fn ft_cursor_reads_and_deletes_aggregate_batches() {
    let (_dir, db) = make_db();
    seed_products(&db);

    let response = apply(
        &db,
        &[
            "FT.AGGREGATE",
            "products",
            "*",
            "LOAD",
            "1",
            "@title",
            "SORTBY",
            "2",
            "@title",
            "ASC",
            "WITHCURSOR",
            "COUNT",
            "1",
        ],
    );
    let Frame::Array(items) = response else {
        panic!("expected cursor response");
    };
    assert_eq!(items.len(), 2);
    let (total, first_batch) = aggregate_rows(items[0].clone());
    assert_eq!(total, 3);
    assert_eq!(first_batch.len(), 1);
    let Frame::Integer(cursor_id) = items[1] else {
        panic!("expected cursor id");
    };
    assert!(cursor_id > 0);

    let read = apply(
        &db,
        &[
            "FT.CURSOR",
            "READ",
            "products",
            &cursor_id.to_string(),
            "COUNT",
            "1",
        ],
    );
    let Frame::Array(items) = read else {
        panic!("expected cursor read response");
    };
    let (_remaining_total, second_batch) = aggregate_rows(items[0].clone());
    assert_eq!(second_batch.len(), 1);
    let Frame::Integer(next_cursor) = items[1] else {
        panic!("expected next cursor id");
    };
    assert_eq!(next_cursor, cursor_id);

    assert!(matches!(
        apply(
            &db,
            &["FT.CURSOR", "DEL", "products", &cursor_id.to_string()]
        ),
        Frame::Ok
    ));
}

#[test]
fn ft_profile_wraps_aggregate_result() {
    let (_dir, db) = make_db();
    seed_products(&db);

    let response = apply(
        &db,
        &[
            "FT.PROFILE",
            "products",
            "AGGREGATE",
            "QUERY",
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "count",
        ],
    );
    let Frame::Array(items) = response else {
        panic!("expected profile response");
    };
    assert_eq!(items.len(), 2);
    let (total, rows) = aggregate_rows(items[0].clone());
    assert_eq!(total, 2);
    assert!(rows.iter().any(|row| {
        row.get("category").map(String::as_str) == Some("jacket")
            && row.get("count").map(String::as_str) == Some("2")
    }));
    assert!(items[1].to_string().contains("Total profile time"));
}
