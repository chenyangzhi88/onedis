use std::sync::Arc;

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

fn command(args: &[&str]) -> Result<Command, anyhow::Error> {
    Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    ))
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    db.handle_command(command(args).expect("failed to parse command"))
        .expect("command failed")
}

fn bulk_text(frame: &Frame) -> String {
    let Frame::BulkString(value) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(value.clone()).unwrap()
}

fn array(frame: Frame) -> Vec<Frame> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items
}

fn total(frame: &Frame) -> Option<i64> {
    let Frame::Array(items) = frame else {
        return None;
    };
    let Some(Frame::Integer(total)) = items.first() else {
        return None;
    };
    Some(*total)
}

fn info_value<'a>(items: &'a [Frame], name: &str) -> Option<&'a Frame> {
    items.chunks(2).find_map(|chunk| {
        let [key, value] = chunk else {
            return None;
        };
        (bulk_text(key) == name).then_some(value)
    })
}

fn create_index(db: &Db) {
    assert!(matches!(
        apply(
            db,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "category",
                "TAG"
            ],
        ),
        Frame::Ok
    ));
}

#[test]
fn ft_refresh_timeout_cancels_and_later_resumes_backfill() {
    let (_dir, db) = make_db();
    assert_eq!(
        apply(
            &db,
            &["HSET", "doc:1", "title", "eventual", "category", "book"]
        )
        .to_string(),
        "2"
    );
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "REFRESH_TIMEOUT_MS", "0"]),
        Frame::Ok
    ));
    create_index(&db);

    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "eventual"])),
        Some(0)
    );
    let info = array(apply(&db, &["FT.INFO", "idx"]));
    assert!(matches!(
        info_value(&info, "state"),
        Some(Frame::BulkString(value)) if value == b"backfilling"
    ));

    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "REFRESH_TIMEOUT_MS", "500"]),
        Frame::Ok
    ));
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "eventual"])),
        Some(1)
    );
}

#[test]
fn ft_outbox_compaction_keeps_latest_mutation_per_key() {
    let (_dir, db) = make_db();
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "REFRESH_MAX_DOCS", "0"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "OUTBOX_COMPACT_THRESHOLD", "2"]),
        Frame::Ok
    ));
    create_index(&db);

    assert_eq!(
        apply(
            &db,
            &["HSET", "doc:1", "title", "first", "category", "book"]
        )
        .to_string(),
        "2"
    );
    assert_eq!(
        apply(&db, &["HSET", "doc:1", "title", "second"]).to_string(),
        "0"
    );
    assert_eq!(
        apply(&db, &["HSET", "doc:1", "title", "latest"]).to_string(),
        "0"
    );

    let info = array(apply(&db, &["FT.INFO", "idx"]));
    let Some(Frame::Integer(pending)) = info_value(&info, "pending_outbox") else {
        panic!("missing pending_outbox");
    };
    assert!(
        *pending <= 2,
        "outbox should be compacted below threshold, got {pending}"
    );

    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "REFRESH_MAX_DOCS", "10"]),
        Frame::Ok
    ));
    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "latest"])), Some(1));
    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "first"])), Some(0));
    let info = array(apply(&db, &["FT.INFO", "idx"]));
    assert!(info_value(&info, "refresh_max_docs").is_some());
    assert!(info_value(&info, "outbox_compact_threshold").is_some());
    assert!(info_value(&info, "memory_budget").is_some());
}

#[test]
fn ft_config_exposes_storage_and_memory_budgets() {
    let (_dir, db) = make_db();
    for (name, value) in [
        ("REFRESH_MAX_DOCS", "7"),
        ("REFRESH_MAX_BYTES", "2048"),
        ("REFRESH_INTERVAL_MS", "10"),
        ("OUTBOX_COMPACT_THRESHOLD", "4"),
        ("REPAIR_THROTTLE_MS", "25"),
        ("MEMORY_BUDGET_SORT_BYTES", "4096"),
        ("MEMORY_BUDGET_VECTOR_HEAP_BYTES", "8192"),
    ] {
        assert!(matches!(
            apply(&db, &["FT.CONFIG", "SET", name, value]),
            Frame::Ok
        ));
        let config = array(apply(&db, &["FT.CONFIG", "GET", name]));
        let first = array(config[0].clone());
        assert_eq!(bulk_text(&first[0]), name);
        assert_eq!(bulk_text(&first[1]), value);
    }
}
