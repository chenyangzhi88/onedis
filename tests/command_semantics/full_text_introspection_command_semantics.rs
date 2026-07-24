use std::{
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

fn command(args: &[&str]) -> Result<Command, anyhow::Error> {
    Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    ))
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command(
        db,
        command(args).expect("failed to parse command"),
    )
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

fn wait_total(db: &Db, args: &[&str], expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = Frame::Null;
    while Instant::now() < deadline {
        last = apply(db, args);
        if total(&last) == Some(expected) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("expected total {expected}, got {}", last);
}

fn info_value<'a>(items: &'a [Frame], name: &str) -> Option<&'a Frame> {
    items.chunks(2).find_map(|chunk| {
        let [key, value] = chunk else {
            return None;
        };
        (bulk_text(key) == name).then_some(value)
    })
}

fn seed_db() -> (TempDir, Db) {
    let (dir, db) = make_db();
    assert!(matches!(
        apply(
            &db,
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
                "TAG",
                "price",
                "NUMERIC"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(
            &db,
            &[
                "HSET",
                "doc:1",
                "title",
                "quick fox",
                "category",
                "book,animal",
                "price",
                "10"
            ],
        )
        .to_string(),
        "3"
    );
    assert_eq!(
        apply(
            &db,
            &[
                "HSET", "doc:2", "title", "slow fox", "category", "book", "price", "20"
            ],
        )
        .to_string(),
        "3"
    );
    wait_total(&db, &["FT.SEARCH", "idx", "fox"], 2);
    (dir, db)
}

#[test]
fn ft_introspection_explain_and_explaincli_cover_ast_nodes() {
    let (_dir, db) = seed_db();

    let explain = bulk_text(&apply(
        &db,
        &[
            "FT.EXPLAIN",
            "idx",
            "(@title:fox|@category:{book}) @price:[0 15]",
        ],
    ));
    assert!(explain.contains("INTERSECT"));
    assert!(explain.contains("UNION"));
    assert!(explain.contains("FIELD title"));
    assert!(explain.contains("TAG @category"));
    assert!(explain.contains("NUMERIC @price"));

    let cli = array(apply(&db, &["FT.EXPLAINCLI", "idx", "fox -slow"]));
    let lines = cli.iter().map(bulk_text).collect::<Vec<_>>();
    assert!(lines.iter().any(|line| line.contains("INTERSECT")));
    assert!(lines.iter().any(|line| line.contains("NOT")));
}

#[test]
fn ft_introspection_profile_search_and_info_counters() {
    let (_dir, db) = seed_db();

    let profile = array(apply(
        &db,
        &["FT.PROFILE", "idx", "SEARCH", "QUERY", "fox", "NOCONTENT"],
    ));
    assert_eq!(total(&profile[0]), Some(2));
    let profile_text = profile[1].to_string();
    assert!(profile_text.contains("Total profile time"));
    assert!(profile_text.contains("Index lookup time"));
    assert!(profile_text.contains("Pipeline"));
    assert!(profile_text.contains("Search"));

    let info = array(apply(&db, &["FT.INFO", "idx"]));
    assert!(matches!(
        info_value(&info, "num_docs"),
        Some(Frame::Integer(2))
    ));
    assert!(matches!(
        info_value(&info, "num_fields"),
        Some(Frame::Integer(3))
    ));
    assert!(info_value(&info, "inverted_sz_mb").is_some());
    assert!(info_value(&info, "outbox_queue_length").is_some());
    assert!(info_value(&info, "runtime_loaded").is_some());
}

#[test]
fn ft_introspection_tagvals_and_config_runtime_default_dialect() {
    let (_dir, db) = seed_db();

    let tagvals = array(apply(&db, &["FT.TAGVALS", "idx", "category"]));
    let values = tagvals.iter().map(bulk_text).collect::<Vec<_>>();
    assert_eq!(values, vec!["animal".to_string(), "book".to_string()]);

    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "MAXSEARCHRESULTS", "500"]),
        Frame::Ok
    ));
    let config = array(apply(&db, &["FT.CONFIG", "GET", "MAXSEARCHRESULTS"]));
    let first = array(config[0].clone());
    assert_eq!(bulk_text(&first[0]), "MAXSEARCHRESULTS");
    assert_eq!(bulk_text(&first[1]), "500");

    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "DEFAULT_DIALECT", "1"]),
        Frame::Ok
    ));
    let config = array(apply(&db, &["FT.CONFIG", "GET", "DEFAULT_DIALECT"]));
    let first = array(config[0].clone());
    assert_eq!(bulk_text(&first[0]), "DEFAULT_DIALECT");
    assert_eq!(bulk_text(&first[1]), "1");
}
