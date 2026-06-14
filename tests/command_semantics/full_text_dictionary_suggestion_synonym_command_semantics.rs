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
    let db = open_db_at(&dir);
    (dir, db)
}

fn open_db_at(dir: &TempDir) -> Db {
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
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
    db.handle_command(command(args)).expect("command failed")
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

fn wait_total(db: &Db, args: &[&str], expected: i64) -> Frame {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = Frame::Null;
    while Instant::now() < deadline {
        last = apply(db, args);
        if total(&last) == Some(expected) {
            return last;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("expected total {expected}, got {}", last.to_string());
}

#[test]
fn ft_dict_persists_and_feeds_spellcheck() {
    let (dir, db) = make_db();
    assert_eq!(
        apply(&db, &["FT.DICTADD", "terms", "redis", "search"]).to_string(),
        "2"
    );
    assert_eq!(
        apply(&db, &["FT.DICTADD", "terms", "redis"]).to_string(),
        "0"
    );
    assert!(
        apply(&db, &["FT.DICTDUMP", "terms"])
            .to_string()
            .contains("redis")
    );

    let reopened = open_db_at(&dir);
    assert!(
        apply(&reopened, &["FT.DICTDUMP", "terms"])
            .to_string()
            .contains("search")
    );

    assert!(matches!(
        apply(
            &reopened,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "body",
                "TEXT"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(&reopened, &["HSET", "doc:1", "body", "redis search"]).to_string(),
        "1"
    );
    wait_total(&reopened, &["FT.SEARCH", "idx", "redis"], 1);

    let spell = apply(
        &reopened,
        &["FT.SPELLCHECK", "idx", "rediz", "TERMS", "INCLUDE", "terms"],
    );
    assert!(spell.to_string().contains("redis"));

    assert_eq!(
        apply(&reopened, &["FT.DICTDEL", "terms", "search"]).to_string(),
        "1"
    );
}

#[test]
fn ft_suggest_scores_payloads_fuzzy_and_delete_work() {
    let (_dir, db) = make_db();
    assert_eq!(
        apply(
            &db,
            &["FT.SUGADD", "ac", "redis", "10", "PAYLOAD", "database"],
        )
        .to_string(),
        "1"
    );
    assert_eq!(
        apply(&db, &["FT.SUGADD", "ac", "redbird", "3"]).to_string(),
        "1"
    );
    assert_eq!(apply(&db, &["FT.SUGLEN", "ac"]).to_string(), "2");
    let ranked = apply(
        &db,
        &[
            "FT.SUGGET",
            "ac",
            "re",
            "WITHSCORES",
            "WITHPAYLOADS",
            "MAX",
            "2",
        ],
    )
    .to_string();
    assert!(ranked.starts_with("redis 10 database"));
    assert!(ranked.contains("redbird 3"));

    let fuzzy = apply(&db, &["FT.SUGGET", "ac", "radis", "FUZZY"]).to_string();
    assert!(fuzzy.contains("redis"));
    assert_eq!(apply(&db, &["FT.SUGDEL", "ac", "redbird"]).to_string(), "1");
    assert_eq!(apply(&db, &["FT.SUGLEN", "ac"]).to_string(), "1");
}

#[test]
fn ft_syn_updates_future_queries_and_dumps_groups() {
    let (_dir, db) = make_db();
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
                "body",
                "TEXT"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(&db, &["HSET", "doc:1", "body", "quick fox"]).to_string(),
        "1"
    );
    wait_total(&db, &["FT.SEARCH", "idx", "quick"], 1);
    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "fast"])), Some(0));

    assert!(matches!(
        apply(&db, &["FT.SYNUPDATE", "idx", "g1", "quick", "fast"]),
        Frame::Ok
    ));
    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "fast"])), Some(1));
    let dump = apply(&db, &["FT.SYNDUMP", "idx"]).to_string();
    assert!(dump.contains("g1"));
    assert!(dump.contains("quick"));
    assert!(dump.contains("fast"));
}
