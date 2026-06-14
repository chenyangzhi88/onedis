use std::sync::Arc;
use std::time::{Duration, Instant};

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
    let vc = Arc::new(VersionCounter::new());
    let ttl = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, vc, ttl)
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

fn apply_owned(db: &Db, args: Vec<String>) -> Frame {
    let refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    apply(db, &refs)
}

fn search_ids(frame: &Frame) -> Vec<String> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items[1..]
        .chunks(2)
        .map(|chunk| {
            let Frame::BulkString(key) = &chunk[0] else {
                panic!("expected key");
            };
            String::from_utf8(key.clone()).unwrap()
        })
        .collect()
}

fn wait_for_search_ids(db: &Db, args: &[&str], expected: &[&str]) -> Frame {
    let deadline = Instant::now() + Duration::from_secs(3);
    let expected = expected
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    let mut last = Frame::Null;
    while Instant::now() < deadline {
        last = apply(db, args);
        if search_ids(&last) == expected {
            return last;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(search_ids(&last), expected);
    last
}

fn info_value<'a>(frame: &'a Frame, name: &str) -> Option<&'a Frame> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items.chunks(2).find_map(|chunk| {
        let [Frame::BulkString(key), value] = chunk else {
            return None;
        };
        (String::from_utf8_lossy(key) == name).then_some(value)
    })
}

#[test]
fn ft_create_backfills_hashes_and_searches_text() {
    let (_dir, db) = make_db();
    assert!(matches!(
        apply(
            &db,
            &[
                "HSET",
                "doc:1",
                "title",
                "redis search",
                "body",
                "fast full text",
                "category",
                "db"
            ]
        ),
        Frame::Integer(3)
    ));
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
                "body",
                "TEXT",
                "category",
                "TAG"
            ]
        ),
        Frame::Ok
    ));

    let result = wait_for_search_ids(
        &db,
        &["FT.SEARCH", "idx", "redis", "RETURN", "1", "title"],
        &["doc:1"],
    );
    let Frame::Array(items) = result else {
        unreachable!();
    };
    assert!(matches!(&items[0], Frame::Integer(1)));
}

#[test]
fn ft_search_tracks_hash_updates_and_deletes() {
    let (_dir, db) = make_db();
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
        ],
    );
    apply(&db, &["HSET", "doc:1", "title", "hello world"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "hello"], &["doc:1"]);

    apply(&db, &["HSET", "doc:1", "title", "other text"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "hello"], &[]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "other"], &["doc:1"]);

    apply(&db, &["HDEL", "doc:1", "title"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "other"], &[]);
}

#[test]
fn ft_search_rebuilds_doc_after_partial_hdel() {
    let (_dir, db) = make_db();
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
            "body",
            "TEXT",
        ],
    );
    apply(
        &db,
        &["HSET", "doc:1", "title", "remove me", "body", "keep me"],
    );
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "remove"], &["doc:1"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "keep"], &["doc:1"]);

    apply(&db, &["HDEL", "doc:1", "title"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "remove"], &[]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "keep"], &["doc:1"]);
}

#[test]
fn ft_search_replays_durable_outbox_after_reopen() {
    let (dir, db) = make_db();
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
        ],
    );
    apply(&db, &["HSET", "doc:1", "title", "queued replay"]);
    drop(db);

    let reopened = open_db_at(&dir);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "queued"], &["doc:1"]);
}

#[test]
fn ft_create_backfill_resumes_across_reopen() {
    let (dir, db) = make_db();
    for id in 0..1100 {
        let key = format!("doc:{id:04}");
        let value = if id == 1099 {
            "late needle".to_string()
        } else {
            "common text".to_string()
        };
        apply_owned(
            &db,
            vec!["HSET".to_string(), key, "title".to_string(), value],
        );
    }
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
        ],
    );
    let _ = apply(&db, &["FT.SEARCH", "idx", "needle"]);
    drop(db);

    let reopened = open_db_at(&dir);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "needle"], &["doc:1099"]);
}

#[test]
fn ft_search_rebuilds_runtime_after_reopen() {
    let (dir, db) = make_db();
    apply(&db, &["HSET", "doc:1", "title", "persistent search"]);
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
        ],
    );
    drop(db);

    let reopened = open_db_at(&dir);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "persistent"], &["doc:1"]);
}

#[test]
fn ft_search_uses_persisted_directory_and_updates_after_reopen() {
    let (dir, db) = make_db();
    apply(&db, &["HSET", "doc:1", "title", "first persisted"]);
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
        ],
    );
    drop(db);

    let reopened = open_db_at(&dir);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "first"], &["doc:1"]);
    apply(&reopened, &["HSET", "doc:2", "title", "second persisted"]);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "second"], &["doc:2"]);
}

#[test]
fn ft_search_removes_deleted_hash_after_refresh() {
    let (_dir, db) = make_db();
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
        ],
    );
    apply(&db, &["HSET", "doc:1", "title", "delete me"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "delete"], &["doc:1"]);
    apply(&db, &["DEL", "doc:1"]);
    let result = wait_for_search_ids(&db, &["FT.SEARCH", "idx", "delete"], &[]);
    let Frame::Array(items) = result else {
        unreachable!();
    };
    assert!(matches!(&items[0], Frame::Integer(0)));
}

#[test]
fn ft_search_removes_expired_hash_after_refresh() {
    let (_dir, db) = make_db();
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
        ],
    );
    apply(&db, &["HSET", "doc:1", "title", "short lived"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "short"], &["doc:1"]);
    apply(&db, &["PEXPIRE", "doc:1", "1"]);
    std::thread::sleep(Duration::from_millis(20));
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "short"], &[]);
}

#[test]
fn ft_info_exposes_index_state_and_pending_work() {
    let (_dir, db) = make_db();
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
        ],
    );
    let info = apply(&db, &["FT.INFO", "idx"]);
    assert!(matches!(
        info_value(&info, "state"),
        Some(Frame::BulkString(value)) if value == b"backfilling" || value == b"ready"
    ));
    assert!(matches!(
        info_value(&info, "pending_outbox"),
        Some(Frame::Integer(_))
    ));
}

#[test]
fn ft_search_after_flushdb_reports_missing_index() {
    let (_dir, db) = make_db();
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
        ],
    );
    apply(&db, &["FLUSHDB"]);
    let err = match db.handle_command(command(&["FT.SEARCH", "idx", "*"])) {
        Ok(_) => panic!("search should fail after flushdb removes index meta"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("fulltext index does not exist"));
}

#[test]
fn ft_search_supports_tag_numeric_and_text_and() {
    let (_dir, db) = make_db();
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
            "score",
            "NUMERIC",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:1",
            "title",
            "redis database",
            "category",
            "db",
            "score",
            "9.5",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:2",
            "title",
            "redis cache",
            "category",
            "cache",
            "score",
            "4.0",
        ],
    );

    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "@category:{db}"], &["doc:1"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "@score:[5 10]"], &["doc:1"]);
    wait_for_search_ids(
        &db,
        &["FT.SEARCH", "idx", "redis @category:{cache}"],
        &["doc:2"],
    );
}
