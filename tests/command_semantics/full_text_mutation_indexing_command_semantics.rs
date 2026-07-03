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

fn search_ids(frame: Frame) -> Vec<String> {
    let Frame::Array(items) = frame else {
        panic!("expected search array");
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

fn assert_search_ids(db: &Db, args: &[&str], expected: &[&str]) {
    let actual = search_ids(apply(db, args));
    let expected = expected
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn wait_until<F>(mut condition: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(condition());
}

#[test]
fn hash_writes_are_searchable_when_command_returns() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
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
        &["HSET", "doc:1", "title", "old term", "body", "stable"],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "old"], &["doc:1"]);

    apply(&db, &["HSET", "doc:1", "title", "new term"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "old"], &[]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "new"], &["doc:1"]);

    apply(&db, &["HMSET", "doc:1", "body", "hmset term"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "hmset"], &["doc:1"]);
}

#[test]
fn lua_hash_writes_publish_fulltext_before_eval_returns() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "lua:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    apply(
        &db,
        &[
            "EVAL",
            "return redis.call('HSET', KEYS[1], 'title', ARGV[1])",
            "1",
            "lua:1",
            "script visible",
        ],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "script"], &["lua:1"]);
}

#[test]
fn hash_delete_key_ttl_and_field_ttl_update_index_immediately() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
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

    apply(&db, &["HSET", "doc:1", "title", "delete me"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "delete"], &["doc:1"]);
    apply(&db, &["DEL", "doc:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "delete"], &[]);

    apply(&db, &["HSET", "doc:overwrite", "title", "overwrite me"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "overwrite"], &["doc:overwrite"]);
    apply(&db, &["SET", "doc:overwrite", "plain string"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "overwrite"], &[]);

    apply(&db, &["HSET", "doc:2", "title", "short lived"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "short"], &["doc:2"]);
    apply(&db, &["PEXPIRE", "doc:2", "1"]);
    std::thread::sleep(Duration::from_millis(10));
    assert_search_ids(&db, &["FT.SEARCH", "idx", "short"], &[]);

    apply(
        &db,
        &["HSET", "doc:3", "title", "field gone", "body", "body stays"],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "gone"], &["doc:3"]);
    let fields = vec!["title".to_string()];
    db.hash_expire_fields_at_ms(
        "doc:3",
        1,
        &fields,
        onedis_server::store::db::ExpireCondition::Always,
    )
    .expect("field expire failed");
    assert_search_ids(&db, &["FT.SEARCH", "idx", "gone"], &[]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "stays"], &["doc:3"]);
}

#[test]
fn json_writes_are_indexed_and_updated_synchronously() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "jidx",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "json:",
            "SCHEMA",
            "$.title",
            "AS",
            "title",
            "TEXT",
            "$.category",
            "AS",
            "category",
            "TAG",
            "$.score",
            "AS",
            "score",
            "NUMERIC",
        ],
    );

    apply(
        &db,
        &[
            "JSON.SET",
            "json:1",
            "$",
            r#"{"title":"redis json","category":"db","score":9}"#,
        ],
    );
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "redis"], &["json:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@category:{db}"], &["json:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@score:[8 10]"], &["json:1"]);

    apply(&db, &["JSON.SET", "json:1", "$.title", r#""other json""#]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "redis"], &[]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "other"], &["json:1"]);

    apply(&db, &["JSON.DEL", "json:1", "$.title"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "other"], &[]);
}

#[test]
fn skipinitialscan_keeps_existing_docs_async_but_indexes_new_writes() {
    let (_dir, db) = make_db();
    apply(&db, &["HSET", "doc:old", "title", "old skipped"]);
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SKIPINITIALSCAN",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "skipped"], &[]);

    apply(&db, &["HSET", "doc:new", "title", "new visible"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "visible"], &["doc:new"]);
}

#[test]
fn indexed_writes_remain_searchable_after_reopen() {
    let (dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    apply(&db, &["HSET", "doc:1", "title", "replay me"]);
    drop(db);

    let reopened = {
        let root = dir.path().join("db");
        let wal_dir = dir.path().join("wal");
        let store = KvStore::new(root, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    };
    wait_until(|| search_ids(apply(&reopened, &["FT.SEARCH", "idx", "replay"])) == vec!["doc:1"]);
}
