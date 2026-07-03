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

fn apply_result(db: &Db, args: &[&str]) -> Result<Frame, anyhow::Error> {
    onedis_server::command_dispatch::handle_command(db, command(args)?)
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

fn array_strings(frame: Frame) -> Vec<String> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items
        .into_iter()
        .map(|item| {
            let Frame::BulkString(value) = item else {
                panic!("expected bulk string");
            };
            String::from_utf8(value).unwrap()
        })
        .collect()
}

#[test]
fn ft_list_alias_and_config_survive_reopen() {
    let (dir, db) = make_db();
    assert!(matches!(
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
                "TEXT"
            ]
        ),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["FT.ALIASADD", "idx_alias", "idx"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "DEFAULT_DIALECT", "3"]),
        Frame::Ok
    ));
    apply(&db, &["HSET", "doc:1", "title", "alias durable"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx_alias", "durable"], &["doc:1"]);
    drop(db);

    let reopened = open_db_at(&dir);
    assert_eq!(array_strings(apply(&reopened, &["FT._LIST"])), vec!["idx"]);
    wait_for_search_ids(
        &reopened,
        &["FT.SEARCH", "idx_alias", "durable"],
        &["doc:1"],
    );
    let config = apply(&reopened, &["FT.CONFIG", "GET", "DEFAULT_DIALECT"]);
    assert_eq!(
        config.as_bytes(),
        b"*1\r\n*2\r\n$15\r\nDEFAULT_DIALECT\r\n$1\r\n3\r\n"
    );
}

#[test]
fn ft_dropindex_keeps_or_deletes_hash_documents() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx_keep",
            "PREFIX",
            "1",
            "keep:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    apply(&db, &["HSET", "keep:1", "title", "keep me"]);
    assert!(matches!(
        apply(&db, &["FT.DROPINDEX", "idx_keep"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "keep:1"]),
        Frame::Integer(1)
    ));
    assert!(apply_result(&db, &["FT.SEARCH", "idx_keep", "*"]).is_err());

    apply(
        &db,
        &[
            "FT.CREATE",
            "idx_delete",
            "PREFIX",
            "1",
            "delete:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    apply(&db, &["HSET", "delete:1", "title", "delete me"]);
    assert!(matches!(
        apply(&db, &["FT.DROPINDEX", "idx_delete", "DD"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "delete:1"]),
        Frame::Integer(0)
    ));
}

#[test]
fn ft_alter_adds_schema_and_survives_reopen() {
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
    apply(&db, &["HSET", "doc:1", "title", "alpha", "body", "beta"]);
    assert!(matches!(
        apply(&db, &["FT.ALTER", "idx", "SCHEMA", "ADD", "body", "TEXT"]),
        Frame::Ok
    ));
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "beta"], &["doc:1"]);
    drop(db);

    let reopened = open_db_at(&dir);
    wait_for_search_ids(&reopened, &["FT.SEARCH", "idx", "beta"], &["doc:1"]);
}

#[test]
fn ft_create_schema_alias_is_queryable_for_tag_fields() {
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
            "category",
            "AS",
            "cat",
            "TAG",
        ],
    );
    apply(&db, &["HSET", "doc:1", "category", "db"]);
    wait_for_search_ids(&db, &["FT.SEARCH", "idx", "@cat:{db}"], &["doc:1"]);
}

#[test]
fn ft_index_management_rejects_unsupported_options_deterministically() {
    let (_dir, db) = make_db();
    let err = match apply_result(&db, &["FT.CONFIG", "SET", "UNKNOWN", "1"]) {
        Ok(_) => panic!("unknown config should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("unsupported fulltext config option")
    );
}
