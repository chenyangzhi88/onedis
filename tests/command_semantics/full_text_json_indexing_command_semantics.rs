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

fn apply_err(db: &Db, args: &[&str]) -> anyhow::Error {
    let command = match command(args) {
        Ok(command) => command,
        Err(err) => return err,
    };
    match db.handle_command(command) {
        Ok(_) => panic!("command should fail"),
        Err(err) => err,
    }
}

fn array(frame: Frame) -> Vec<Frame> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items
}

fn integer(frame: &Frame) -> i64 {
    let Frame::Integer(value) = frame else {
        panic!("expected integer");
    };
    *value
}

fn bulk_text(frame: &Frame) -> String {
    let Frame::BulkString(value) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(value.clone()).unwrap()
}

fn search_ids(frame: Frame) -> Vec<String> {
    let items = array(frame);
    items[1..]
        .chunks(2)
        .map(|chunk| bulk_text(&chunk[0]))
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

fn field_value(fields: &Frame, name: &str) -> Option<String> {
    let Frame::Array(items) = fields else {
        panic!("expected fields array");
    };
    items.chunks(2).find_map(|chunk| {
        if bulk_text(&chunk[0]) == name {
            Some(bulk_text(&chunk[1]))
        } else {
            None
        }
    })
}

fn seed_json_index() -> (TempDir, Db) {
    let (dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "jidx",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "prod:",
            "SCHEMA",
            "$.name",
            "AS",
            "name",
            "TEXT",
            "$.tags[*]",
            "AS",
            "tags",
            "TAG",
            "$.prices[*]",
            "AS",
            "price",
            "NUMERIC",
            "$.variants[*].title",
            "AS",
            "variant",
            "TEXT",
        ],
    );
    apply(
        &db,
        &[
            "JSON.SET",
            "prod:1",
            "$",
            r#"{"name":"linen shirt","tags":["summer","sale"],"prices":[12,19],"variants":[{"title":"red cotton"},{"title":"blue linen"}]}"#,
        ],
    );
    (dir, db)
}

#[test]
fn ft_search_json_indexing_indexes_json_arrays_and_nested_values() {
    let (_dir, db) = seed_json_index();

    assert_search_ids(&db, &["FT.SEARCH", "jidx", "linen"], &["prod:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@tags:{sale}"], &["prod:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@price:[18 20]"], &["prod:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@price:[20 30]"], &[]);
}

#[test]
fn ft_search_json_indexing_returns_json_attributes_with_dialect_multi_value_shape() {
    let (_dir, db) = seed_json_index();

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "jidx",
            "@tags:{sale}",
            "RETURN",
            "3",
            "name",
            "tags",
            "price",
            "DIALECT",
            "3",
        ],
    ));
    assert_eq!(integer(&result[0]), 1);
    assert_eq!(bulk_text(&result[1]), "prod:1");
    assert_eq!(
        field_value(&result[2], "name").as_deref(),
        Some(r#""linen shirt""#)
    );
    assert_eq!(
        field_value(&result[2], "tags").as_deref(),
        Some(r#"["summer","sale"]"#)
    );
    assert_eq!(field_value(&result[2], "price").as_deref(), Some("[12,19]"));

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "jidx",
            "@tags:{sale}",
            "RETURN",
            "1",
            "tags",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(
        field_value(&result[2], "tags").as_deref(),
        Some(r#""summer""#)
    );
}

#[test]
fn ft_search_json_indexing_json_mutations_refresh_array_index_entries() {
    let (_dir, db) = seed_json_index();

    apply(&db, &["JSON.SET", "prod:1", "$.tags[1]", r#""clearance""#]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "@tags:{sale}"], &[]);
    assert_search_ids(
        &db,
        &["FT.SEARCH", "jidx", "@tags:{clearance}"],
        &["prod:1"],
    );

    apply(&db, &["JSON.DEL", "prod:1", "$.variants[0]"]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "cotton"], &[]);
    assert_search_ids(&db, &["FT.SEARCH", "jidx", "linen"], &["prod:1"]);
}

#[test]
fn ft_create_json_indexing_rejects_invalid_json_schema_paths() {
    let (_dir, db) = make_db();
    let err = apply_err(
        &db,
        &[
            "FT.CREATE",
            "bad",
            "ON",
            "JSON",
            "SCHEMA",
            "$.tags[",
            "AS",
            "tags",
            "TAG",
        ],
    );
    assert_eq!(err.to_string(), "ERR invalid JSON path");
}
