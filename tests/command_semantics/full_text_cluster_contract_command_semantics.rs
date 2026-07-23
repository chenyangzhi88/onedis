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
    try_apply(db, args).expect("command failed")
}

fn try_apply(db: &Db, args: &[&str]) -> Result<Frame, anyhow::Error> {
    onedis_server::command_dispatch::handle_command(
        db,
        command(args).expect("failed to parse command"),
    )
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

fn nested_value<'a>(items: &'a [Frame], name: &str) -> Option<&'a Frame> {
    info_value(items, name)
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

fn seed_docs(db: &Db) {
    create_index(db);
    assert_eq!(
        apply(
            db,
            &["HSET", "doc:1", "title", "quick fox", "category", "book"]
        )
        .to_string(),
        "2"
    );
    assert_eq!(
        apply(
            db,
            &["HSET", "doc:2", "title", "slow fox", "category", "book"]
        )
        .to_string(),
        "2"
    );
}

#[test]
fn ft_cluster_default_and_single_shard_keep_local_search_behavior() {
    let (_dir, db) = make_db();
    seed_docs(&db);

    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "fox"])), Some(2));
    assert_eq!(
        total(&apply(
            &db,
            &["FT.AGGREGATE", "idx", "*", "LOAD", "1", "@title"]
        )),
        Some(2)
    );

    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "CLUSTER_ENABLED", "true"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "CLUSTER_SHARDS", "1"]),
        Frame::Ok
    ));
    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "fox"])), Some(2));
    assert_eq!(
        total(&apply(
            &db,
            &["FT.AGGREGATE", "idx", "*", "LOAD", "1", "@title"]
        )),
        Some(2)
    );
}

#[test]
fn ft_cluster_multi_shard_uses_local_coordinator_results() {
    let (_dir, db) = make_db();
    seed_docs(&db);
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "CLUSTER_ENABLED", "yes"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["FT.CONFIG", "SET", "CLUSTER_SHARDS", "2"]),
        Frame::Ok
    ));

    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "fox"])), Some(2));
    assert_eq!(total(&apply(&db, &["FT.AGGREGATE", "idx", "*"])), Some(2));
    assert_eq!(total(&apply(&db, &["FT.HYBRID", "idx", "fox"])), Some(2));
}

#[test]
fn ft_cluster_info_and_config_expose_contract() {
    let (_dir, db) = make_db();
    seed_docs(&db);

    for (name, value) in [
        ("CLUSTER_ENABLED", "1"),
        ("CLUSTER_SHARDS", "3"),
        ("CLUSTER_SHARD_ID", "1"),
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

    let info = array(apply(&db, &["FT.INFO", "idx"]));
    let cluster = match info_value(&info, "cluster") {
        Some(Frame::Array(items)) => items,
        Some(other) => panic!("missing cluster info: {}", other.to_string()),
        None => panic!("missing cluster info"),
    };
    assert!(matches!(
        nested_value(cluster, "enabled"),
        Some(Frame::Integer(1))
    ));
    assert!(matches!(
        nested_value(cluster, "shards"),
        Some(Frame::Integer(3))
    ));
    assert!(matches!(
        nested_value(cluster, "shard_id"),
        Some(Frame::Integer(1))
    ));
    assert!(matches!(
        nested_value(cluster, "router_state"),
        Some(Frame::BulkString(value)) if value == b"local_coordinator"
    ));
    assert!(matches!(
        nested_value(cluster, "merge_policy"),
        Some(Frame::BulkString(value)) if value == b"score_desc_key_asc"
    ));
}

#[test]
fn ft_cluster_config_rejects_invalid_values() {
    let (_dir, db) = make_db();
    assert!(try_apply(&db, &["FT.CONFIG", "SET", "CLUSTER_SHARDS", "0"]).is_err());
    assert!(try_apply(&db, &["FT.CONFIG", "SET", "CLUSTER_ENABLED", "maybe"]).is_err());
    assert!(try_apply(&db, &["FT.CONFIG", "SET", "CLUSTER_ROUTING", "remote"]).is_err());
}
