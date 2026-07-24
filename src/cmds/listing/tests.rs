use crate::{
    command::Command,
    frame::Frame,
    store::{
        db::{Db, Structure},
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    },
};
use std::sync::Arc;

fn test_db() -> Db {
    let unique = format!(
        "onedis-listing-cmd-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"))
        .join(unique);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn frame(args: &[&str]) -> Frame {
    Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    )
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    crate::command_dispatch::handle_command(db, Command::parse_from_frame(frame(args)).unwrap())
        .unwrap()
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    crate::command_dispatch::handle_command_async(
        db,
        Command::parse_from_frame(frame(args)).unwrap(),
    )
    .await
    .unwrap()
}

fn parse_err(args: &[&str]) -> String {
    match Command::parse_from_frame(frame(args)) {
        Ok(command) => panic!("expected parse error, got {}", command.name()),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn listing_wrappers_cover_blocking_move_pop_lmpop_lpos_and_error_edges() {
    let db = test_db();
    assert!(matches!(
        apply(&db, &["rpush", "src", "a", "b", "c", "b"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(&db, &["blpop", "missing", "src", "0"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["brpop", "src", "0"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(&db, &["brpoplpush", "src", "dst", "0"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(&db, &["blmove", "dst", "src", "left", "right", "0"]).await,
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["lmove", "src", "dst", "right", "left"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["lmpop", "2", "missing", "dst", "right", "count", "2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["blmpop", "0", "2", "missing", "src", "left", "count", "1"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "lpos", "dst", "b", "rank", "-1", "count", "2", "maxlen", "10"
            ]
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["lpos", "dst", "not-found"]),
        Frame::Null
    ));

    assert!(matches!(apply(&db, &["blpop", "empty", "0"]), Frame::Null));
    assert!(matches!(
        apply_async(&db, &["brpoplpush", "empty", "dst", "0"]).await,
        Frame::Null
    ));
    assert!(matches!(
        apply(&db, &["lmpop", "1", "empty", "left"]),
        Frame::Null
    ));

    db.insert("plain".to_string(), Structure::String("value".to_string()));
    for args in [
        &["blpop", "plain", "0"][..],
        &["brpop", "plain", "0"][..],
        &["brpoplpush", "plain", "dst", "0"][..],
        &["blmove", "plain", "dst", "left", "right", "0"][..],
        &["lmpop", "1", "plain", "left"][..],
        &["lpos", "plain", "value"][..],
    ] {
        assert!(
            matches!(apply(&db, args), Frame::Error(message) if message.contains("wrong kind")),
            "{args:?}"
        );
    }

    for args in [
        &["blpop", "key"][..],
        &["blpop", "key", "bad"][..],
        &["blpop", "key", "-1"][..],
        &["brpop", "key", "bad"][..],
        &["brpoplpush", "src", "dst"][..],
        &["brpoplpush", "src", "dst", "bad"][..],
        &["brpoplpush", "src", "dst", "-1"][..],
        &["blmove", "src", "dst", "left", "right"][..],
        &["blmove", "src", "dst", "bad", "right", "0"][..],
        &["blmove", "src", "dst", "left", "bad", "0"][..],
        &["blmove", "src", "dst", "left", "right", "bad"][..],
        &["blmove", "src", "dst", "left", "right", "-1"][..],
        &["lmove", "src", "dst", "bad", "right"][..],
        &["lmpop", "0", "left"][..],
        &["lmpop", "x", "list", "left"][..],
        &["lmpop", "1", "list", "bad"][..],
        &["lmpop", "1", "list", "left", "bad", "1"][..],
        &["lmpop", "1", "list", "left", "count", "0"][..],
        &["lmpop", "1", "list", "left", "count", "bad"][..],
        &["lpos", "list"][..],
        &["lpos", "list", "a", "rank"][..],
        &["lpos", "list", "a", "rank", "0"][..],
        &["lpos", "list", "a", "rank", "bad"][..],
        &["lpos", "list", "a", "count", "bad"][..],
        &["lpos", "list", "a", "maxlen", "bad"][..],
        &["lpos", "list", "a", "unknown"][..],
    ] {
        assert!(!parse_err(args).is_empty(), "{args:?}");
    }
}
