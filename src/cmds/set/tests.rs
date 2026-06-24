use crate::command::Command;
use crate::frame::Frame;
use crate::store::db::Db;
use crate::store::kv_store::KvStore;
use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
use std::sync::Arc;

fn test_db() -> Db {
    let unique = format!(
        "onedis-set-cmd-test-{}",
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
    let command = Command::parse_from_frame(frame(args)).unwrap();
    db.handle_command(command).unwrap()
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    let command = Command::parse_from_frame(frame(args)).unwrap();
    db.handle_command_async(command).await.unwrap()
}

fn parse_err(args: &[&str]) -> String {
    match Command::parse_from_frame(frame(args)) {
        Ok(command) => panic!("expected parse error, got {}", command.name()),
        Err(error) => error.to_string(),
    }
}

fn bulk_members(frame: Frame) -> Vec<String> {
    let Frame::Array(values) = frame else {
        panic!("expected array, got {}", frame.to_string());
    };
    let mut values = values
        .into_iter()
        .map(|value| match value {
            Frame::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected bulk string, got {}", other.to_string()),
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[tokio::test]
async fn set_commands_cover_sync_async_results_and_wrong_type_edges() {
    let db = test_db();
    seed_sets(&db);
    Box::pin(assert_membership_queries(&db)).await;
    Box::pin(assert_set_algebra_and_store_commands(&db)).await;
    Box::pin(assert_mutating_commands(&db)).await;
    Box::pin(assert_scan_commands(&db)).await;
    assert_wrong_type_edges(&db);
}

fn seed_sets(db: &Db) {
    assert!(matches!(
        apply(db, &["sadd", "a", "one", "two", "three"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(db, &["sadd", "b", "two", "three", "four"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(db, &["sadd", "c", "three", "five"]),
        Frame::Integer(2)
    ));
}

async fn assert_membership_queries(db: &Db) {
    assert!(matches!(apply(db, &["scard", "a"]), Frame::Integer(3)));
    assert!(matches!(
        apply_async(db, &["scard", "a"]).await,
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(db, &["sismember", "a", "one"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(db, &["sismember", "a", "missing"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(db, &["smismember", "a", "one", "missing", "two"]),
        Frame::Array(values) if values.len() == 3
    ));
}

async fn assert_set_algebra_and_store_commands(db: &Db) {
    assert_eq!(
        bulk_members(apply(db, &["sdiff", "a", "b"])),
        vec!["one".to_string()]
    );
    assert_eq!(
        bulk_members(apply_async(db, &["sdiff", "a", "b"]).await),
        vec!["one".to_string()]
    );
    assert!(matches!(
        apply(db, &["sdiffstore", "out-diff", "a", "b"]),
        Frame::Integer(1)
    ));
    assert_eq!(
        bulk_members(apply(db, &["smembers", "out-diff"])),
        vec!["one".to_string()]
    );
    assert_eq!(
        bulk_members(apply(db, &["sinter", "a", "b", "c"])),
        vec!["three".to_string()]
    );
    assert!(matches!(
        apply(db, &["sintercard", "3", "a", "b", "c"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(db, &["sintercard", "2", "a", "b", "limit", "1"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(db, &["sinterstore", "out-inter", "a", "b"]),
        Frame::Integer(2)
    ));
    assert_eq!(
        bulk_members(apply(db, &["sunion", "out-diff", "out-inter"])),
        vec!["one".to_string(), "three".to_string(), "two".to_string()]
    );
    assert!(matches!(
        apply_async(db, &["sunionstore", "out-union", "a", "c"]).await,
        Frame::Integer(4)
    ));
}

async fn assert_mutating_commands(db: &Db) {
    assert!(matches!(
        apply(db, &["smove", "a", "moved", "one"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(db, &["smove", "a", "moved", "absent"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(db, &["spop", "moved"]),
        Frame::BulkString(_) | Frame::Null
    ));
    assert!(matches!(
        apply_async(db, &["spop", "b", "2"]).await,
        Frame::Array(values) if values.len() <= 2
    ));
    assert!(matches!(
        apply(db, &["srandmember", "c"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(db, &["srandmember", "c", "-3"]).await,
        Frame::Array(values) if values.len() == 3
    ));
    assert!(matches!(
        apply(db, &["srem", "c", "five", "absent"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(db, &["srem", "c", "three"]).await,
        Frame::Integer(1)
    ));
}

async fn assert_scan_commands(db: &Db) {
    let scan = apply(
        &db,
        &["sscan", "out-union", "0", "match", "t*", "count", "10"],
    );
    assert!(matches!(
        scan,
        Frame::Array(values) if values.len() == 2 && matches!(values.first(), Some(Frame::Integer(_)))
    ));
    let async_scan = apply_async(db, &["sscan", "out-union", "0"]).await;
    assert!(matches!(async_scan, Frame::Array(values) if values.len() == 2));
}

fn assert_wrong_type_edges(db: &Db) {
    apply(db, &["set", "plain", "value"]);
    for args in [
        &["scard", "plain"][..],
        &["sdiff", "plain"][..],
        &["sinter", "plain"][..],
        &["sismember", "plain", "x"][..],
        &["smembers", "plain"][..],
        &["smismember", "plain", "x"][..],
        &["smove", "plain", "dst", "x"][..],
        &["spop", "plain"][..],
        &["srandmember", "plain"][..],
        &["srem", "plain", "x"][..],
        &["sscan", "plain", "0"][..],
        &["sunion", "plain"][..],
        &["sunionstore", "dst", "plain"][..],
    ] {
        assert!(
            matches!(apply(db, args), Frame::Error(message) if message.contains("wrong kind")),
            "{args:?}"
        );
    }
}

#[test]
fn set_command_parsers_reject_arity_and_option_errors() {
    for args in [
        &["sadd", "k"][..],
        &["scard"][..],
        &["scard", "k", "extra"][..],
        &["sdiff"][..],
        &["sdiffstore", "dst"][..],
        &["sinter"][..],
        &["sintercard"][..],
        &["sismember", "k"][..],
        &["smembers"][..],
        &["smismember", "k"][..],
        &["smove", "src", "dst"][..],
        &["spop"][..],
        &["spop", "k", "bad"][..],
        &["srandmember"][..],
        &["srandmember", "k", "bad"][..],
        &["srem", "k"][..],
        &["sscan", "k"][..],
        &["sscan", "k", "0", "match"][..],
        &["sscan", "k", "0", "count"][..],
        &["sscan", "k", "0", "unknown"][..],
        &["sunion"][..],
        &["sunionstore", "dst"][..],
    ] {
        assert!(!parse_err(args).is_empty(), "{args:?}");
    }
    assert!(parse_err(&["sintercard", "x", "a"]).contains("integer"));
    assert!(parse_err(&["sintercard", "2", "a"]).contains("syntax"));
    assert!(parse_err(&["sintercard", "1", "a", "limit"]).contains("syntax"));
    assert!(parse_err(&["sintercard", "1", "a", "limit", "x"]).contains("integer"));
    assert!(parse_err(&["sintercard", "1", "a", "bad"]).contains("syntax"));
}
