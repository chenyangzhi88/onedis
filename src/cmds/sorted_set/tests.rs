use crate::command::Command;
use crate::frame::Frame;
use crate::store::db::Db;
use crate::store::kv_store::KvStore;
use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
use std::sync::Arc;

fn test_db() -> Db {
    let unique = format!(
        "onedis-zset-cmd-test-{}",
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
    crate::command_dispatch::handle_command(db, command).unwrap()
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    let command = Command::parse_from_frame(frame(args)).unwrap();
    crate::command_dispatch::handle_command_async(db, command)
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
async fn sorted_set_commands_cover_sync_async_pop_store_and_wrong_type_edges() {
    let db = test_db();
    Box::pin(assert_basic_read_and_range_commands(&db)).await;
    Box::pin(assert_lex_range_commands(&db)).await;
    Box::pin(assert_setops_store_scan_and_random_commands(&db)).await;
    Box::pin(assert_pop_remove_commands(&db)).await;
    assert_wrong_type_edges(&db);
}

async fn assert_basic_read_and_range_commands(db: &Db) {
    assert!(matches!(
        apply(db, &["zadd", "z", "1", "a", "2", "b", "3", "c"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_async(db, &["zadd", "z2", "2", "b", "4", "d"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(apply(db, &["zcard", "z"]), Frame::Integer(3)));
    assert!(matches!(
        apply_async(db, &["zcount", "z", "1", "2"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(db, &["zscore", "z", "a"]),
        Frame::BulkString(value) if value == b"1"
    ));
    assert!(matches!(
        apply_async(db, &["zmscore", "z", "a", "missing"]).await,
        Frame::Array(values) if values.len() == 2 && matches!(values.get(1), Some(Frame::Null))
    ));
    assert!(matches!(
        apply(db, &["zincrby", "z", "1.5", "a"]),
        Frame::BulkString(value) if value == b"2.5"
    ));
    assert!(matches!(apply(db, &["zrank", "z", "a"]), Frame::Integer(_)));
    assert!(matches!(
        apply_async(db, &["zrevrank", "z", "a"]).await,
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(db, &["zrange", "z", "0", "-1", "withscores"]),
        Frame::Array(values) if values.len() == 6
    ));
    assert!(matches!(
        apply(db, &["zrange", "z", "1", "3", "byscore", "limit", "0", "2", "withscores"]),
        Frame::Array(values) if values.len() == 4
    ));
    assert!(matches!(
        apply_async(db, &["zrange", "z", "1", "3", "byscore", "rev", "limit", "0", "2"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(db, &["zrevrange", "z", "0", "1", "withscores"]).await,
        Frame::Array(values) if values.len() == 4
    ));
    assert!(matches!(
        apply(db, &["zrangebyscore", "z", "1", "3", "withscores"]),
        Frame::Array(values) if values.len() >= 4
    ));
    assert!(matches!(
        apply(db, &["zrevrangebyscore", "z", "3", "1", "withscores"]),
        Frame::Array(values) if values.len() >= 4
    ));
}

async fn assert_lex_range_commands(db: &Db) {
    assert!(matches!(
        apply(
            &db,
            &["zadd", "lex", "0", "alpha", "0", "bravo", "0", "charlie"]
        ),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(db, &["zlexcount", "lex", "[alpha", "[charlie"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_async(db, &["zrangebylex", "lex", "[alpha", "+", "limit", "0", "2"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(db, &["zrange", "lex", "[alpha", "+", "bylex", "limit", "1", "2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(db, &["zrange", "lex", "[alpha", "+", "bylex", "rev", "limit", "0", "2"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(db, &["zrevrangebylex", "lex", "+", "-", "limit", "0", "1"]),
        Frame::Array(values) if values.len() == 1
    ));
}

async fn assert_setops_store_scan_and_random_commands(db: &Db) {
    assert!(matches!(
        apply(db, &["zunion", "2", "z", "z2", "weights", "2", "3", "aggregate", "max", "withscores"]),
        Frame::Array(values) if !values.is_empty()
    ));
    assert!(matches!(
        apply_async(db, &["zinter", "2", "z", "z2", "weights", "1", "1", "aggregate", "min", "withscores"]).await,
        Frame::Array(values) if !values.is_empty()
    ));
    assert!(matches!(
        apply(db, &["zdiff", "2", "z", "z2", "withscores"]),
        Frame::Array(values) if !values.is_empty()
    ));
    assert!(matches!(
        apply(db, &["zunionstore", "zu", "2", "z", "z2", "aggregate", "sum"]),
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(
        apply_async(db, &["zinterstore", "zi", "2", "z", "z2"]).await,
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(
        apply_async(db, &["zdiffstore", "zd", "2", "z", "z2"]).await,
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(
        apply(db, &["zintercard", "2", "z", "z2"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(db, &["zrangestore", "zr", "z", "0", "-1"]),
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "zrangestore",
                "zr-score",
                "z",
                "1",
                "3",
                "byscore",
                "limit",
                "1",
                "2"
            ]
        ),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "zrangestore",
                "zr-rev",
                "z",
                "1",
                "3",
                "byscore",
                "rev",
                "limit",
                "0",
                "2"
            ]
        )
        .await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(db, &["zscan", "z", "0", "match", "*", "count", "10"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(db, &["zrandmember", "z", "-3", "withscores"]).await,
        Frame::Array(values) if values.len() == 6
    ));
}

async fn assert_pop_remove_commands(db: &Db) {
    assert!(matches!(
        apply(db, &["bzpopmin", "zu", "0"]),
        Frame::Array(values) if values.len() == 3
    ));
    assert!(matches!(
        apply_async(db, &["bzpopmax", "zu", "0"]).await,
        Frame::Array(values) if values.len() == 3
    ));
    assert!(matches!(
        apply(db, &["bzmpop", "0", "2", "missing", "zu", "min", "count", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(db, &["zpopmin", "z", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(db, &["zpopmax", "z", "1"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(db, &["zrem", "z", "a", "missing"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(db, &["zremrangebyscore", "z", "0", "10"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(db, &["zremrangebyrank", "z2", "0", "0"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply_async(db, &["zremrangebylex", "lex", "-", "[bravo"]).await,
        Frame::Integer(_)
    ));
}

fn assert_wrong_type_edges(db: &Db) {
    apply(db, &["set", "plain", "value"]);
    for args in [
        &["zcard", "plain"][..],
        &["zcount", "plain", "0", "1"][..],
        &["zscore", "plain", "x"][..],
        &["zrange", "plain", "0", "-1"][..],
        &["zscan", "plain", "0"][..],
        &["zpopmin", "plain"][..],
    ] {
        assert!(
            matches!(apply(db, args), Frame::Error(message) if message.contains("wrong kind")),
            "{args:?}"
        );
    }
}

#[test]
fn sorted_set_parsers_cover_error_edges() {
    for args in [
        &["zadd", "z", "1"][..],
        &["zadd", "z", "bad", "m"][..],
        &["zcard"][..],
        &["zcount", "z", "a", "1"][..],
        &["zincrby", "z", "nan", "m"][..],
        &["zrank", "z"][..],
        &["zrem", "z"][..],
        &["zscan", "z"][..],
        &["zscan", "z", "bad"][..],
        &["zscan", "z", "0", "match"][..],
        &["zscan", "z", "0", "count"][..],
        &["zscan", "z", "0", "bad"][..],
        &["bzpopmin", "z", "bad"][..],
        &["bzpopmax", "z", "-1"][..],
        &["bzmpop", "x"][..],
        &["bzmpop", "-1", "1", "z", "min"][..],
        &["bzmpop", "0", "0", "min"][..],
        &["bzmpop", "0", "1", "z", "bad"][..],
        &["bzmpop", "0", "1", "z", "min", "bad", "1"][..],
        &["bzmpop", "0", "1", "z", "min", "count", "x"][..],
        &["zinter", "0"][..],
        &["zinter", "x", "z"][..],
        &["zinter", "1"][..],
        &["zinter", "1", "z", "weights", "bad"][..],
        &["zinter", "1", "z", "aggregate", "bad"][..],
        &["zinter", "1", "z", "unknown"][..],
        &["zunionstore", "dst", "0"][..],
        &["zdiffstore", "dst"][..],
        &["zintercard", "0"][..],
        &["zrandmember"][..],
        &["zrandmember", "z", "x"][..],
        &["zrange", "z", "0", "-1", "byscore", "bylex"][..],
        &["zrange", "z", "0", "-1", "limit", "0", "1"][..],
        &["zrange", "z", "bad", "-1"][..],
        &["zrange", "z", "0", "bad"][..],
        &["zrange", "z", "bad", "1", "byscore"][..],
        &["zrange", "z", "1", "bad", "byscore"][..],
        &["zrange", "z", "bad", "+", "bylex"][..],
        &["zrange", "z", "[a", "bad", "bylex"][..],
        &["zrange", "z", "0", "-1", "limit", "bad", "1", "byscore"][..],
        &["zrange", "z", "0", "-1", "limit", "0", "bad", "byscore"][..],
        &["zrange", "z", "0", "-1", "unknown"][..],
        &["zrangestore", "dst", "z", "0"][..],
        &["zrangestore", "dst", "z", "0", "-1", "bylex"][..],
        &["zrangestore", "dst", "z", "0", "-1", "withscores"][..],
        &["zrangestore", "dst", "z", "0", "-1", "limit", "0", "1"][..],
        &[
            "zrangestore",
            "dst",
            "z",
            "0",
            "-1",
            "limit",
            "bad",
            "1",
            "byscore",
        ][..],
        &[
            "zrangestore",
            "dst",
            "z",
            "0",
            "-1",
            "limit",
            "0",
            "bad",
            "byscore",
        ][..],
        &["zrangestore", "dst", "z", "bad", "1", "byscore"][..],
        &["zrangestore", "dst", "z", "1", "bad", "byscore"][..],
        &["zrangestore", "dst", "z", "bad", "1"][..],
        &["zrangestore", "dst", "z", "1", "bad"][..],
        &["zrangestore", "dst", "z", "0", "-1", "unknown"][..],
        &["zrevrange", "z", "0", "-1", "bad"][..],
        &["zrevrangebyscore", "z", "bad", "1"][..],
        &["zremrangebyrank", "z", "bad", "1"][..],
        &["zremrangebyscore", "z", "bad", "1"][..],
    ] {
        assert!(!parse_err(args).is_empty(), "{args:?}");
    }
}
