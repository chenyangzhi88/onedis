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
    let db_path = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&db_path).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(db_path, wal_dir, 1);
    let vc = Arc::new(VersionCounter::new());
    let ttl = TtlManager::new(store.clone(), TtlConfig::default());
    (dir, Db::new(0, store, vc, ttl))
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

fn try_apply(db: &Db, args: &[&str]) -> anyhow::Result<Frame> {
    onedis_server::command_dispatch::handle_command(db, command(args))
}

fn assert_ok(frame: Frame) {
    assert!(matches!(frame, Frame::Ok));
}

fn assert_integer(frame: Frame, expected: i64) {
    assert!(matches!(frame, Frame::Integer(value) if value == expected));
}

fn assert_bulk_string(frame: Frame, expected: &str) {
    assert!(
        matches!(frame, Frame::BulkString(bytes) if bytes.as_slice() == expected.as_bytes()),
        "expected bulk string {expected:?}"
    );
}

fn bulk_string(frame: Frame) -> String {
    let Frame::BulkString(bytes) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(bytes).expect("bulk string is not utf-8")
}

#[test]
fn redis_lua_commands_are_supported() {
    let (_dir, db) = make_db();

    assert_ok(apply(&db, &["SCRIPT", "FLUSH"]));

    let sha = bulk_string(apply(&db, &["SCRIPT", "LOAD", "return 'cached'"]));
    let exists = apply(
        &db,
        &[
            "SCRIPT",
            "EXISTS",
            &sha,
            "ffffffffffffffffffffffffffffffffffffffff",
        ],
    );
    assert!(matches!(exists, Frame::Array(values)
            if matches!(values.as_slice(), [Frame::Integer(1), Frame::Integer(0)])));
    assert_bulk_string(apply(&db, &["EVALSHA", &sha, "0"]), "cached");

    assert_bulk_string(
        apply(&db, &["EVAL", "return ARGV[1]", "0", "hello"]),
        "hello",
    );

    assert_ok(apply(
        &db,
        &[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "lua:key",
            "value",
        ],
    ));
    assert_bulk_string(apply(&db, &["GET", "lua:key"]), "value");

    let failed = try_apply(
        &db,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], 'bad'); error('boom')",
            "1",
            "lua:key",
        ],
    );
    assert!(failed.is_err());
    assert_bulk_string(apply(&db, &["GET", "lua:key"]), "value");

    assert_ok(apply(&db, &["SET", "lua:not-int", "abc"]));
    assert_integer(
        apply(
            &db,
            &[
                "EVAL",
                "local r = redis.pcall('INCR', KEYS[1]); return r['err'] ~= nil",
                "1",
                "lua:not-int",
            ],
        ),
        1,
    );

    let composite = apply(
        &db,
        &[
            "EVAL",
            "return {redis.status_reply('PONG'), redis.error_reply('ERR nope'), 3, false, 'tail'}",
            "0",
        ],
    );
    let Frame::Array(values) = composite else {
        panic!("expected array");
    };
    assert!(matches!(&values[0], Frame::SimpleString(text) if text == "PONG"));
    assert!(matches!(&values[1], Frame::Error(text) if text == "ERR nope"));
    assert!(matches!(&values[2], Frame::Integer(3)));
    assert!(matches!(&values[3], Frame::Null));
    assert!(matches!(&values[4], Frame::BulkString(bytes) if bytes.as_slice() == b"tail"));

    assert_bulk_string(
        apply(&db, &["EVAL", "return redis.sha1hex(ARGV[1])", "0", "abc"]),
        "a9993e364706816aba3e25717850c26c9cd0d89d",
    );
    assert_integer(
        apply(
            &db,
            &[
                "EVAL",
                "redis.setresp(3); redis.log(redis.LOG_NOTICE, 'msg'); return redis.acl_check_cmd('GET', KEYS[1])",
                "1",
                "lua:key",
            ],
        ),
        1,
    );
    assert_integer(
        apply(&db, &["EVAL", "return redis.replicate_commands()", "0"]),
        1,
    );

    assert_bulk_string(
        apply(
            &db,
            &[
                "EVAL_RO",
                "return redis.call('GET', KEYS[1])",
                "1",
                "lua:key",
            ],
        ),
        "value",
    );
    let read_only_write = try_apply(
        &db,
        &[
            "EVAL_RO",
            "return redis.call('SET', KEYS[1], 'read-only-write')",
            "1",
            "lua:key",
        ],
    );
    assert!(read_only_write.is_err());
    assert_bulk_string(apply(&db, &["GET", "lua:key"]), "value");

    assert_ok(apply(&db, &["SCRIPT", "DEBUG", "YES"]));
    assert!(try_apply(&db, &["SCRIPT", "KILL"]).is_err());
    assert_ok(apply(&db, &["SCRIPT", "FLUSH", "SYNC"]));
    assert!(try_apply(&db, &["EVALSHA", &sha, "0"]).is_err());
}
