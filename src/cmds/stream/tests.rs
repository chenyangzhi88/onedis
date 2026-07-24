use super::{
    xautoclaim::Xautoclaim, xclaim::Xclaim, xgroup::Xgroup, xinfo::Xinfo, xpending::Xpending,
    xread::Xread,
};
use crate::frame::Frame;
use crate::store::db::{Db, StreamId, StreamReadGroupStart};
use crate::store::kv_store::KvStore;
use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
use std::sync::Arc;

fn test_root(prefix: &str) -> std::path::PathBuf {
    let unique = format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique)
}

fn test_db() -> Db {
    let root = test_root("onedis-stream-cmd-test");
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

fn assert_array(frame: Frame) -> Vec<Frame> {
    match frame {
        Frame::Array(items) => items,
        _ => panic!("expected array frame"),
    }
}

fn assert_error(frame: Frame) {
    assert!(matches!(frame, Frame::Error(_)));
}

fn assert_integer(frame: Frame, expected: i64) {
    match frame {
        Frame::Integer(value) => assert_eq!(value, expected),
        _ => panic!("expected integer frame"),
    }
}

fn assert_ok(frame: Frame) {
    match frame {
        Frame::SimpleString(value) => assert_eq!(value, "OK"),
        _ => panic!("expected OK frame"),
    }
}

fn assert_bulk_text(frame: &Frame, expected: &str) {
    match frame {
        Frame::BulkString(value) => assert_eq!(value.as_slice(), expected.as_bytes()),
        _ => panic!("expected bulk string frame"),
    }
}

#[tokio::test]
async fn stream_wrappers_parse_apply_and_async_error_frames_cover_edges() {
    let db = test_db();
    let id1 = StreamId { ms: 1, seq: 0 };
    let id2 = StreamId { ms: 2, seq: 0 };
    db.stream_add("s", Some(id1), &[("f".to_string(), "v1".to_string())])
        .unwrap();
    db.stream_add("s", Some(id2), &[("f".to_string(), "v2".to_string())])
        .unwrap();

    assert!(Xgroup::parse_from_frame(frame(&["XGROUP"])).is_err());
    assert!(Xgroup::parse_from_frame(frame(&["XGROUP", "CREATE", "s"])).is_err());
    assert!(Xgroup::parse_from_frame(frame(&["XGROUP", "CREATE", "s", "g", "bad"])).is_err());
    assert!(Xgroup::parse_from_frame(frame(&["XGROUP", "BOGUS"])).is_err());
    assert_ok(
        Xgroup::parse_from_frame(frame(&["XGROUP", "CREATE", "s", "g", "0-0", "MKSTREAM"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
    );
    assert_integer(
        Xgroup::parse_from_frame(frame(&["XGROUP", "CREATECONSUMER", "s", "g", "c1"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
        1,
    );
    assert_ok(
        Xgroup::parse_from_frame(frame(&["XGROUP", "SETID", "s", "g", "$"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
    );
    assert_ok(
        Xgroup::parse_from_frame(frame(&["XGROUP", "SETID", "s", "g", "0-0"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
    );

    db.stream_read_group(
        "g",
        "c1",
        &[("s".to_string(), StreamReadGroupStart::New)],
        Some(1),
        false,
    )
    .unwrap();

    assert!(Xpending::parse_from_frame(frame(&["XPENDING", "s"])).is_err());
    assert!(Xpending::parse_from_frame(frame(&["XPENDING", "s", "g", "bad", "+", "10"])).is_err());
    let summary = assert_array(
        Xpending::parse_from_frame(frame(&["XPENDING", "s", "g"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
    );
    assert_integer(summary[0].clone(), 1);
    let range = assert_array(
        Xpending::parse_from_frame(frame(&["XPENDING", "s", "g", "-", "+", "10", "c1"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
    );
    assert_eq!(range.len(), 1);

    assert!(Xinfo::parse_from_frame(frame(&["XINFO", "CONSUMERS", "s"])).is_err());
    assert!(Xinfo::parse_from_frame(frame(&["XINFO", "BOGUS", "s"])).is_err());
    assert!(
        !assert_array(
            Xinfo::parse_from_frame(frame(&["XINFO", "GROUPS", "s"]))
                .unwrap()
                .apply(&db)
                .unwrap()
        )
        .is_empty()
    );
    assert!(
        !assert_array(
            Xinfo::parse_from_frame(frame(&["XINFO", "CONSUMERS", "s", "g"]))
                .unwrap()
                .apply_async(&db)
                .await
                .unwrap()
        )
        .is_empty()
    );
    let stream_info = assert_array(
        Xinfo::parse_from_frame(frame(&["XINFO", "STREAM", "s"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
    );
    assert_bulk_text(&stream_info[0], "length");

    assert!(Xread::parse_from_frame(frame(&["XREAD", "STREAMS", "s"])).is_err());
    assert!(
        Xread::parse_from_frame(frame(&["XREAD", "COUNT", "bad", "STREAMS", "s", "0-0"])).is_err()
    );
    assert!(Xread::parse_from_frame(frame(&["XREAD", "STREAMS", "s", "bad"])).is_err());
    assert!(matches!(
        Xread::parse_from_frame(frame(&["XREAD", "STREAMS", "s", "$"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
        Frame::Null
    ));
    assert!(
        !assert_array(
            Xread::parse_from_frame(frame(&[
                "XREAD", "COUNT", "2", "BLOCK", "1", "STREAMS", "s", "0-0",
            ]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap()
        )
        .is_empty()
    );

    assert!(Xclaim::parse_from_frame(frame(&["XCLAIM", "s", "g", "c2"])).is_err());
    assert!(Xclaim::parse_from_frame(frame(&["XCLAIM", "s", "g", "c2", "bad", "1-0"])).is_err());
    assert!(Xclaim::parse_from_frame(frame(&["XCLAIM", "s", "g", "c2", "0", "bad"])).is_err());
    assert!(
        Xclaim::parse_from_frame(frame(&["XCLAIM", "s", "g", "c2", "0", "1-0", "IDLE", "1",]))
            .is_err()
    );
    assert!(
        !assert_array(
            Xclaim::parse_from_frame(frame(&["XCLAIM", "s", "g", "c2", "0", "1-0", "JUSTID",]))
                .unwrap()
                .apply_async(&db)
                .await
                .unwrap()
        )
        .is_empty()
    );

    assert!(Xautoclaim::parse_from_frame(frame(&["XAUTOCLAIM", "s"])).is_err());
    assert!(
        Xautoclaim::parse_from_frame(frame(&["XAUTOCLAIM", "s", "g", "c3", "bad", "0-0",]))
            .is_err()
    );
    assert!(
        Xautoclaim::parse_from_frame(frame(&["XAUTOCLAIM", "s", "g", "c3", "0", "bad",])).is_err()
    );
    assert!(
        Xautoclaim::parse_from_frame(frame(&[
            "XAUTOCLAIM",
            "s",
            "g",
            "c3",
            "0",
            "0-0",
            "IGNORED",
        ]))
        .is_err()
    );
    let autoclaim = assert_array(
        Xautoclaim::parse_from_frame(frame(&[
            "XAUTOCLAIM",
            "s",
            "g",
            "c3",
            "0",
            "0-0",
            "COUNT",
            "10",
        ]))
        .unwrap()
        .apply_async(&db)
        .await
        .unwrap(),
    );
    assert_eq!(autoclaim.len(), 3);

    assert_integer(
        Xgroup::parse_from_frame(frame(&["XGROUP", "DELCONSUMER", "s", "g", "c3"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
        1,
    );
    assert_integer(
        Xgroup::parse_from_frame(frame(&["XGROUP", "DESTROY", "s", "g"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
        1,
    );
    assert_error(
        Xpending::parse_from_frame(frame(&["XPENDING", "s", "g"]))
            .unwrap()
            .apply_async(&db)
            .await
            .unwrap(),
    );
    assert_error(
        Xinfo::parse_from_frame(frame(&["XINFO", "CONSUMERS", "s", "g"]))
            .unwrap()
            .apply(&db)
            .unwrap(),
    );
}
