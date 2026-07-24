mod support;

use support::*;

#[tokio::test]
async fn transaction_async_scans_see_pending_writes_and_deletes() {
    let store = test_store(11);
    store.put_raw(b"txn-scan:a", b"old");
    store.put_raw(b"other:a", b"ignore");

    let txn_store = store.begin_transaction().unwrap();
    txn_store.put_raw(b"txn-scan:b", b"new");
    txn_store.delete_key(b"txn-scan:a");

    let entries = txn_store.scan_prefix_raw_async(b"txn-scan:").await;
    assert_eq!(entries, vec![(b"txn-scan:b".to_vec(), b"new".to_vec())]);
}

#[tokio::test]
async fn transaction_async_copy_and_move_copy_complex_structures() {
    let store = test_store(12).begin_transaction().unwrap();
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    let db0 = Db::new(
        0,
        store.clone(),
        version_counter.clone(),
        ttl_manager.clone(),
    );

    assert!(matches!(
        apply_command_async(&db0, &["sadd", "source-set", "a", "b"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command_async(&db0, &["copy", "source-set", "copy-set"]).await,
        Frame::Integer(1)
    ));
    let copied = apply_command_async(&db0, &["smembers", "copy-set"]).await;
    assert!(array_contains_bulk(&copied, "a"));
    assert!(array_contains_bulk(&copied, "b"));

    assert!(matches!(
        apply_command_async(&db0, &["move", "copy-set", "1"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db0, &["exists", "copy-set"]).await,
        Frame::Integer(0)
    ));

    let db1 = Db::new(1, store, version_counter, ttl_manager);
    let moved = apply_command_async(&db1, &["smembers", "copy-set"]).await;
    assert!(array_contains_bulk(&moved, "a"));
    assert!(array_contains_bulk(&moved, "b"));
}

#[test]
fn incrby_updates_numeric_string_in_kv_engine() {
    let db = test_db();
    db.insert("counter".to_string(), Structure::String("40".to_string()));

    let frame = Incrby {
        key: "counter".to_string(),
        increment: 2,
    }
    .apply(&db)
    .unwrap();

    assert!(matches!(frame, Frame::Integer(42)));
    assert!(matches!(
        db.get("counter"),
        Some(Structure::String(value)) if value == "42"
    ));
}

#[test]
fn getset_replaces_string_and_returns_old_value() {
    let db = test_db();
    db.insert("name".to_string(), Structure::String("alice".to_string()));

    let frame = GetSet {
        key: "name".to_string(),
        value: "bob".to_string(),
    }
    .apply(&db)
    .unwrap();

    assert!(matches!(frame, Frame::BulkString(value) if value.as_slice() == b"alice"));
    assert!(matches!(
        db.get("name"),
        Some(Structure::String(value)) if value == "bob"
    ));
}

#[test]
fn setrange_writes_sparse_string_into_kv_engine() {
    let db = test_db();
    db.insert("blob".to_string(), Structure::String("abc".to_string()));

    let frame = SetRange {
        key: "blob".to_string(),
        offset: 5,
        value: b"Z".to_vec(),
    }
    .apply(&db)
    .unwrap();

    assert!(matches!(frame, Frame::Integer(6)));
    assert!(matches!(
        db.get("blob"),
        Some(Structure::String(value)) if value.as_bytes() == b"abc\0\0Z"
    ));
}

#[test]
fn rename_moves_value_within_same_kv_engine_db() {
    let db = test_db();
    db.insert("old".to_string(), Structure::String("value".to_string()));

    let frame = Rename {
        old_key: "old".to_string(),
        new_key: "new".to_string(),
    }
    .apply(&db)
    .unwrap();

    assert!(matches!(frame, Frame::Ok));
    assert!(db.get("old").is_none());
    assert!(matches!(
        db.get("new"),
        Some(Structure::String(value)) if value == "value"
    ));
}

#[test]
fn key_command_smoke_covers_getrange_randomkey_dbsize_and_renamenx() {
    let db = test_db();

    apply_command(&db, &["set", "alpha", "abcdef"]);
    assert!(matches!(
        apply_command(&db, &["getrange", "alpha", "1", "3"]),
        Frame::BulkString(value) if value.as_slice() == b"bcd"
    ));
    assert!(matches!(
        apply_command(&db, &["substr", "alpha", "2", "4"]),
        Frame::BulkString(value) if value.as_slice() == b"cde"
    ));
    assert!(matches!(
        apply_command(&db, &["dbsize"]),
        Frame::Integer(value) if value >= 1
    ));
    assert!(matches!(
        apply_command(&db, &["randomkey"]),
        Frame::BulkString(_)
    ));

    assert!(matches!(
        apply_command(&db, &["renamenx", "alpha", "beta"]),
        Frame::Integer(1)
    ));
    apply_command(&db, &["set", "gamma", "exists"]);
    assert!(matches!(
        apply_command(&db, &["renamenx", "beta", "gamma"]),
        Frame::Integer(0)
    ));
}

#[test]
fn key_command_smoke_covers_copy_unlink_touch_and_expiretime() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["set", "ttl-key", "value", "px", "5000"]),
        Frame::Ok
    ));
    let pexpiretime = match apply_command(&db, &["pexpiretime", "ttl-key"]) {
        Frame::Integer(value) => value,
        other => panic!("unexpected pexpiretime frame: {}", other),
    };
    assert!(pexpiretime > 0);
    assert!(matches!(
        apply_command(&db, &["expiretime", "ttl-key"]),
        Frame::Integer(value) if value == pexpiretime / 1000
    ));

    assert!(matches!(
        apply_command(&db, &["copy", "ttl-key", "ttl-copy"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["copy", "ttl-key", "ttl-copy"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["copy", "ttl-key", "ttl-copy", "replace"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["get", "ttl-copy"]),
        Frame::BulkString(value) if value.as_slice() == b"value"
    ));
    assert!(matches!(
        apply_command(&db, &["pexpiretime", "ttl-copy"]),
        Frame::Integer(value) if value == pexpiretime
    ));

    assert!(matches!(
        apply_command(&db, &["touch", "ttl-key", "ttl-copy", "missing"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["unlink", "ttl-key", "ttl-copy", "missing"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["touch", "ttl-key", "ttl-copy"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["expiretime", "ttl-key"]),
        Frame::Integer(-2)
    ));

    assert!(matches!(
        apply_command(&db, &["set", "flush-a", "1"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["set", "flush-b", "2"]),
        Frame::Ok
    ));
    assert!(matches!(apply_command(&db, &["flushall"]), Frame::Ok));
    assert!(matches!(
        apply_command(&db, &["exists", "flush-a", "flush-b"]),
        Frame::Integer(0)
    ));
}

#[test]
fn list_command_smoke_covers_pop_pushx_and_lset() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["lpushx", "missing-list", "x"]),
        Frame::Integer(0)
    ));
    apply_command(&db, &["rpush", "letters", "b", "c"]);
    assert!(matches!(
        apply_command(&db, &["lpushx", "letters", "a"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["rpushx", "letters", "d"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply_command(&db, &["lset", "letters", "1", "B"]),
        Frame::SimpleString(value) if value == "OK"
    ));
    assert!(matches!(
        apply_command(&db, &["linsert", "letters", "before", "B", "pre-B"]),
        Frame::Integer(5)
    ));
    assert!(matches!(
        apply_command(&db, &["lpos", "letters", "pre-B"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["lpop", "letters"]),
        Frame::BulkString(value) if value.as_slice() == b"a"
    ));
    assert!(matches!(
        apply_command(&db, &["rpop", "letters"]),
        Frame::BulkString(value) if value.as_slice() == b"d"
    ));
    let range = apply_command(&db, &["lrange", "letters", "0", "-1"]);
    assert!(array_contains_bulk(&range, "pre-B"));
    assert!(array_contains_bulk(&range, "B"));
    assert!(array_contains_bulk(&range, "c"));

    assert!(matches!(
        apply_command(&db, &["rpoplpush", "letters", "letters-2"]),
        Frame::BulkString(value) if value.as_slice() == b"c"
    ));
    assert!(matches!(
        apply_command(&db, &["lmove", "letters", "letters-2", "left", "right"]),
        Frame::BulkString(value) if value.as_slice() == b"pre-B"
    ));
    assert!(matches!(
        apply_command(&db, &["lmpop", "2", "missing", "letters-2", "left", "count", "2"]),
        Frame::Array(items) if items.len() == 2
    ));
    apply_command(&db, &["rpush", "blocking-a", "1", "2"]);
    apply_command(&db, &["rpush", "blocking-b", "x"]);
    assert!(matches!(
        apply_command(&db, &["blpop", "missing", "blocking-a", "1"]),
        Frame::Array(items) if items.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["brpop", "blocking-a", "1"]),
        Frame::Array(items) if items.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["brpoplpush", "blocking-b", "blocking-c", "1"]),
        Frame::BulkString(value) if value.as_slice() == b"x"
    ));
    apply_command(&db, &["rpush", "blocking-d", "left", "right"]);
    assert!(matches!(
        apply_command(&db, &["blmove", "blocking-d", "blocking-e", "right", "left", "1"]),
        Frame::BulkString(value) if value.as_slice() == b"right"
    ));
    apply_command(&db, &["rpush", "blocking-f", "a", "b"]);
    assert!(matches!(
        apply_command(&db, &["blmpop", "1", "1", "blocking-f", "left", "count", "2"]),
        Frame::Array(items) if items.len() == 2
    ));
}

#[test]
fn ttl_and_persist_observe_same_backed_key() {
    let db = test_db();
    db.insert(
        "session".to_string(),
        Structure::String("token".to_string()),
    );
    db.expire("session".to_string(), 2_000);

    let ttl_frame = Ttl::parse_from_frame(frame_args(&["ttl", "session"]))
        .unwrap()
        .apply(&db)
        .unwrap();
    let ttl_seconds = match ttl_frame {
        Frame::Integer(value) => value,
        other => panic!("unexpected frame: {}", other),
    };
    assert!((1..=2).contains(&ttl_seconds));

    let persist_frame = Persist::parse_from_frame(frame_args(&["persist", "session"]))
        .unwrap()
        .apply(&db)
        .unwrap();
    assert!(matches!(persist_frame, Frame::Integer(1)));

    let ttl_after_persist = Ttl::parse_from_frame(frame_args(&["ttl", "session"]))
        .unwrap()
        .apply(&db)
        .unwrap();
    assert!(matches!(ttl_after_persist, Frame::Integer(-1)));
}

#[test]
fn expire_still_removes_key_after_command_level_write() {
    let db = test_db();
    Append {
        key: "temp".to_string(),
        val: b"value".to_vec(),
    }
    .apply(&db)
    .unwrap();
    db.expire("temp".to_string(), 20);

    std::thread::sleep(Duration::from_millis(30));
    assert!(db.get("temp").is_none());
}

#[test]
fn set_and_get_round_trip_through_command_dispatch() {
    let db = test_db();

    let set_frame = apply_command(&db, &["set", "name", "alice"]);
    assert!(matches!(set_frame, Frame::Ok));

    let get_frame = apply_command(&db, &["get", "name"]);
    assert!(matches!(get_frame, Frame::BulkString(value) if value.as_slice() == b"alice"));
}

#[test]
fn binary_string_values_round_trip_through_set_get_and_mget() {
    let db = test_db();
    let payload = vec![0, b'a', 0xff, b'\n'];

    let set_frame = apply_frame(
        &db,
        Frame::Array(vec![
            Frame::bulk_string("set"),
            Frame::bulk_string("blob"),
            Frame::bulk_string(payload.clone()),
        ]),
    );
    assert!(matches!(set_frame, Frame::Ok));

    let get_frame = apply_command(&db, &["get", "blob"]);
    assert!(matches!(get_frame, Frame::BulkString(value) if value == payload));

    let mget_frame = apply_command(&db, &["mget", "blob", "missing"]);
    match mget_frame {
        Frame::Array(values) => {
            assert_eq!(values.len(), 2);
            assert!(
                matches!(&values[0], Frame::BulkString(value) if value.as_slice() == payload.as_slice())
            );
            assert!(matches!(&values[1], Frame::Null));
        }
        other => panic!("unexpected frame: {}", other),
    }
}

#[test]
fn json_get_and_type_commands_round_trip_indexed_json() {
    let db = test_db();

    assert!(matches!(
        apply_command(
            &db,
            &[
                "json.set",
                "doc",
                "$",
                r#"{"name":"alice","age":30,"tags":["rust","db"]}"#,
            ],
        ),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["json.get", "doc", "$.name"]),
        Frame::BulkString(value) if value == br#""alice""#
    ));
    assert!(matches!(
        apply_command(&db, &["json.get", "doc", "$.tags[1]"]),
        Frame::BulkString(value) if value == br#""db""#
    ));
    assert!(matches!(
        apply_command(&db, &["json.type", "doc", "$"]),
        Frame::SimpleString(value) if value == "object"
    ));
    assert!(matches!(
        apply_command(&db, &["json.type", "doc", "$.age"]),
        Frame::SimpleString(value) if value == "integer"
    ));
    assert!(matches!(
        apply_command(&db, &["json.type", "doc", "$.missing"]),
        Frame::Null
    ));
}

#[test]
fn set_options_cover_conditions_get_absolute_expiry_and_keep_ttl() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["set", "guard", "one", "nx"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["set", "guard", "two", "nx"]),
        Frame::Null
    ));
    assert!(matches!(
        apply_command(&db, &["get", "guard"]),
        Frame::BulkString(value) if value.as_slice() == b"one"
    ));

    assert!(matches!(
        apply_command(&db, &["set", "guard", "two", "xx", "get"]),
        Frame::BulkString(value) if value.as_slice() == b"one"
    ));
    assert!(matches!(
        apply_command(&db, &["set", "missing", "value", "xx"]),
        Frame::Null
    ));

    assert!(matches!(
        apply_command(&db, &["set", "ephemeral", "v1", "px", "5000"]),
        Frame::Ok
    ));
    let before_keep = match apply_command(&db, &["pttl", "ephemeral"]) {
        Frame::Integer(value) => value,
        other => panic!("unexpected PTTL frame: {}", other),
    };
    assert!(matches!(
        apply_command(&db, &["set", "ephemeral", "v2", "keepttl"]),
        Frame::Ok
    ));
    let after_keep = match apply_command(&db, &["pttl", "ephemeral"]) {
        Frame::Integer(value) => value,
        other => panic!("unexpected PTTL frame: {}", other),
    };
    assert!(after_keep > 0 && after_keep <= before_keep);

    let pxat = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 5000)
        .to_string();
    assert!(matches!(
        apply_command(&db, &["set", "absolute", "v", "pxat", &pxat]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["pttl", "absolute"]),
        Frame::Integer(value) if value > 0
    ));

    let exat_past = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(1))
    .to_string();
    assert!(matches!(
        apply_command(&db, &["set", "gone", "v", "exat", &exat_past]),
        Frame::Ok
    ));
    assert!(matches!(apply_command(&db, &["get", "gone"]), Frame::Null));
}

#[test]
fn getex_psetex_and_msetnx_follow_string_ttl_semantics() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["psetex", "session", "5000", "payload"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["getex", "session", "persist"]),
        Frame::BulkString(value) if value.as_slice() == b"payload"
    ));
    assert!(matches!(
        apply_command(&db, &["pttl", "session"]),
        Frame::Integer(-1)
    ));

    assert!(matches!(
        apply_command(&db, &["getex", "session", "px", "5000"]),
        Frame::BulkString(value) if value.as_slice() == b"payload"
    ));
    assert!(matches!(
        apply_command(&db, &["pttl", "session"]),
        Frame::Integer(value) if value > 0
    ));

    let past_pxat = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .saturating_sub(1))
    .to_string();
    assert!(matches!(
        apply_command(&db, &["getex", "session", "pxat", &past_pxat]),
        Frame::BulkString(value) if value.as_slice() == b"payload"
    ));
    assert!(matches!(
        apply_command(&db, &["get", "session"]),
        Frame::Null
    ));

    assert!(matches!(
        apply_command(&db, &["msetnx", "a", "1", "b", "2"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["msetnx", "b", "new", "c", "3"]),
        Frame::Integer(0)
    ));
    assert!(matches!(apply_command(&db, &["get", "c"]), Frame::Null));

    let payload = vec![b'x', 0, 0xfe];
    assert!(matches!(
        apply_frame(
            &db,
            Frame::Array(vec![
                Frame::bulk_string("msetnx"),
                Frame::bulk_string("bin"),
                Frame::bulk_string(payload.clone()),
            ]),
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["getdel", "bin"]),
        Frame::BulkString(value) if value == payload
    ));
    assert!(matches!(apply_command(&db, &["get", "bin"]), Frame::Null));
}

#[test]
fn stream_commands_round_trip_through_command_dispatch() {
    let db = test_db();

    let id1 = apply_command(&db, &["XADD", "events", "1-0", "type", "created"]);
    assert!(matches!(id1, Frame::BulkString(value) if value.as_slice() == b"1-0"));
    let id2 = apply_command(
        &db,
        &["XADD", "events", "2-0", "type", "updated", "user", "alice"],
    );
    assert!(matches!(id2, Frame::BulkString(value) if value.as_slice() == b"2-0"));

    assert!(matches!(
        apply_command(&db, &["XLEN", "events"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["TYPE", "events"]),
        Frame::SimpleString(value) if value == "stream"
    ));

    let range = apply_command(&db, &["XRANGE", "events", "-", "+"]);
    let Frame::Array(entries) = range else {
        panic!("unexpected XRANGE frame");
    };
    assert_eq!(entries.len(), 2);

    let read = apply_command(&db, &["XREAD", "STREAMS", "events", "1-0"]);
    let Frame::Array(streams) = read else {
        panic!("unexpected XREAD frame");
    };
    assert_eq!(streams.len(), 1);

    let rev = apply_command(&db, &["XREVRANGE", "events", "+", "-"]);
    let Frame::Array(rev_entries) = rev else {
        panic!("unexpected XREVRANGE frame");
    };
    assert_eq!(rev_entries.len(), 2);

    assert!(matches!(
        apply_command(&db, &["XGROUP", "CREATE", "events", "workers", "0-0"]),
        Frame::SimpleString(value) if value == "OK"
    ));
    let group_read = apply_command(
        &db,
        &[
            "XREADGROUP",
            "GROUP",
            "workers",
            "alice",
            "COUNT",
            "1",
            "STREAMS",
            "events",
            ">",
        ],
    );
    let Frame::Array(group_streams) = group_read else {
        panic!("unexpected XREADGROUP frame");
    };
    assert_eq!(group_streams.len(), 1);

    let pending = apply_command(&db, &["XPENDING", "events", "workers"]);
    let Frame::Array(pending_summary) = pending else {
        panic!("unexpected XPENDING frame");
    };
    assert!(matches!(&pending_summary[0], Frame::Integer(1)));
    assert!(matches!(
        apply_command(&db, &["XCLAIM", "events", "workers", "bob", "0", "1-0"]),
        Frame::Array(entries) if entries.len() == 1
    ));
    assert!(matches!(
        apply_command(&db, &["XACK", "events", "workers", "1-0"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["XAUTOCLAIM", "events", "workers", "bob", "0", "0-0"]),
        Frame::Array(items) if items.len() == 3
    ));
    assert!(matches!(
        apply_command(&db, &["XINFO", "GROUPS", "events"]),
        Frame::Array(groups) if groups.len() == 1
    ));
    assert!(matches!(
        apply_command(&db, &["XDEL", "events", "1-0"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["XTRIM", "events", "MAXLEN", "0"]),
        Frame::Integer(1)
    ));
}

#[test]
fn stream_consumer_group_lifecycle_matches_redis_visibility_semantics() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["XGROUP", "CREATE", "jobs", "workers", "0-0", "MKSTREAM"]),
        Frame::SimpleString(value) if value == "OK"
    ));
    assert!(matches!(
        apply_command(
            &db,
            &["XGROUP", "CREATECONSUMER", "jobs", "workers", "idle"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(
            &db,
            &["XGROUP", "CREATECONSUMER", "jobs", "workers", "idle"]
        ),
        Frame::Integer(0)
    ));

    let consumers = apply_command(&db, &["XINFO", "CONSUMERS", "jobs", "workers"]);
    let Frame::Array(consumers) = consumers else {
        panic!("unexpected XINFO CONSUMERS frame");
    };
    assert_eq!(consumers.len(), 1);
    let Frame::Array(idle_consumer) = &consumers[0] else {
        panic!("unexpected consumer frame");
    };
    assert!(matches!(&idle_consumer[1], Frame::BulkString(value) if value == b"idle"));
    assert!(matches!(&idle_consumer[3], Frame::Integer(0)));

    let groups = apply_command(&db, &["XINFO", "GROUPS", "jobs"]);
    let Frame::Array(groups) = groups else {
        panic!("unexpected XINFO GROUPS frame");
    };
    assert_eq!(groups.len(), 1);
    let Frame::Array(group) = &groups[0] else {
        panic!("unexpected group frame");
    };
    assert!(matches!(&group[1], Frame::BulkString(value) if value == b"workers"));
    assert!(matches!(&group[3], Frame::Integer(1)));
    assert!(matches!(&group[5], Frame::Integer(0)));

    assert!(matches!(
        apply_command(&db, &["XADD", "jobs", "1-0", "task", "pack"]),
        Frame::BulkString(value) if value == b"1-0"
    ));
    assert!(matches!(
        apply_command(
            &db,
            &[
                "XREADGROUP",
                "GROUP",
                "workers",
                "active",
                "STREAMS",
                "jobs",
                ">"
            ],
        ),
        Frame::Array(streams) if streams.len() == 1
    ));

    let pending = apply_command(
        &db,
        &["XPENDING", "jobs", "workers", "-", "+", "10", "active"],
    );
    let Frame::Array(pending_entries) = pending else {
        panic!("unexpected XPENDING range frame");
    };
    assert_eq!(pending_entries.len(), 1);
    let Frame::Array(pending_entry) = &pending_entries[0] else {
        panic!("unexpected pending entry frame");
    };
    assert!(matches!(&pending_entry[0], Frame::BulkString(value) if value == b"1-0"));
    assert!(matches!(&pending_entry[1], Frame::BulkString(value) if value == b"active"));

    assert!(matches!(
        apply_command(&db, &["XGROUP", "DELCONSUMER", "jobs", "workers", "idle"]),
        Frame::Integer(0)
    ));
    let consumers = apply_command(&db, &["XINFO", "CONSUMERS", "jobs", "workers"]);
    let Frame::Array(consumers) = consumers else {
        panic!("unexpected XINFO CONSUMERS frame after idle delete");
    };
    assert_eq!(consumers.len(), 1);
    let Frame::Array(active_consumer) = &consumers[0] else {
        panic!("unexpected active consumer frame");
    };
    assert!(matches!(&active_consumer[1], Frame::BulkString(value) if value == b"active"));
    assert!(matches!(&active_consumer[3], Frame::Integer(1)));

    assert!(matches!(
        apply_command(&db, &["XGROUP", "DELCONSUMER", "jobs", "workers", "active"]),
        Frame::Integer(1)
    ));
    let summary = apply_command(&db, &["XPENDING", "jobs", "workers"]);
    let Frame::Array(summary) = summary else {
        panic!("unexpected XPENDING summary frame");
    };
    assert!(matches!(&summary[0], Frame::Integer(0)));

    assert!(matches!(
        apply_command(&db, &["XGROUP", "DESTROY", "jobs", "workers"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["XINFO", "GROUPS", "jobs"]),
        Frame::Array(groups) if groups.is_empty()
    ));
}

#[tokio::test]
async fn concurrent_stream_xadd_with_unique_ids_keeps_all_entries_ordered() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for i in 0..32 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("{}-0", i + 1);
            let value = format!("v{}", i);
            let frame =
                apply_command_async(&db, &["XADD", "concurrent-stream", &id, "field", &value])
                    .await;
            assert!(matches!(frame, Frame::BulkString(returned) if returned == id.as_bytes()));
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert!(matches!(
        apply_command_async(&db, &["XLEN", "concurrent-stream"]).await,
        Frame::Integer(32)
    ));
    let range = apply_command_async(&db, &["XRANGE", "concurrent-stream", "-", "+"]).await;
    let Frame::Array(entries) = range else {
        panic!("unexpected XRANGE frame after concurrent XADD");
    };
    assert_eq!(entries.len(), 32);
    assert!(
        matches!(&entries[0], Frame::Array(entry) if matches!(&entry[0], Frame::BulkString(id) if id == b"1-0"))
    );
    assert!(
        matches!(&entries[31], Frame::Array(entry) if matches!(&entry[0], Frame::BulkString(id) if id == b"32-0"))
    );
}

#[test]
fn mset_mget_and_strlen_share_same_kv_backing() {
    let db = test_db();

    let mset_frame = apply_command(&db, &["mset", "k1", "ab", "k2", "xyz"]);
    assert!(matches!(mset_frame, Frame::Ok));

    let strlen_frame = apply_command(&db, &["strlen", "k2"]);
    assert!(matches!(strlen_frame, Frame::Integer(3)));

    let mget_frame = apply_command(&db, &["mget", "k1", "k2", "missing"]);
    match mget_frame {
        Frame::Array(values) => {
            assert_eq!(values.len(), 3);
            assert!(matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"ab"));
            assert!(matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"xyz"));
            assert!(matches!(&values[2], Frame::Null));
        }
        other => panic!("unexpected frame: {}", other),
    }
}

#[tokio::test]
async fn string_async_wrappers_cover_numeric_range_and_lcs_edges() {
    let db = test_db();

    assert!(Command::parse_from_frame(frame_args(&["incrby", "counter"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["incrby", "counter", "nan"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["setrange", "blob", "-1", "x"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["getrange", "blob", "bad", "1"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["lcs", "a", "b", "badopt"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["lcs", "a", "b", "minmatchlen"])).is_err());

    assert!(matches!(
        apply_command_async(&db, &["incrby", "counter", "41"]).await,
        Frame::Integer(41)
    ));
    assert!(matches!(
        apply_command_async(&db, &["incrby", "counter", "1"]).await,
        Frame::Integer(42)
    ));
    assert!(matches!(
        apply_command_async(&db, &["set", "counter", "not-int"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_command_async(&db, &["incrby", "counter", "1"]).await,
        Frame::Error(message) if message.contains("integer")
    ));

    assert!(matches!(
        apply_command_async(&db, &["incrbyfloat", "float", "1.25"]).await,
        Frame::BulkString(value) if value == b"1.25"
    ));
    assert!(matches!(
        apply_command_async(&db, &["incrbyfloat", "float", "0.75"]).await,
        Frame::BulkString(value) if value == b"2"
    ));
    assert!(matches!(
        apply_command_async(&db, &["set", "float", "not-float"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_command_async(&db, &["incrbyfloat", "float", "1"]).await,
        Frame::Error(message) if message.contains("float")
    ));

    assert!(matches!(
        apply_command_async(&db, &["setrange", "blob", "2", "XYZ"]).await,
        Frame::Integer(5)
    ));
    assert!(matches!(
        apply_command_async(&db, &["getrange", "blob", "0", "-1"]).await,
        Frame::BulkString(value) if value == b"\0\0XYZ"
    ));
    assert!(matches!(
        apply_command_async(&db, &["getrange", "blob", "4", "2"]).await,
        Frame::BulkString(value) if value.is_empty()
    ));
    assert!(matches!(
        apply_command_async(&db, &["getrange", "missing", "0", "-1"]).await,
        Frame::BulkString(value) if value.is_empty()
    ));

    assert!(matches!(
        apply_command_async(&db, &["set", "lcs-a", "abcdef"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_command_async(&db, &["set", "lcs-b", "azced"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_command_async(&db, &["lcs", "lcs-a", "lcs-b"]).await,
        Frame::BulkString(value) if value.len() == 3
    ));
    assert!(matches!(
        apply_command_async(&db, &["lcs", "lcs-a", "lcs-b", "len"]).await,
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command_async(&db, &["lcs", "lcs-a", "lcs-b", "idx", "minmatchlen", "2"]).await,
        Frame::Array(values) if values.len() == 4
    ));
}

#[test]
fn incr_and_decr_commands_update_same_numeric_value() {
    let db = test_db();
    apply_command(&db, &["set", "counter", "10"]);

    let incr_frame = apply_command(&db, &["incr", "counter"]);
    assert!(matches!(incr_frame, Frame::Integer(11)));

    let decr_frame = apply_command(&db, &["decr", "counter"]);
    assert!(matches!(decr_frame, Frame::Integer(10)));

    let decrby_frame = apply_command(&db, &["decrby", "counter", "3"]);
    assert!(matches!(decrby_frame, Frame::Integer(7)));

    let incrbyfloat_frame = apply_command(&db, &["incrbyfloat", "counter", "0.5"]);
    assert!(matches!(incrbyfloat_frame, Frame::BulkString(value) if value.as_slice() == b"7.5"));

    assert!(matches!(
        db.get("counter"),
        Some(Structure::String(value)) if value == "7.5"
    ));
}

#[test]
fn integer_string_updates_preserve_ttl_and_report_overflow() {
    let db = test_db();
    apply_command(&db, &["set", "counter", "9223372036854775807"]);
    apply_command(&db, &["expire", "counter", "60"]);

    let overflow = apply_command(&db, &["incr", "counter"]);
    assert!(matches!(overflow, Frame::Error(message) if message.contains("overflow")));

    let decr = apply_command(&db, &["decr", "counter"]);
    assert!(matches!(decr, Frame::Integer(9223372036854775806)));
    assert!(db.ttl_millis("counter") > 0);
}

#[test]
fn integer_string_updates_reject_non_integer_and_wrong_type() {
    let db = test_db();
    apply_command(&db, &["set", "bad-number", "1.5"]);

    let non_integer = apply_command(&db, &["incr", "bad-number"]);
    assert!(matches!(non_integer, Frame::Error(message) if message.contains("not an integer")));

    apply_command(&db, &["lpush", "list", "1"]);
    let wrong_type = apply_command(&db, &["incr", "list"]);
    assert!(matches!(wrong_type, Frame::Error(message) if message.contains("wrong kind")));
}

#[test]
fn setnx_setex_and_getdel_follow_string_semantics() {
    let db = test_db();

    let setnx_created = apply_command(&db, &["setnx", "lock", "token1"]);
    assert!(matches!(setnx_created, Frame::Integer(1)));

    let setnx_existing = apply_command(&db, &["setnx", "lock", "token2"]);
    assert!(matches!(setnx_existing, Frame::Integer(0)));
    assert!(matches!(
        db.get("lock"),
        Some(Structure::String(value)) if value == "token1"
    ));

    let setex_frame = apply_command(&db, &["setex", "session", "1", "payload"]);
    assert!(matches!(setex_frame, Frame::Ok));
    let ttl_frame = apply_command(&db, &["ttl", "session"]);
    assert!(matches!(ttl_frame, Frame::Integer(value) if (0..=1).contains(&value)));

    let getdel_frame = apply_command(&db, &["getdel", "lock"]);
    assert!(matches!(getdel_frame, Frame::BulkString(value) if value.as_slice() == b"token1"));
    assert!(db.get("lock").is_none());

    let missing_getdel = apply_command(&db, &["getdel", "lock"]);
    assert!(matches!(missing_getdel, Frame::Null));
}
