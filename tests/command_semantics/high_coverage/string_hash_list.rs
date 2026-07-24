#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn string_key_hash_and_list_commands_cover_sync_async_and_errors() {
    let db = test_db("command-semantics-core");

    assert!(matches!(apply(&db, &["SET", "s", "10"]), Frame::Ok));
    assert_eq!(bulk(apply(&db, &["GETSET", "s", "11"])), "10");
    assert_eq!(bulk(apply(&db, &["GETRANGE", "s", "0", "0"])), "1");
    assert_eq!(bulk(apply(&db, &["GETRANGE", "missing", "0", "-1"])), "");
    assert!(parse_err(&["GETRANGE", "s", "0", "1", "extra"]).contains("wrong number"));
    assert!(matches!(
        apply(&db, &["SETRANGE", "s", "2", "xy"]),
        Frame::Integer(4)
    ));
    assert_eq!(bulk(apply(&db, &["GET", "s"])), "11xy");
    assert!(matches!(apply(&db, &["STRLEN", "s"]), Frame::Integer(4)));
    assert!(matches!(
        apply(&db, &["APPEND", "s", "z"]),
        Frame::Integer(5)
    ));
    assert!(matches!(
        apply(&db, &["INCRBY", "counter", "4"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(&db, &["DECRBY", "counter", "2"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(&db, &["INCRBYFLOAT", "float", "1.5"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["MSET", "m1", "v1", "m2", "v2"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["MSETNX", "m1", "new", "m3", "v3"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(&db, &["MSETEX", "EX", "2", "m4", "v4", "m5", "v5"]),
        Frame::Ok
    ));
    assert_eq!(array(apply(&db, &["MGET", "m1", "m2", "missing"])).len(), 3);
    assert_eq!(bulk(apply_async(&db, &["GETDEL", "m2"]).await), "v2");
    assert!(matches!(apply(&db, &["GET", "m2"]), Frame::Null));

    assert!(matches!(
        apply(&db, &["EXPIRE", "s", "20"]),
        Frame::Integer(1)
    ));
    assert!(matches!(apply(&db, &["TTL", "s"]), Frame::Integer(ttl) if ttl > 0));
    assert!(matches!(apply(&db, &["PERSIST", "s"]), Frame::Integer(1)));
    assert!(matches!(apply(&db, &["PTTL", "s"]), Frame::Integer(-1)));
    assert!(matches!(
        apply(&db, &["TOUCH", "s", "missing"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["COPY", "s", "s-copy"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["RENAMENX", "s-copy", "s-copy-2"]),
        Frame::Integer(1)
    ));
    assert!(
        matches!(apply(&db, &["TYPE", "s-copy-2"]), Frame::SimpleString(kind) if kind == "string")
    );
    assert!(matches!(apply(&db, &["RANDOMKEY"]), Frame::BulkString(_)));
    let scan = apply_async(&db, &["SCAN", "0", "MATCH", "s*", "COUNT", "2"]).await;
    assert!(contains_bulk(&scan, "s") || contains_bulk(&scan, "s-copy-2"));
    assert!(matches!(
        apply(&db, &["DEL", "s-copy-2", "missing"]),
        Frame::Integer(1)
    ));
    assert!(matches!(apply(&db, &["UNLINK", "s"]), Frame::Integer(1)));

    assert!(matches!(
        apply(&db, &["HSET", "h", "a", "1", "b", "2"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(&db, &["HSETNX", "h", "a", "new"]),
        Frame::Integer(0)
    ));
    assert_eq!(bulk(apply(&db, &["HGET", "h", "a"])), "1");
    assert!(matches!(
        apply(&db, &["HEXISTS", "h", "b"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["HINCRBY", "h", "n", "3"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(&db, &["HINCRBYFLOAT", "h", "f", "1.25"]),
        Frame::BulkString(_)
    ));
    assert_eq!(array(apply(&db, &["HMGET", "h", "a", "missing"])).len(), 2);
    assert!(contains_bulk(&apply(&db, &["HKEYS", "h"]), "a"));
    assert!(contains_bulk(&apply(&db, &["HVALS", "h"]), "2"));
    assert!(contains_bulk(&apply(&db, &["HGETALL", "h"]), "a"));
    assert!(matches!(
        apply(&db, &["HSTRLEN", "h", "a"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["HEXPIRE", "h", "20", "FIELDS", "1", "a"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["HTTL", "h", "FIELDS", "1", "a"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["HPERSIST", "h", "FIELDS", "1", "a"]),
        Frame::Array(_)
    ));
    assert!(contains_bulk(
        &apply(&db, &["HSCAN", "h", "0", "MATCH", "*", "COUNT", "10"]),
        "a"
    ));
    assert!(matches!(
        apply_async(&db, &["HDEL", "h", "b"]).await,
        Frame::Integer(1)
    ));
    assert!(contains_bulk(&apply(&db, &["HGETDEL", "h", "a"]), "1"));

    assert!(matches!(
        apply(&db, &["RPUSH", "list", "a", "b", "c", "b", "d"]),
        Frame::Integer(5)
    ));
    assert_eq!(bulk(apply(&db, &["LINDEX", "list", "1"])), "b");
    assert!(matches!(
        apply(&db, &["LPOS", "list", "b"]),
        Frame::Integer(1)
    ));
    assert_eq!(
        array(apply(
            &db,
            &[
                "LPOS", "list", "b", "RANK", "1", "COUNT", "2", "MAXLEN", "5"
            ]
        ))
        .len(),
        2
    );
    assert!(matches!(
        apply(&db, &["LINSERT", "list", "BEFORE", "c", "x"]),
        Frame::Integer(6)
    ));
    assert!(
        matches!(apply(&db, &["LSET", "list", "0", "z"]), Frame::SimpleString(ok) if ok == "OK")
    );
    assert!(matches!(
        apply(&db, &["LREM", "list", "1", "b"]),
        Frame::Integer(1)
    ));
    assert!(
        matches!(apply(&db, &["LTRIM", "list", "0", "3"]), Frame::SimpleString(ok) if ok == "OK")
    );
    assert!(contains_bulk(
        &apply(&db, &["LRANGE", "list", "0", "-1"]),
        "z"
    ));
    assert!(matches!(
        apply(&db, &["LMOVE", "list", "list2", "LEFT", "RIGHT"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["RPOPLPUSH", "list", "list2"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["BLMPOP", "0", "2", "missing", "list2", "LEFT", "COUNT", "2"]
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["RPUSH", "list3", "left", "right"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(&db, &["LPOP", "list3"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["RPOP", "list3"]),
        Frame::BulkString(_)
    ));

    db.insert("wrong".to_string(), Structure::String("value".to_string()));
    assert!(matches!(
        apply(&db, &["HGET", "wrong", "field"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        apply(&db, &["LPOS", "wrong", "value"]),
        Frame::Error(_)
    ));
    assert!(parse_err(&["LPOS", "list", "a", "RANK", "0"]).contains("RANK"));
    assert!(parse_err(&["SCAN", "0", "MATCH"]).contains("MATCH"));
    assert!(parse_err(&["HSCAN", "h", "0", "COUNT"]).contains("COUNT"));
    assert!(parse_err(&["BLMPOP", "-1", "1", "list", "LEFT"]).contains("negative"));
    assert!(parse_err(&["MSET", "only-key"]).contains("wrong"));
}

