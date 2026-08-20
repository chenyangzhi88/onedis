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
        apply(&db, &["MSETEX", "2", "m4", "v4", "m5", "v5", "EX", "2"]),
        Frame::Integer(1)
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
    assert!(contains_bulk(
        &apply(&db, &["HGETDEL", "h", "FIELDS", "1", "a"]),
        "1"
    ));

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
    assert!(matches!(
        apply(&db, &["RPUSH", "list4", "a", "b", "c", "d"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(&db, &["LPOP", "list4", "2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["RPOP", "list4", "2"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(&db, &["LPOP", "missing-list", "2"]),
        Frame::Array(values) if values.is_empty()
    ));

    db.insert("wrong".to_string(), Structure::String("value".to_string())).unwrap();
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
    assert!(parse_err(&["SCAN", "0", "COUNT", "0"]).contains("syntax"));
    assert!(parse_err(&["HSCAN", "h", "0", "COUNT"]).contains("syntax"));
    assert!(parse_err(&["HSCAN", "h", "0", "COUNT", "0"]).contains("syntax"));
    assert!(parse_err(&["BLMPOP", "-1", "1", "list", "LEFT"]).contains("negative"));
    assert!(parse_err(&["MSET", "only-key"]).contains("wrong"));
}

#[tokio::test]
async fn hash_commands_preserve_binary_values_ttls_and_bounded_scan_semantics() {
    let db = test_db("command-semantics-hash-regressions");
    let binary = vec![0, 0xff, b'\r', b'\n', 1, 2, 3];
    let hset = Frame::Array(vec![
        Frame::bulk_string("HSET"),
        Frame::bulk_string("binary-hash"),
        Frame::bulk_string("payload"),
        Frame::BulkString(binary.clone()),
    ]);
    let command = Command::parse_from_frame(hset).unwrap();
    assert!(matches!(
        onedis_server::command_dispatch::handle_command(&db, command).unwrap(),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["HGET", "binary-hash", "payload"]),
        Frame::BulkString(value) if value == binary
    ));
    assert!(matches!(
        apply(&db, &["HSTRLEN", "binary-hash", "payload"]),
        Frame::Integer(7)
    ));
    assert!(matches!(
        apply(&db, &["HVALS", "binary-hash"]),
        Frame::Array(values)
            if values.iter().any(|value| matches!(value, Frame::BulkString(bytes) if bytes == &binary))
    ));
    assert!(matches!(
        apply(&db, &["HGETALL", "binary-hash"]),
        Frame::Array(values)
            if values.iter().any(|value| matches!(value, Frame::BulkString(bytes) if bytes == &binary))
    ));
    let hsetex = Frame::Array(vec![
        Frame::bulk_string("HSETEX"),
        Frame::bulk_string("binary-expiring-hash"),
        Frame::bulk_string("PX"),
        Frame::bulk_string("60000"),
        Frame::bulk_string("FIELDS"),
        Frame::bulk_string("1"),
        Frame::bulk_string("payload"),
        Frame::BulkString(binary.clone()),
    ]);
    let command = Command::parse_from_frame(hsetex).unwrap();
    assert!(matches!(
        onedis_server::command_dispatch::handle_command_async(&db, command)
            .await
            .unwrap(),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HGETDEL",
                "binary-expiring-hash",
                "FIELDS",
                "1",
                "payload"
            ]
        ),
        Frame::Array(values)
            if matches!(values.as_slice(), [Frame::BulkString(value)] if value == &binary)
    ));

    assert!(matches!(
        apply(
            &db,
            &["HSET", "ttl-hash", "integer", "1", "float", "1.5"]
        ),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HPEXPIRE",
                "ttl-hash",
                "60000",
                "FIELDS",
                "2",
                "integer",
                "float"
            ]
        ),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["HINCRBY", "ttl-hash", "integer", "2"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_async(&db, &["HINCRBYFLOAT", "ttl-hash", "float", "0.5"]).await,
        Frame::BulkString(value) if value == b"2"
    ));
    let ttls = array(apply(
        &db,
        &[
            "HPTTL",
            "ttl-hash",
            "FIELDS",
            "2",
            "integer",
            "float",
        ],
    ));
    assert!(
        ttls.iter()
            .all(|ttl| matches!(ttl, Frame::Integer(value) if *value > 0))
    );

    assert!(matches!(
        apply(
            &db,
            &[
                "HSET",
                "scan-hash",
                "first",
                "value-1",
                "second",
                "value-2"
            ]
        ),
        Frame::Integer(2)
    ));
    let scan = array(apply(
        &db,
        &["HSCAN", "scan-hash", "0", "COUNT", "10", "NOVALUES"],
    ));
    let fields = array(scan[1].clone());
    assert_eq!(fields.len(), 2);
    assert!(fields.iter().all(
        |field| matches!(field, Frame::BulkString(value) if value == b"first" || value == b"second")
    ));
    let malformed_pattern = apply(&db, &["HSCAN", "scan-hash", "0", "MATCH", "["]);
    assert!(matches!(
        malformed_pattern,
        Frame::Array(values) if matches!(&values[1], Frame::Array(fields) if fields.is_empty())
    ));

    assert!(parse_err(&["HSCAN", "scan-hash", "bad"]).contains("invalid cursor"));
    assert!(
        parse_err(&["HSCAN", "scan-hash", "0", "COUNT", "500001"])
            .contains("configured response limit")
    );
    assert!(
        parse_err(&["HRANDFIELD", "scan-hash", "-9223372036854775808"])
            .contains("configured response limit")
    );
    assert!(
        parse_err(&[
            "HSETEX",
            "ttl-hash",
            "EX",
            "9223372036854775807",
            "FIELDS",
            "1",
            "new",
            "value",
        ])
        .contains("invalid expire")
    );

    assert!(matches!(
        apply(&db, &["HSET", "delayed-expire", "field", "value"]),
        Frame::Integer(1)
    ));
    let delayed = parse(&[
        "HPEXPIRE",
        "delayed-expire",
        "50",
        "FIELDS",
        "1",
        "field",
    ]);
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(matches!(
        onedis_server::command_dispatch::handle_command(&db, delayed).unwrap(),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(1)])
    ));
    assert_eq!(
        bulk(apply(&db, &["HGET", "delayed-expire", "field"])),
        "value"
    );

    assert!(matches!(
        apply(&db, &["HPEXPIRE", "delayed-expire", "1", "FIELDS", "1", "field"]),
        Frame::Array(_)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(matches!(
        apply(&db, &["HSET", "delayed-expire", "field", "new"]),
        Frame::Integer(1)
    ));

    assert!(parse_err(&["HPEXPIRE", "ttl-hash", "-1", "FIELDS", "1", "integer"])
        .contains("invalid expire"));

    assert!(matches!(
        apply(&db, &["HSET", "duplicate-getdel", "field", "value"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HGETDEL",
                "duplicate-getdel",
                "FIELDS",
                "2",
                "field",
                "field",
            ],
        ),
        Frame::Array(values)
            if matches!(
                values.as_slice(),
                [Frame::BulkString(value), Frame::Null] if value == b"value"
            )
    ));

    assert!(matches!(
        apply(&db, &["HSET", "duplicate-expire", "field", "value"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HPEXPIRE",
                "duplicate-expire",
                "60000",
                "NX",
                "FIELDS",
                "2",
                "field",
                "field",
            ],
        ),
        Frame::Array(values)
            if matches!(
                values.as_slice(),
                [Frame::Integer(1), Frame::Integer(0)]
            )
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HPEXPIREAT",
                "duplicate-expire",
                "1",
                "NX",
                "FIELDS",
                "1",
                "field",
            ],
        ),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(0)])
    ));
    assert_eq!(
        bulk(apply(&db, &["HGET", "duplicate-expire", "field"])),
        "value"
    );
    assert!(matches!(
        apply(
            &db,
            &[
                "HPERSIST",
                "duplicate-expire",
                "FIELDS",
                "2",
                "field",
                "field",
            ],
        ),
        Frame::Array(values)
            if matches!(
                values.as_slice(),
                [Frame::Integer(1), Frame::Integer(-1)]
            )
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HPEXPIRE",
                "duplicate-expire",
                "0",
                "FIELDS",
                "2",
                "field",
                "field",
            ],
        ),
        Frame::Array(values)
            if matches!(
                values.as_slice(),
                [Frame::Integer(2), Frame::Integer(-2)]
            )
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "duplicate-expire"]),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(&db, &["HSET", "past-getex", "field", "value"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HGETEX",
                "past-getex",
                "PXAT",
                "1",
                "FIELDS",
                "2",
                "field",
                "field",
            ],
        )
        .await,
        Frame::Array(values)
            if matches!(
                values.as_slice(),
                [Frame::BulkString(value), Frame::Null] if value == b"value"
            )
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "past-getex"]),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(&db, &["HSET", "past-setex", "keep", "value"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HSETEX",
                "past-setex",
                "PX",
                "0",
                "FIELDS",
                "1",
                "drop",
                "value",
            ],
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["HEXISTS", "past-setex", "drop"]),
        Frame::Integer(0)
    ));
    assert_eq!(bulk(apply(&db, &["HGET", "past-setex", "keep"])), "value");
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HSETEX",
                "past-setex",
                "PXAT",
                "1",
                "FIELDS",
                "1",
                "keep",
                "replacement",
            ],
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "past-setex"]),
        Frame::Integer(0)
    ));
}
