#[tokio::test]
async fn hll_commands_use_redis_compatible_bounded_storage() {
    let db = test_db("command-semantics-hll");

    assert!(matches!(
        apply(&db, &["PFCOUNT", "missing"]),
        Frame::Integer(0)
    ));
    assert!(matches!(apply(&db, &["PFADD", "empty"]), Frame::Integer(1)));
    assert!(matches!(apply(&db, &["PFADD", "empty"]), Frame::Integer(0)));
    assert!(matches!(
        apply(&db, &["PFCOUNT", "empty"]),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(&db, &["PFADD", "hll", "a", "b", "a"]),
        Frame::Integer(1)
    ));
    assert!(matches!(apply(&db, &["PFCOUNT", "hll"]), Frame::Integer(2)));
    let encoded = apply(&db, &["GET", "hll"]);
    assert!(
        matches!(encoded, Frame::BulkString(bytes) if bytes.len() <= 12_304
            && bytes.starts_with(b"HYLL")
            && matches!(bytes[4], 0 | 1))
    );

    assert!(matches!(
        apply(&db, &["PEXPIRE", "hll", "60000"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(&db, &["PFADD", "hll", "c"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["PTTL", "hll"]),
        Frame::Integer(ttl) if ttl > 0
    ));

    assert!(matches!(
        apply_async(&db, &["PFADD", "other", "d"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(&db, &["PFMERGE", "hll", "other"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["PFCOUNT", "hll"]).await,
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(&db, &["PFMERGE", "created-empty"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["PFCOUNT", "created-empty"]),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(&db, &["SET", "invalid-hll", "plain"]),
        Frame::Ok
    ));
    assert!(
        matches!(apply(&db, &["PFCOUNT", "invalid-hll"]), Frame::Error(message)
            if message.contains("valid HyperLogLog"))
    );
    assert!(matches!(
        apply(&db, &["SADD", "wrong-hll-type", "member"]),
        Frame::Integer(1)
    ));
    assert!(
        matches!(apply(&db, &["PFADD", "wrong-hll-type", "x"]), Frame::Error(message)
            if message.contains("WRONGTYPE"))
    );

    let binary_element = Frame::Array(vec![
        Frame::bulk_string("PFADD".to_string()),
        Frame::bulk_string("binary-hll".to_string()),
        Frame::BulkString(vec![0xff, 0x00]),
    ]);
    let command = Command::parse_from_frame(binary_element).unwrap();
    assert!(matches!(
        onedis_server::command_dispatch::handle_command(&db, command).unwrap(),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["PFCOUNT", "binary-hll"]),
        Frame::Integer(1)
    ));

    let binary_key = Frame::Array(vec![
        Frame::bulk_string("PFCOUNT".to_string()),
        Frame::BulkString(vec![0xff]),
    ]);
    assert!(Command::parse_from_frame(binary_key).is_err());
}
