#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_commands_cover_groups_pending_ranges_claims_and_error_paths() {
    let db = test_db("command-semantics-stream");

    let id1 = bulk(apply(&db, &["XADD", "events", "1-0", "type", "created"]));
    let id2 = bulk(apply(&db, &["XADD", "events", "2-0", "type", "updated"]));
    assert_eq!(id1, "1-0");
    assert_eq!(id2, "2-0");
    assert!(matches!(apply(&db, &["XLEN", "events"]), Frame::Integer(2)));
    assert!(contains_bulk(
        &apply(&db, &["XRANGE", "events", "-", "+"]),
        "created"
    ));
    assert!(contains_bulk(
        &apply(&db, &["XREVRANGE", "events", "+", "-", "COUNT", "1"]),
        "2-0"
    ));
    assert!(contains_bulk(
        &apply(&db, &["XREAD", "COUNT", "2", "STREAMS", "events", "0-0"]),
        "events"
    ));

    assert!(
        matches!(apply(&db, &["XGROUP", "CREATE", "events", "g", "0-0"]), Frame::SimpleString(ok) if ok == "OK")
    );
    assert!(matches!(
        apply(&db, &["XGROUP", "CREATECONSUMER", "events", "g", "c1"]),
        Frame::Integer(_)
    ));
    let read_group = apply_async(
        &db,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "events",
            ">",
        ],
    )
    .await;
    assert!(contains_bulk(&read_group, "1-0"));
    assert!(matches!(
        apply(&db, &["XPENDING", "events", "g"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["XPENDING", "events", "g", "-", "+", "10", "c1"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["XCLAIM", "events", "g", "c2", "0", "1-0"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(
            &db,
            &["XAUTOCLAIM", "events", "g", "c3", "0", "0-0", "COUNT", "10"]
        ),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["XACK", "events", "g", "1-0", "2-0"]).await,
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(
            &db,
            &["XACKDEL", "events", "g", "IDS", "1", "1-0"]
        ),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(1)])
    ));
    assert!(matches!(
        apply(&db, &["XDELEX", "events", "IDS", "1", "2-0"]),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(1)])
    ));
    assert!(matches!(
        apply(&db, &["XDELEX", "events", "IDS", "1", "2-0"]),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(-1)])
    ));
    assert!(matches!(
        apply(
            &db,
            &["XACKDEL", "missing-stream", "g", "IDS", "1", "1-0"]
        ),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(-1)])
    ));

    assert!(matches!(
        apply(&db, &["XADD", "events", "3-0", "type", "again"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["XSETID", "events", "3-0"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["XTRIM", "events", "MAXLEN", "1"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(&db, &["XINFO", "STREAM", "events"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["XINFO", "GROUPS", "events"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["XINFO", "CONSUMERS", "events", "g"]),
        Frame::Array(_)
    ));
    assert!(
        matches!(apply(&db, &["XGROUP", "SETID", "events", "g", "$"]), Frame::SimpleString(ok) if ok == "OK")
    );
    assert!(matches!(
        apply(&db, &["XGROUP", "DELCONSUMER", "events", "g", "c1"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(&db, &["XGROUP", "DESTROY", "events", "g"]),
        Frame::Integer(1)
    ));

    assert!(parse_err(&["XGROUP", "NOPE"]).contains("unknown"));
    assert!(parse_err(&["XPENDING", "events", "g", "bad", "+", "10"]).contains("Invalid"));
    assert!(parse_err(&["XREADGROUP", "GROUP", "g"]).contains("syntax"));
    assert!(parse_err(&["XINFO", "BOGUS", "events"]).contains("syntax"));
}
