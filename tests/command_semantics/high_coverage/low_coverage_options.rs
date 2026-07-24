#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn low_coverage_wrappers_cover_special_options_and_error_edges() {
    let db = test_db("command-semantics-special-options");

    assert!(matches!(
        apply(&db, &["BITFIELD", "bf", "SET", "u8", "0", "7"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["BITFIELD", "bf", "GET", "u8", "0", "INCRBY", "i8", "0", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["BITFIELD", "bf", "SET", "u8", "8", "3"]).await,
        Frame::Array(_)
    ));
    assert!(
        parse_err(&["BITFIELD_RO", "bf", "SET", "u8", "0", "1"]).contains("only supports GET")
    );
    assert!(parse_err(&["BITFIELD_RO"]).contains("wrong"));
    assert!(parse_err(&["BITFIELD", "bf", "SET", "x8", "0", "1"]).contains("invalid"));
    assert!(parse_err(&["BITFIELD", "bf", "GET", "u8"]).contains("syntax"));
    assert!(parse_err(&["BITFIELD", "bf", "NOOP"]).contains("syntax"));
    assert!(matches!(
        apply(
            &db,
            &[
                "BITFIELD",
                "overflow",
                "SET",
                "i8",
                "0",
                "127",
                "OVERFLOW",
                "SAT",
                "INCRBY",
                "i8",
                "0",
                "1"
            ]
        ),
        Frame::Array(values) if matches!(values.last(), Some(Frame::Integer(127)))
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "BITFIELD",
                "overflow",
                "OVERFLOW",
                "FAIL",
                "INCRBY",
                "i8",
                "0",
                "1"
            ]
        ),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Null])
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "BITFIELD",
                "failed-write",
                "OVERFLOW",
                "FAIL",
                "SET",
                "u8",
                "0",
                "256",
            ],
        ),
        Frame::Array(values) if matches!(values.as_slice(), [Frame::Null])
    ));
    assert!(matches!(
        apply(&db, &["EXISTS", "failed-write"]),
        Frame::Integer(0)
    ));
    assert!(parse_err(&["BITFIELD", "bf", "OVERFLOW", "BAD"]).contains("syntax"));
    assert!(parse_err(&["BITFIELD", "bf", "GET", "u64", "0"]).contains("invalid"));
    assert!(parse_err(&[
        "BITFIELD",
        "bf",
        "GET",
        "u8",
        "#18446744073709551615"
    ])
    .contains("range"));
    assert!(matches!(
        apply(&db, &["SET", "bit-unit", "x"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["BITCOUNT", "bit-unit", "1", "4", "BIT"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(&db, &["BITPOS", "bit-unit", "0", "0", "7", "BIT"]),
        Frame::Integer(0)
    ));

    assert!(matches!(apply(&db, &["SET", "ttl-key", "v"]), Frame::Ok));
    assert_eq!(bulk(apply(&db, &["GETEX", "ttl-key"])), "v");
    assert_eq!(bulk(apply(&db, &["GETEX", "ttl-key", "EX", "60"])), "v");
    assert_eq!(bulk(apply(&db, &["GETEX", "ttl-key", "PX", "60000"])), "v");
    assert_eq!(
        bulk(apply(&db, &["GETEX", "ttl-key", "EXAT", "4102444800"])),
        "v"
    );
    assert_eq!(
        bulk(apply_async(&db, &["GETEX", "ttl-key", "PXAT", "4102444800000"]).await),
        "v"
    );
    assert!(matches!(apply(&db, &["GETEX", "missing"]), Frame::Null));
    assert!(parse_err(&["GETEX"]).contains("wrong"));
    assert!(parse_err(&["GETEX", "k", "EX", "0"]).contains("invalid"));
    assert!(parse_err(&["GETEX", "k", "PERSIST", "1"]).contains("syntax"));
    assert!(parse_err(&["GETEX", "k", "PX", "bad"]).contains("invalid"));

    assert!(matches!(
        apply(&db, &["MSETEX", "2", "mx1", "1", "mx2", "2", "PX", "60000"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(
            &db,
            &["MSETEX", "2", "mx3", "3", "mx3b", "3b", "EXAT", "4102444800"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["MSETEX", "2", "mx4", "4", "mx4b", "4b", "PXAT", "4102444800000"]
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(parse_err(&["MSETEX"]).contains("wrong"));
    assert!(parse_err(&["MSETEX", "2", "k", "v", "k2", "v2", "EX", "0"]).contains("invalid"));
    assert!(parse_err(&["MSETEX", "2", "k", "v", "k2", "v2", "BAD"]).contains("syntax"));
    assert!(parse_err(&["MSETEX", "2", "k", "v"]).contains("wrong"));
    assert!(matches!(
        apply(&db, &["SET", "mx-guard", "old"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "MSETEX",
                "2",
                "mx-new",
                "new",
                "mx-guard",
                "changed",
                "PX",
                "60000",
                "NX"
            ]
        ),
        Frame::Integer(0)
    ));
    assert!(matches!(apply(&db, &["GET", "mx-new"]), Frame::Null));
    assert_eq!(bulk(apply(&db, &["GET", "mx-guard"])), "old");
    assert!(matches!(
        apply(
            &db,
            &["MSETEX", "2", "mx-guard", "changed", "mx-missing", "new", "XX"]
        ),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(
            &db,
            &["HSETEX", "hh", "FNX", "EX", "60", "FIELDS", "1", "a", "1"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "HSETEX", "hh", "FNX", "PX", "60000", "FIELDS", "1", "a", "2"
            ]
        ),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(
            &db,
            &["HSETEX", "hh", "FXX", "KEEPTTL", "FIELDS", "1", "a", "3"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HSETEX",
                "hh",
                "PXAT",
                "4102444800000",
                "FIELDS",
                "1",
                "b",
                "4"
            ]
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(parse_err(&["HSETEX", "hh"]).contains("wrong"));
    assert!(parse_err(&["HSETEX", "hh", "FNX", "FXX", "FIELDS", "1", "a", "1"]).contains("syntax"));
    assert!(parse_err(&["HSETEX", "hh", "EX", "0", "FIELDS", "1", "a", "1"]).contains("invalid"));
    assert!(parse_err(&["HSETEX", "hh", "FIELDS", "2", "a", "1"]).contains("syntax"));
    assert!(parse_err(&["HGETEX", "hh", "FIELDS", "bad", "a"]).contains("integer"));
    assert!(parse_err(&["HGETDEL", "hh", "a"]).contains("syntax"));
    assert!(parse_err(&["HSETEX", "hh", "KEEPTTL", "EX", "1", "FIELDS", "1", "a", "1"])
        .contains("syntax"));
    assert!(
        parse_err(&["HEXPIRE", "hh", "60", "NX", "XX", "FIELDS", "1", "a"])
            .contains("not compatible")
    );

    assert!(matches!(
        apply(&db, &["ZADD", "zr", "1", "a", "2", "b", "3", "c"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(&db, &["ZADD", "zr", "NX", "4", "a", "4", "d"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["ZADD", "zr", "XX", "GT", "CH", "5", "a", "1", "b"]),
        Frame::Integer(1)
    ));
    assert_eq!(
        bulk(apply(&db, &["ZADD", "zr", "XX", "INCR", "2", "a"])),
        "7"
    );
    assert!(matches!(
        apply(&db, &["ZADD", "zr", "XX", "INCR", "1", "missing"]),
        Frame::Null
    ));
    assert!(parse_err(&["ZADD", "zr", "NX", "XX", "1", "a"]).contains("syntax"));
    assert!(parse_err(&["ZADD", "zr", "INCR", "1", "a", "2", "b"]).contains("single"));
    assert!(matches!(
        apply(&db, &["ZRANDMEMBER", "zr"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["ZRANDMEMBER", "zr", "WITHSCORES"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["ZRANDMEMBER", "zr", "-4", "WITHSCORES"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply(&db, &["ZRANDMEMBER", "missing"]),
        Frame::Null
    ));
    assert!(matches!(
        apply(&db, &["ZRANDMEMBER", "missing", "2"]),
        Frame::Array(values) if values.is_empty()
    ));
    assert!(parse_err(&["ZRANDMEMBER"]).contains("wrong"));
    assert!(parse_err(&["ZRANDMEMBER", "zr", "bad"]).contains("integer"));
    assert!(parse_err(&["ZRANDMEMBER", "zr", "1", "BAD"]).contains("syntax"));
    assert!(matches!(
        apply(&db, &["ZRANGE", "zr", "3", "(1", "BYSCORE", "REV"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply(
            &db,
            &["ZRANGESTORE", "zr-copy", "zr", "7", "1", "BYSCORE", "REV"]
        ),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "GEOADD",
                "geo-duplicates",
                "1",
                "1",
                "same",
                "2",
                "2",
                "same"
            ]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["SET", "geo-wrong-type", "value"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["GEOPOS", "geo-wrong-type", "member"]),
        Frame::Error(message) if message.contains("WRONGTYPE")
    ));
    assert!(parse_err(&["GEODIST", "geo-duplicates", "a", "b", "BAD"]).contains("unsupported"));
    assert!(parse_err(&[
        "GEOSEARCHSTORE",
        "dest",
        "geo-duplicates",
        "FROMLONLAT",
        "1",
        "1",
        "BYRADIUS",
        "1",
        "m",
        "WITHDIST"
    ])
    .contains("syntax"));

    assert!(matches!(
        apply(&db, &["XGROUP", "CREATE", "s-mk", "g", "$", "MKSTREAM"]),
        Frame::SimpleString(ok) if ok == "OK"
    ));
    assert!(matches!(
        apply_async(&db, &["XGROUP", "SETID", "s-mk", "g", "0-0"]).await,
        Frame::SimpleString(ok) if ok == "OK"
    ));
    assert!(matches!(
        apply_async(&db, &["XGROUP", "CREATECONSUMER", "s-mk", "g", "c1"]).await,
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply(&db, &["XGROUP", "DELCONSUMER", "s-mk", "g", "c1"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        apply_async(&db, &["XGROUP", "DESTROY", "s-mk", "g"]).await,
        Frame::Integer(1)
    ));
    assert!(parse_err(&["XGROUP"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "CREATE", "s", "g"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "SETID", "s", "g"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "DESTROY", "s", "g", "extra"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "CREATECONSUMER", "s", "g"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "DELCONSUMER", "s", "g"]).contains("wrong"));
    assert!(parse_err(&["XGROUP", "CREATE", "s", "g", "bad-id"]).contains("Invalid"));

    assert!(matches!(
        apply(&db, &["XADD", "range", "1-0", "f", "v1"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply(&db, &["XADD", "range", "2-0", "f", "v2"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(&db, &["XRANGE", "range", "1-0", "2-0", "COUNT", "1"]).await,
        Frame::Array(values) if values.len() == 1
    ));
    assert!(matches!(
        apply_async(&db, &["XREVRANGE", "range", "2-0", "1-0", "COUNT", "1"]).await,
        Frame::Array(values) if values.len() == 1
    ));
    assert!(parse_err(&["XRANGE", "range", "-", "+", "COUNT", "bad"]).contains("integer"));
    assert!(parse_err(&["XRANGE", "range", "-", "+", "BAD"]).contains("syntax"));
    assert!(parse_err(&["XREVRANGE", "range", "bad", "-"]).contains("Invalid"));
    assert!(parse_err(&["XTRIM", "range"]).contains("wrong"));
    assert!(parse_err(&["XTRIM", "range", "BAD", "1"]).contains("syntax"));
    assert!(parse_err(&["XSETID", "range"]).contains("wrong"));
    assert!(parse_err(&["XSETID", "range", "bad"]).contains("Invalid"));
    assert!(parse_err(&[
        "XAUTOCLAIM",
        "range",
        "g",
        "c",
        "0",
        "0-0",
        "IGNORED"
    ])
    .contains("syntax"));
    assert!(parse_err(&["XTRIM", "range", "MAXLEN", "1", "IGNORED"]).contains("syntax"));
    assert!(parse_err(&["XSETID", "range", "2-0", "IGNORED"]).contains("wrong"));
    assert!(parse_err(&["DBSIZE", "IGNORED"]).contains("wrong"));
    assert!(parse_err(&["INFO", "all", "IGNORED"]).contains("wrong"));
    assert!(parse_err(&["FLUSHDB", "IGNORED"]).contains("syntax"));
    assert!(parse_err(&["FLUSHALL", "ASYNC", "IGNORED"]).contains("syntax"));
    assert!(parse_err(&["SAVE", "IGNORED"]).contains("wrong"));
    assert!(parse_err(&["BGSAVE", "IGNORED"]).contains("wrong"));

    assert!(matches!(apply(&db, &["SET", "decr-min", "0"]), Frame::Ok));
    assert!(matches!(
        apply(&db, &["DECRBY", "decr-min", "-9223372036854775808"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        apply_async(&db, &["DECRBY", "decr-min-async", "-9223372036854775808"]).await,
        Frame::Error(_)
    ));
    assert!(parse_err(&["DECRBY", "k"]).contains("wrong"));
    assert!(parse_err(&["DECRBY", "k", "bad"]).contains("integer"));

    assert_eq!(
        bulk(apply(&db, &["INCRBYFLOAT", "float-missing", "2.25"])),
        "2.25"
    );
    assert!(matches!(
        apply(&db, &["SET", "float-bad", "not-a-float"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["INCRBYFLOAT", "float-bad", "1.0"]),
        Frame::Error(message) if message.contains("valid float")
    ));
    assert!(matches!(
        apply(&db, &["LPUSH", "float-list", "v"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["INCRBYFLOAT", "float-list", "1.0"]),
        Frame::Error(message) if message.contains("WRONGTYPE")
    ));
    assert_eq!(
        bulk(apply_async(&db, &["INCRBYFLOAT", "float-async", "3.5"]).await),
        "3.5"
    );
    assert!(parse_err(&["INCRBYFLOAT", "k"]).contains("wrong"));
    assert!(parse_err(&["INCRBYFLOAT", "k", "bad"]).contains("valid float"));

    assert!(matches!(
        apply(&db, &["SET", "unlink-sync", "v"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["UNLINK", "unlink-sync", "unlink-missing"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["SET", "unlink-async", "v"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["UNLINK", "unlink-async", "unlink-missing"]).await,
        Frame::Integer(1)
    ));
    assert!(parse_err(&["UNLINK"]).contains("wrong"));
}
