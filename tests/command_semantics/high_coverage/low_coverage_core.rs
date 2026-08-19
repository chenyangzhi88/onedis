#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn low_coverage_command_wrappers_cover_json_scan_copy_move_client_and_config_edges() {
    let db = test_db("command-semantics-low-coverage-wrappers");

    assert!(matches!(
        apply_async(&db, &["JSON.SET", "doc", "$", r#"{"name":"redis","n":1}"#]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.SET", "doc", "$.extra", r#""v""#, "NX"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.SET", "doc", "$.extra", r#""new""#, "NX"]).await,
        Frame::Null
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.SET", "doc", "$.extra", r#""new""#, "XX"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["JSON.GET", "doc", "$.extra"]),
        Frame::BulkString(_)
    ));
    assert!(
        matches!(apply(&db, &["JSON.TYPE", "doc", "$"]), Frame::SimpleString(kind) if kind == "object")
    );
    assert!(
        matches!(apply_async(&db, &["JSON.TYPE", "doc", "$.extra"]).await, Frame::SimpleString(kind) if kind == "string")
    );
    assert!(matches!(
        apply_async(&db, &["JSON.DEL", "doc", "$.extra"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(apply(&db, &["JSON.GET", "missing"]), Frame::Null));
    assert!(parse_err(&["JSON.SET", "doc"]).contains("wrong"));
    assert!(parse_err(&["JSON.SET", "doc", "$", "1", "BAD"]).contains("syntax"));
    assert!(parse_err(&["JSON.GET"]).contains("wrong"));
    assert!(parse_err(&["JSON.TYPE", "doc", "$", "extra"]).contains("wrong"));
    let invalid_path = Frame::Array(vec![
        Frame::bulk_string("JSON.DEL".to_string()),
        Frame::bulk_string("doc".to_string()),
        Frame::BulkString(vec![0xff]),
    ]);
    assert!(Command::parse_from_frame(invalid_path).is_err());
    assert!(matches!(
        apply(&db, &["JSON.TYPE", "doc", "$"]),
        Frame::SimpleString(kind) if kind == "object"
    ));

    assert!(matches!(
        apply_async(
            &db,
            &[
                "JSON.MSET",
                "json:a",
                "$",
                r#"{"name":"one","n":1,"items":[1,2]}"#,
                "json:b",
                "$",
                r#"{"name":"two","n":10}"#,
            ],
        )
        .await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.MGET", "json:a", "json:b", "$.name"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.NUMINCRBY", "json:a", "$.n", "2.5"]).await,
        Frame::BulkString(value) if value == b"3.5"
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.STRAPPEND", "json:a", "$.name", r#""dis""#]).await,
        Frame::Integer(6)
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.ARRAPPEND", "json:a", "$.items", "3", "4"]).await,
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.ARRINSERT", "json:a", "$.items", "1", "9"]).await,
        Frame::Integer(5)
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.ARRPOP", "json:a", "$.items", "1"]).await,
        Frame::BulkString(value) if value == b"9"
    ));
    assert!(matches!(
        apply_async(&db, &["JSON.OBJKEYS", "json:a"]).await,
        Frame::Array(values) if values.len() == 3
    ));

    let failed_mset = onedis_server::command_dispatch::handle_command_async(
        &db,
        parse(&[
            "JSON.MSET",
            "json:a",
            "$.n",
            "99",
            "json:b",
            "$.missing.child",
            "1",
        ]),
    )
    .await;
    assert!(failed_mset.is_err());
    assert!(matches!(
        apply_async(&db, &["JSON.GET", "json:a", "$.n"]).await,
        Frame::BulkString(value) if value == b"3.5"
    ));

    for idx in 0..7 {
        assert!(matches!(
            apply(&db, &["SET", &format!("scan:{idx}"), "v"]),
            Frame::Ok
        ));
    }
    let first_page = apply(&db, &["SCAN", "0", "MATCH", "scan:*", "COUNT", "3"]);
    let next_cursor = match first_page {
        Frame::Array(values) => match values.first() {
            Some(Frame::BulkString(cursor)) => {
                std::str::from_utf8(cursor).unwrap().parse::<u64>().unwrap()
            }
            _ => panic!("expected bulk string cursor"),
        },
        other => panic!("expected scan array, got {}", other),
    };
    assert!(next_cursor > 0);
    let second_page = apply_async(
        &db,
        &[
            "SCAN",
            &next_cursor.to_string(),
            "MATCH",
            "scan:*",
            "COUNT",
            "20",
        ],
    )
    .await;
    assert!(matches!(second_page, Frame::Array(_)));
    let empty_page = apply(&db, &["SCAN", "999", "MATCH", "scan:*", "COUNT", "2"]);
    assert!(matches!(empty_page, Frame::Array(_)));
    assert!(parse_err(&["SCAN"]).contains("requires"));
    assert!(parse_err(&["SCAN", "bad"]).contains("invalid digit"));
    assert!(parse_err(&["SCAN", "0", "COUNT", "bad"]).contains("invalid digit"));
    assert!(matches!(
        apply(&db, &["SCAN", "0", "TYPE", "string"]),
        Frame::Array(values) if values.len() == 2
    ));

    assert!(matches!(
        apply(&db, &["COPY", "scan:0", "scan:copy"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["COPY", "missing", "scan:none"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(&db, &["COPY", "scan:0", "scan:copy"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_async(&db, &["COPY", "scan:0", "scan:copy", "REPLACE"]).await,
        Frame::Integer(1)
    ));
    match onedis_server::command_dispatch::handle_command(&db, parse(&["COPY", "scan:0", "scan:0"]))
    {
        Err(error) => assert!(error.to_string().contains("same")),
        Ok(_) => panic!("COPY with the same source and destination must fail"),
    }
    match onedis_server::command_dispatch::handle_command_async(
        &db,
        parse(&["COPY", "scan:0", "scan:0", "REPLACE"]),
    )
    .await
    {
        Err(error) => assert!(error.to_string().contains("same")),
        Ok(_) => panic!("COPY with the same source and destination must fail"),
    }
    assert!(parse_err(&["COPY", "a"]).contains("wrong"));
    assert!(parse_err(&["COPY", "a", "b", "DB"]).contains("syntax"));
    assert!(parse_err(&["COPY", "a", "b", "DB", "bad"]).contains("out of range"));
    assert!(parse_err(&["COPY", "a", "b", "REPLACE", "REPLACE"]).contains("syntax"));
    assert!(parse_err(&["COPY", "a", "b", "NOPE"]).contains("syntax"));

    assert!(matches!(
        apply(&db, &["HSET", "rename:hash", "f", "v"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["PEXPIRE", "rename:hash", "60000"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["RENAME", "rename:hash", "rename:target"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["HGET", "rename:target", "f"]),
        Frame::BulkString(value) if value == b"v"
    ));
    assert!(matches!(
        apply(&db, &["PTTL", "rename:target"]),
        Frame::Integer(ttl) if ttl > 0
    ));
    assert!(matches!(
        apply(&db, &["RENAMENX", "rename:target", "rename:target"]),
        Frame::Integer(0)
    ));

    assert!(matches!(
        apply(&db, &["SET", "expire:opts", "v"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply(&db, &["EXPIRE", "expire:opts", "100", "NX", "NX"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["EXPIRE", "expire:opts", "200", "XX", "GT"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["EXPIRE", "expire:opts", "50", "LT", "XX"]),
        Frame::Integer(1)
    ));
    assert!(parse_err(&["EXPIRE", "expire:opts", "50", "GT", "LT"]).contains("not compatible"));
    assert!(parse_err(&["EXPIRE", "expire:opts", "50", "NX", "GT"]).contains("not compatible"));
    assert!(parse_err(&["SCAN", "0", "COUNT", "1000001"]).contains("response limit"));

    assert!(matches!(
        apply(&db, &["MOVE", "scan:1", "0"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(&db, &["MOVE", "missing", "1"]),
        Frame::Integer(0)
    ));
    assert!(parse_err(&["MOVE", "a"]).contains("wrong"));
    assert!(parse_err(&["MOVE", "a", "bad"]).contains("integer"));

    assert!(matches!(client_apply(&["CLIENT", "HELP"]), Frame::Array(_)));
    assert!(matches!(client_apply(&["CLIENT", "INFO"]), Frame::Error(_)));
    assert!(matches!(client_apply(&["CLIENT", "LIST"]), Frame::Error(_)));
    assert!(matches!(
        client_apply(&["CLIENT", "SETINFO", "LIB-NAME", "onedis"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "SETINFO", "only-one"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "SETNAME", "tester"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "SETNAME"]),
        Frame::Error(_)
    ));
    assert!(matches!(client_apply(&["CLIENT", "GETNAME"]), Frame::Error(_)));
    assert!(matches!(
        client_apply(&["CLIENT", "GETNAME", "extra"]),
        Frame::Error(_)
    ));
    assert!(matches!(client_apply(&["CLIENT", "ID"]), Frame::Error(_)));
    assert!(matches!(
        client_apply(&["CLIENT", "ID", "extra"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "GETREDIR"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "TRACKINGINFO"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "UNBLOCK", "123"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "KILL", "ID", "123"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        client_apply(&["CLIENT", "UNKNOWN"]),
        Frame::Error(_)
    ));
    assert!(parse_err(&["CLIENT"]).contains("wrong"));

    assert!(matches!(
        config_apply(&["CONFIG", "GET", "port"]),
        Frame::Array(_)
    ));
    assert!(matches!(
        config_apply(&["CONFIG", "GET", "*clients", "max*"]),
        Frame::Array(_)
    ));
    assert!(matches!(config_apply(&["CONFIG", "HELP"]), Frame::Array(_)));
    assert!(parse_err(&["CONFIG"]).contains("wrong"));
    assert!(parse_err(&["CONFIG", "GET"]).contains("wrong"));
    assert!(parse_err(&["CONFIG", "HELP", "extra"]).contains("wrong"));
    assert!(parse_err(&["CONFIG", "SET", "x", "y"]).contains("unknown"));
}
