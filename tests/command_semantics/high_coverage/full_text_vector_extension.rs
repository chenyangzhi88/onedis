#[test]
fn async_full_text_vector_and_extension_dispatch_paths_are_covered() {
    std::thread::Builder::new()
        .name("async-extension-dispatch-coverage".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async_full_text_vector_and_extension_dispatch_paths_are_covered_inner());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn async_full_text_vector_and_extension_dispatch_paths_are_covered_inner() {
    let db = test_db("command-semantics-async-extensions");

    assert!(matches!(
        apply_async(
            &db,
            &[
                "VADD",
                "points",
                "VALUES",
                "2",
                "1",
                "0",
                "a",
                "SETATTR",
                r#"{"kind":"seed","price":1}"#,
                "M",
                "8",
                "EF",
                "16",
            ],
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "VADD",
                "points",
                "VALUES",
                "2",
                "0",
                "1",
                "b",
                "SETATTR",
                r#"{"kind":"seed","price":2}"#,
            ],
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(&db, &["VCARD", "points"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_async(&db, &["VDIM", "points"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_async(&db, &["VEMB", "points", "a"]).await,
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_async(&db, &["VGETATTR", "points", "a"]).await,
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(&db, &["VSETATTR", "points", "a", r#"{"kind":"updated"}"#]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "VSIM",
                "points",
                "VALUES",
                "2",
                "1",
                "0",
                "COUNT",
                "2",
                "WITHSCORES",
                "WITHATTRIBS",
            ],
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["VINFO", "points"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["VRANDMEMBER", "points", "2"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["VLINKS", "points", "a", "WITHSCORES"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["VREM", "points", "b"]).await,
        Frame::Integer(1)
    ));
    assert!(parse_err(&["VEMB", "points", "a", "IGNORED"]).contains("syntax"));
    assert!(parse_err(&["VLINKS", "points", "a", "IGNORED"]).contains("syntax"));

    assert!(matches!(
        apply_async(
            &db,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
                "price",
                "NUMERIC",
            ],
        )
        .await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HSET",
                "doc:1",
                "title",
                "redis search",
                "tags",
                "db,search",
                "price",
                "10",
            ],
        )
        .await,
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "HSET",
                "doc:2",
                "title",
                "vector search",
                "tags",
                "vector",
                "price",
                "20",
            ],
        )
        .await,
        Frame::Integer(3)
    ));
    let search = wait_async_total(&db, &["FT.SEARCH", "idx", "search"], 2).await;
    assert!(contains_bulk(&search, "doc:1"));
    assert!(matches!(
        apply_async(&db, &["FT._LIST"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.INFO", "idx"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.TAGVALS", "idx", "tags"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.CONFIG", "SET", "DEFAULT_DIALECT", "2"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.CONFIG", "GET", "DEFAULT_DIALECT"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.ALTER", "idx", "SCHEMA", "ADD", "body", "TEXT"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.ALIASADD", "idx_alias", "idx"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.ALIASUPDATE", "idx_alias", "idx"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.ALIASDEL", "idx_alias"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.EXPLAIN", "idx", "search"]).await,
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "FT.PROFILE",
                "idx",
                "SEARCH",
                "QUERY",
                "search",
                "NOCONTENT"
            ],
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["FT.AGGREGATE", "idx", "*", "WITHCURSOR", "COUNT", "1"]
        )
        .await,
        Frame::Array(_)
    ));
    let cursor = apply_async(
        &db,
        &["FT.AGGREGATE", "idx", "*", "WITHCURSOR", "COUNT", "1"],
    )
    .await;
    if let Frame::Array(items) = cursor
        && let Some(Frame::Integer(cursor_id)) = items.get(1)
            && *cursor_id > 0 {
                let id = cursor_id.to_string();
                let _ = onedis_server::command_dispatch::handle_command_async(
                    &db,
                    parse(&["FT.CURSOR", "READ", "idx", &id, "COUNT", "1"]),
                )
                .await;
                let _ = onedis_server::command_dispatch::handle_command_async(
                    &db,
                    parse(&["FT.CURSOR", "DEL", "idx", &id]),
                )
                .await;
            }
    assert!(matches!(
        apply_async(&db, &["FT.DICTADD", "terms", "redis", "search"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.DICTDUMP", "terms"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["FT.SPELLCHECK", "idx", "rediz", "TERMS", "INCLUDE", "terms"]
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.DICTDEL", "terms", "search"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["FT.SUGADD", "ac", "redis", "10", "PAYLOAD", "database"]
        )
        .await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(
            &db,
            &["FT.SUGGET", "ac", "re", "WITHSCORES", "WITHPAYLOADS"]
        )
        .await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.SUGLEN", "ac"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.SUGDEL", "ac", "redis"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.SYNUPDATE", "idx", "grp", "redis", "valkey"]).await,
        Frame::Ok
    ));
    assert!(matches!(
        apply_async(&db, &["FT.SYNDUMP", "idx"]).await,
        Frame::Array(_)
    ));
    assert!(matches!(
        apply_async(&db, &["FT.DROPINDEX", "idx"]).await,
        Frame::Ok
    ));
}
