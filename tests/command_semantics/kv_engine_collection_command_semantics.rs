mod support;

use support::*;

#[test]
fn hincrby_updates_hash_field_atomically() {
    let db = test_db();

    let hset = apply_command(&db, &["hset", "stats", "views", "1", "ratio", "1.5"]);
    assert!(matches!(hset, Frame::Integer(2)));

    let first = apply_command(&db, &["hincrby", "stats", "views", "5"]);
    assert!(matches!(first, Frame::Integer(6)));

    let second = apply_command(&db, &["hincrby", "stats", "views", "-2"]);
    assert!(matches!(second, Frame::Integer(4)));

    let hget = apply_command(&db, &["hget", "stats", "views"]);
    assert!(matches!(hget, Frame::BulkString(value) if value.as_slice() == b"4"));

    assert!(matches!(
        apply_command(&db, &["hrandfield", "stats"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_command(&db, &["hrandfield", "stats", "2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["hrandfield", "stats", "1", "withvalues"]),
        Frame::Array(values) if values.len() == 2
    ));

    let ratio = apply_command(&db, &["hincrbyfloat", "stats", "ratio", "2.25"]);
    assert!(matches!(ratio, Frame::BulkString(value) if value.as_slice() == b"3.75"));

    let new_float = apply_command(&db, &["hincrbyfloat", "stats", "new-ratio", "1.25"]);
    assert!(matches!(new_float, Frame::BulkString(value) if value.as_slice() == b"1.25"));

    apply_command(&db, &["hset", "stats", "bad", "nan"]);
    let bad = apply_command(&db, &["hincrby", "stats", "bad", "1"]);
    assert!(matches!(bad, Frame::Error(message) if message.contains("not an integer")));
    let bad_float = apply_command(&db, &["hincrbyfloat", "stats", "bad", "1"]);
    assert!(matches!(bad_float, Frame::Error(message) if message.contains("not a float")));
}

#[test]
fn hash_field_absolute_and_millisecond_ttl_commands_are_dispatched() {
    let db = test_db();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    assert!(matches!(
        apply_command(&db, &["hset", "field-ttl", "a", "1", "b", "2", "c", "3"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["hpexpire", "field-ttl", "5000", "fields", "1", "a"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(1)))
    ));
    assert!(matches!(
        apply_command(&db, &["hpttl", "field-ttl", "fields", "1", "a"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(ttl)) if *ttl > 0 && *ttl <= 5000)
    ));

    let future_secs = (now + Duration::from_secs(5)).as_secs().to_string();
    assert!(matches!(
        apply_command(
            &db,
            &["hexpireat", "field-ttl", &future_secs, "fields", "1", "b"]
        ),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(1)))
    ));
    assert!(matches!(
        apply_command(&db, &["hexpiretime", "field-ttl", "fields", "1", "b"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(expire_at)) if *expire_at == future_secs.parse::<i64>().unwrap())
    ));

    let future_millis = (now + Duration::from_secs(5)).as_millis().to_string();
    assert!(matches!(
        apply_command(
            &db,
            &[
                "hpexpireat",
                "field-ttl",
                &future_millis,
                "fields",
                "1",
                "c",
            ]
        ),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(1)))
    ));
    assert!(matches!(
        apply_command(&db, &["hpexpiretime", "field-ttl", "fields", "1", "c"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(expire_at)) if *expire_at == future_millis.parse::<i64>().unwrap())
    ));
}

#[test]
fn concurrent_hash_field_ttl_updates_keep_all_fields_visible() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = Arc::new(test_db());

    let workers = (0..16)
        .map(|idx| {
            let db = db.clone();
            rt.spawn(async move {
                let field = format!("f{idx}");
                let value = idx.to_string();
                assert!(matches!(
                    apply_command_async(&db, &["hset", "ttl-hash", &field, &value]).await,
                    Frame::Integer(0 | 1)
                ));
                assert!(matches!(
                    apply_command_async(&db, &["hpexpire", "ttl-hash", "5000", "fields", "1", &field]).await,
                    Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(1)))
                ));
                assert!(matches!(
                    apply_command_async(&db, &["hget", "ttl-hash", &field]).await,
                    Frame::BulkString(stored) if stored == value.as_bytes()
                ));
            })
        })
        .collect::<Vec<_>>();

    rt.block_on(async {
        for worker in workers {
            worker.await.unwrap();
        }
    });

    for idx in 0..16 {
        let field = format!("f{idx}");
        assert!(matches!(
            apply_command(&db, &["hget", "ttl-hash", &field]),
            Frame::BulkString(value) if value == idx.to_string().as_bytes()
        ));
        assert!(matches!(
            apply_command(&db, &["hpttl", "ttl-hash", "fields", "1", &field]),
            Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(ttl)) if *ttl > 0 && *ttl <= 5000)
        ));
    }
}

#[test]
fn hincrby_legacy_single_field_path_still_works() {
    let db = test_db();

    let first = apply_command(&db, &["hincrby", "stats", "views", "5"]);
    assert!(matches!(first, Frame::Integer(5)));

    let second = apply_command(&db, &["hincrby", "stats", "views", "-2"]);
    assert!(matches!(second, Frame::Integer(3)));

    let hget = apply_command(&db, &["hget", "stats", "views"]);
    assert!(matches!(hget, Frame::BulkString(value) if value.as_slice() == b"3"));

    apply_command(&db, &["hset", "stats", "bad", "nan"]);
    let bad = apply_command(&db, &["hincrby", "stats", "bad", "1"]);
    assert!(matches!(bad, Frame::Error(message) if message.contains("not an integer")));
}

#[tokio::test]
async fn hash_wrapper_commands_cover_read_write_missing_wrong_type_and_async_paths() {
    let db = test_db();

    for args in [
        &["hexists", "h"][..],
        &["hgetall"][..],
        &["hkeys"][..],
        &["hlen"][..],
        &["hmget", "h"][..],
        &["hsetnx", "h", "f"][..],
        &["hstrlen", "h"][..],
        &["hvals"][..],
    ] {
        assert!(Command::parse_from_frame(frame_args(args)).is_err());
    }

    assert!(matches!(
        apply_command_async(&db, &["hsetnx", "hash-read", "a", "alpha"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hsetnx", "hash-read", "a", "ignored"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hset", "hash-read", "b", "beta"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hexists", "hash-read", "a"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hexists", "hash-read", "missing"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hlen", "hash-read"]).await,
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hstrlen", "hash-read", "a"]).await,
        Frame::Integer(5)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hstrlen", "hash-read", "missing"]).await,
        Frame::Integer(0)
    ));

    let hmget = apply_command_async(&db, &["hmget", "hash-read", "a", "missing", "b"]).await;
    assert!(matches!(
        hmget,
        Frame::Array(values)
            if matches!(values.first(), Some(Frame::BulkString(value)) if value == b"alpha")
                && matches!(values.get(1), Some(Frame::Null))
                && matches!(values.get(2), Some(Frame::BulkString(value)) if value == b"beta")
    ));

    for frame in [
        apply_command_async(&db, &["hkeys", "hash-read"]).await,
        apply_command_async(&db, &["hvals", "hash-read"]).await,
        apply_command_async(&db, &["hgetall", "hash-read"]).await,
    ] {
        assert!(array_contains_bulk(&frame, "a") || array_contains_bulk(&frame, "alpha"));
        assert!(array_contains_bulk(&frame, "b") || array_contains_bulk(&frame, "beta"));
    }

    assert!(matches!(
        apply_command_async(&db, &["hmget", "missing-hash", "a"]).await,
        Frame::Array(values) if matches!(values.first(), Some(Frame::Null))
    ));
    assert!(matches!(
        apply_command_async(&db, &["hlen", "missing-hash"]).await,
        Frame::Integer(0)
    ));

    apply_command_async(&db, &["set", "not-a-hash", "value"]).await;
    for args in [
        &["hexists", "not-a-hash", "f"][..],
        &["hgetall", "not-a-hash"][..],
        &["hkeys", "not-a-hash"][..],
        &["hlen", "not-a-hash"][..],
        &["hmget", "not-a-hash", "f"][..],
        &["hsetnx", "not-a-hash", "f", "v"][..],
        &["hstrlen", "not-a-hash", "f"][..],
        &["hvals", "not-a-hash"][..],
    ] {
        assert!(
            matches!(apply_command_async(&db, args).await, Frame::Error(message) if message.contains("wrong kind")),
            "{args:?}"
        );
    }
}

#[tokio::test]
async fn concurrent_hsetnx_allows_exactly_one_writer_per_field() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();

    for idx in 0..32usize {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let value = idx.to_string();
            apply_command_async(&db, &["hsetnx", "once", "field", &value]).await
        }));
    }

    let mut inserted = 0;
    for task in tasks {
        match task.await.unwrap() {
            Frame::Integer(1) => inserted += 1,
            Frame::Integer(0) => {}
            _ => panic!("unexpected HSETNX result"),
        }
    }

    assert_eq!(inserted, 1);
    assert!(matches!(
        apply_command_async(&db, &["hlen", "once"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["hexists", "once", "field"]).await,
        Frame::Integer(1)
    ));
}

#[test]
fn zincrby_updates_sorted_set_score() {
    let db = test_db();

    let first = apply_command(&db, &["zincrby", "leaders", "1.5", "alice"]);
    assert!(matches!(first, Frame::BulkString(value) if value.as_slice() == b"1.5"));

    let second = apply_command(&db, &["zincrby", "leaders", "2.25", "alice"]);
    assert!(matches!(second, Frame::BulkString(value) if value.as_slice() == b"3.75"));

    let zscore = apply_command(&db, &["zscore", "leaders", "alice"]);
    assert!(matches!(zscore, Frame::BulkString(value) if value.as_slice() == b"3.75"));
}

#[test]
fn sdiffstore_writes_difference_to_destination() {
    let db = test_db();

    apply_command(&db, &["sadd", "a", "one", "two", "three"]);
    apply_command(&db, &["sadd", "b", "two"]);
    apply_command(&db, &["sadd", "c", "three"]);

    let stored = apply_command(&db, &["sdiffstore", "out", "a", "b", "c"]);
    assert!(matches!(stored, Frame::Integer(1)));

    let members = apply_command(&db, &["smembers", "out"]);
    match members {
        Frame::Array(values) => {
            assert_eq!(values.len(), 1);
            assert!(matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"one"));
        }
        other => panic!("unexpected frame: {}", other),
    }

    let empty = apply_command(&db, &["sdiffstore", "out", "b", "b"]);
    assert!(matches!(empty, Frame::Integer(0)));
    assert!(db.get("out").is_none());
}

#[test]
fn set_command_smoke_covers_union_inter_membership_remove_and_pop() {
    let db = test_db();

    apply_command(&db, &["sadd", "s1", "a", "b", "c"]);
    apply_command(&db, &["sadd", "s2", "b", "c", "d"]);

    assert!(matches!(
        apply_command(&db, &["sismember", "s1", "a"]),
        Frame::Integer(1)
    ));

    let inter = apply_command(&db, &["sinter", "s1", "s2"]);
    assert!(array_contains_bulk(&inter, "b"));
    assert!(array_contains_bulk(&inter, "c"));

    let union = apply_command(&db, &["sunion", "s1", "s2"]);
    assert!(array_contains_bulk(&union, "a"));
    assert!(array_contains_bulk(&union, "d"));

    assert!(matches!(
        apply_command(&db, &["sunionstore", "sout", "s1", "s2"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply_command(&db, &["sinterstore", "iout", "s1", "s2"]),
        Frame::Integer(2)
    ));
    let iout = apply_command(&db, &["smembers", "iout"]);
    assert!(array_contains_bulk(&iout, "b"));
    assert!(array_contains_bulk(&iout, "c"));
    assert!(matches!(
        apply_command(&db, &["smismember", "s1", "a", "d", "b"]),
        Frame::Array(values) if matches!(
            values.as_slice(),
            [Frame::Integer(1), Frame::Integer(0), Frame::Integer(1)]
        )
    ));
    assert!(matches!(
        apply_command(&db, &["srandmember", "s1"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_command(&db, &["srandmember", "s1", "2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["srem", "sout", "a", "missing"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["spop", "sout"]),
        Frame::BulkString(_)
    ));
}

#[test]
fn sorted_set_command_smoke_covers_range_rank_count_remove_and_scan() {
    let db = test_db();

    assert!(matches!(
        apply_command(
            &db,
            &["zadd", "scores", "1", "alice", "2", "bob", "3", "cara"]
        ),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["zcard", "scores"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["zcount", "scores", "1", "2"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["zrank", "scores", "bob"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zrevrank", "scores", "bob"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zmscore", "scores", "alice", "missing", "cara"]),
        Frame::Array(values) if values.len() == 3
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"1")
            && matches!(&values[1], Frame::Null)
            && matches!(&values[2], Frame::BulkString(value) if value.as_slice() == b"3")
    ));

    let range = apply_command(&db, &["zrange", "scores", "0", "-1"]);
    assert!(array_contains_bulk(&range, "alice"));
    assert!(array_contains_bulk(&range, "cara"));

    let range_with_scores = apply_command(&db, &["zrange", "scores", "0", "0", "withscores"]);
    assert!(matches!(
        range_with_scores,
        Frame::Array(values) if values.len() == 2
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"alice")
            && matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"1")
    ));

    let rev = apply_command(&db, &["zrevrange", "scores", "0", "0"]);
    assert!(array_contains_bulk(&rev, "cara"));

    let modern_rev = apply_command(&db, &["zrange", "scores", "0", "0", "rev"]);
    assert!(matches!(
        modern_rev,
        Frame::Array(values) if matches!(values.as_slice(), [Frame::BulkString(value)] if value.as_slice() == b"cara")
    ));

    let by_score = apply_command(&db, &["zrangebyscore", "scores", "2", "3"]);
    assert!(array_contains_bulk(&by_score, "bob"));
    assert!(array_contains_bulk(&by_score, "cara"));

    let modern_by_score = apply_command(
        &db,
        &[
            "zrange", "scores", "1", "3", "byscore", "rev", "limit", "0", "2",
        ],
    );
    assert!(matches!(
        modern_by_score,
        Frame::Array(values) if values.len() == 2
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"cara")
            && matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"bob")
    ));

    assert!(matches!(
        apply_command(&db, &["zrangestore", "top", "scores", "0", "1"]),
        Frame::Integer(2)
    ));
    let stored = apply_command(&db, &["zrange", "top", "0", "-1", "withscores"]);
    assert!(matches!(
        stored,
        Frame::Array(values) if values.len() == 4
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"alice")
            && matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"1")
            && matches!(&values[2], Frame::BulkString(value) if value.as_slice() == b"bob")
            && matches!(&values[3], Frame::BulkString(value) if value.as_slice() == b"2")
    ));
    assert!(matches!(
        apply_command(
            &db,
            &[
                "zrangestore",
                "top-by-score",
                "scores",
                "1",
                "3",
                "byscore",
                "rev",
                "limit",
                "0",
                "2",
            ],
        ),
        Frame::Integer(2)
    ));

    assert!(matches!(
        apply_command(
            &db,
            &["zadd", "lex", "0", "alpha", "0", "bravo", "0", "charlie"]
        ),
        Frame::Integer(3)
    ));
    let lex = apply_command(&db, &["zrange", "lex", "[alpha", "(charlie", "bylex"]);
    assert!(matches!(
        lex,
        Frame::Array(values) if values.len() == 2
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"alpha")
            && matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"bravo")
    ));
    let lex_limited = apply_command(
        &db,
        &["zrange", "lex", "-", "+", "bylex", "rev", "limit", "0", "2"],
    );
    assert!(matches!(
        lex_limited,
        Frame::Array(values) if values.len() == 2
            && matches!(&values[0], Frame::BulkString(value) if value.as_slice() == b"charlie")
            && matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"bravo")
    ));
    assert!(matches!(
        apply_command(&db, &["zlexcount", "lex", "[alpha", "[charlie"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["zrangebylex", "lex", "[alpha", "(charlie"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["zrevrangebylex", "lex", "+", "-", "limit", "0", "1"]),
        Frame::Array(values) if values.len() == 1
    ));

    let scan = apply_command(&db, &["zscan", "scores", "0"]);
    assert!(matches!(scan, Frame::Array(_)));

    assert!(matches!(
        apply_command(&db, &["zadd", "scores2", "2", "bob", "4", "dave"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["zunion", "2", "scores", "scores2", "withscores"]),
        Frame::Array(values) if values.len() == 8
    ));
    assert!(matches!(
        apply_command(&db, &["zinter", "2", "scores", "scores2", "withscores"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["zdiff", "2", "scores", "scores2"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["zintercard", "2", "scores", "scores2"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zunionstore", "uout", "2", "scores", "scores2"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply_command(&db, &["zinterstore", "iout", "2", "scores", "scores2"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zdiffstore", "dout", "2", "scores", "scores2"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["zpopmin", "uout", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["zpopmax", "uout", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    assert!(matches!(
        apply_command(&db, &["zmpop", "2", "missing", "uout", "min", "count", "1"]),
        Frame::Array(values) if values.len() == 2
    ));
    apply_command(&db, &["zadd", "blockz", "1", "a", "2", "b"]);
    assert!(matches!(
        apply_command(&db, &["bzpopmin", "blockz", "1"]),
        Frame::Array(values) if values.len() == 3
    ));
    assert!(matches!(
        apply_command(&db, &["bzpopmax", "blockz", "1"]),
        Frame::Array(values) if values.len() == 3
    ));
    apply_command(&db, &["zadd", "blockzm", "1", "a"]);
    assert!(matches!(
        apply_command(&db, &["bzmpop", "1", "1", "blockzm", "min", "count", "1"]),
        Frame::Array(values) if values.len() == 2
    ));

    assert!(matches!(
        apply_command(&db, &["zrem", "scores", "bob", "missing"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zremrangebyscore", "scores", "1", "1"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zremrangebyrank", "scores", "0", "-1"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["zremrangebylex", "lex", "-", "+"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["zcard", "scores"]),
        Frame::Integer(0)
    ));
}

#[test]
fn expire_family_updates_ttl_on_kv_engine_backed_key() {
    let db = test_db();

    apply_command(&db, &["set", "exp", "value"]);

    let expire_frame = apply_command(&db, &["expire", "exp", "1"]);
    assert!(matches!(expire_frame, Frame::Integer(1)));
    let ttl_frame = apply_command(&db, &["ttl", "exp"]);
    assert!(matches!(ttl_frame, Frame::Integer(value) if (0..=1).contains(&value)));

    let pexpire_frame = apply_command(&db, &["pexpire", "exp", "1500"]);
    assert!(matches!(pexpire_frame, Frame::Integer(1)));
    let pttl_frame = apply_command(&db, &["pttl", "exp"]);
    assert!(matches!(pttl_frame, Frame::Integer(value) if value > 0 && value <= 1500));
}

#[test]
fn expire_family_returns_redis_integer_and_handles_immediate_delete() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["expire", "missing", "10"]),
        Frame::Integer(0)
    ));

    apply_command(&db, &["set", "gone", "value"]);
    assert!(matches!(
        apply_command(&db, &["expire", "gone", "0"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["exists", "gone"]),
        Frame::Integer(0)
    ));

    apply_command(&db, &["set", "pgone", "value"]);
    assert!(matches!(
        apply_command(&db, &["pexpire", "pgone", "-1"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["exists", "pgone"]),
        Frame::Integer(0)
    ));
}

#[test]
fn expire_family_supports_nx_xx_gt_lt_options() {
    let db = test_db();

    apply_command(&db, &["set", "opt", "value"]);

    assert!(matches!(
        apply_command(&db, &["expire", "opt", "10", "NX"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["expire", "opt", "20", "NX"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["expire", "opt", "20", "XX"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["pexpire", "opt", "1000", "GT"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["pexpire", "opt", "25000", "GT"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["pexpire", "opt", "1000", "LT"]),
        Frame::Integer(1)
    ));

    apply_command(&db, &["set", "plain", "value"]);
    assert!(matches!(
        apply_command(&db, &["expire", "plain", "10", "XX"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["expire", "plain", "10", "GT"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["expire", "plain", "10", "LT"]),
        Frame::Integer(1)
    ));
}

#[test]
fn expireat_family_returns_integer_and_deletes_past_deadlines() {
    let db = test_db();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    apply_command(&db, &["set", "at", "value"]);
    let future_secs = (now + Duration::from_secs(5)).as_secs().to_string();
    assert!(matches!(
        apply_command(&db, &["expireat", "at", &future_secs]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["ttl", "at"]),
        Frame::Integer(value) if value > 0 && value <= 5
    ));

    let past_secs = now.as_secs().saturating_sub(1).to_string();
    assert!(matches!(
        apply_command(&db, &["expireat", "at", &past_secs]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["exists", "at"]),
        Frame::Integer(0)
    ));

    apply_command(&db, &["set", "pat", "value"]);
    let future_millis = (now + Duration::from_secs(5)).as_millis().to_string();
    assert!(matches!(
        apply_command(&db, &["pexpireat", "pat", &future_millis]),
        Frame::Integer(1)
    ));
    let past_millis = now.as_millis().saturating_sub(1).to_string();
    assert!(matches!(
        apply_command(&db, &["pexpireat", "pat", &past_millis]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["exists", "pat"]),
        Frame::Integer(0)
    ));
}

#[test]
fn bitmap_and_bitfield_commands_use_string_storage() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["setbit", "bits", "1", "1"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(&db, &["getbit", "bits", "1"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["bitcount", "bits"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["bitpos", "bits", "1"]),
        Frame::Integer(1)
    ));

    let values = apply_command(
        &db,
        &["bitfield", "bits", "set", "u4", "4", "10", "get", "u4", "4"],
    );
    assert!(matches!(
        values,
        Frame::Array(items)
            if matches!(items.first(), Some(Frame::Integer(0)))
                && matches!(items.get(1), Some(Frame::Integer(10)))
    ));

    apply_command(&db, &["set", "left", "\u{f0}"]);
    apply_command(&db, &["set", "right", "\u{0f}"]);
    assert!(matches!(
        apply_command(&db, &["bitop", "or", "out", "left", "right"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["bitcount", "out"]),
        Frame::Integer(value) if value >= 8
    ));
}

#[tokio::test]
async fn bitmap_commands_cover_parser_errors_ranges_ro_signed_and_async_errors() {
    let db = test_db();

    for args in [
        &["getbit", "bits"][..],
        &["getbit", "bits", "nope"][..],
        &["setbit", "bits", "1"][..],
        &["setbit", "bits", "nope", "1"][..],
        &["setbit", "bits", "1", "nope"][..],
        &["bitcount"][..],
        &["bitcount", "bits", "bad", "1"][..],
        &["bitpos", "bits"][..],
        &["bitpos", "bits", "bad"][..],
        &["bitpos", "bits", "1", "bad"][..],
        &["bitfield"][..],
        &["bitfield", "bits", "get", "x8", "0"][..],
        &["bitfield", "bits", "get", "u8", "bad"][..],
        &["bitfield", "bits", "set", "u8", "0", "bad"][..],
        &["bitfield", "bits", "unknown"][..],
    ] {
        assert!(
            Command::parse_from_frame(frame_args(args)).is_err(),
            "{args:?}"
        );
    }

    assert!(matches!(
        apply_command_async(&db, &["setbit", "async-bits", "0", "1"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command_async(&db, &["setbit", "async-bits", "8", "1"]).await,
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command_async(&db, &["getbit", "async-bits", "8"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitcount", "async-bits", "0", "0"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitcount", "async-bits", "-1", "-1"]).await,
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitpos", "async-bits", "1", "1", "1"]).await,
        Frame::Integer(8)
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitpos", "async-bits", "1", "3", "2"]).await,
        Frame::Integer(-1)
    ));

    let readonly =
        apply_command_async(&db, &["bitfield_ro", "async-bits", "get", "u8", "#1"]).await;
    assert!(matches!(
        readonly,
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(128)))
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitfield_ro", "async-bits", "set", "u4", "0", "1"]).await,
        Frame::Error(message) if message.contains("only supports GET")
    ));

    let signed = apply_command_async(
        &db,
        &[
            "bitfield",
            "signed-bits",
            "set",
            "i4",
            "0",
            "-1",
            "get",
            "i4",
            "0",
            "incrby",
            "u4",
            "#1",
            "2",
        ],
    )
    .await;
    assert!(matches!(
        signed,
        Frame::Array(values)
            if matches!(values.first(), Some(Frame::Integer(0)))
                && matches!(values.get(1), Some(Frame::Integer(-1)))
                && matches!(values.get(2), Some(Frame::Integer(2)))
    ));
    assert!(matches!(
        apply_command_async(&db, &["bitfield", "signed-bits", "get", "u64", "0"]).await,
        Frame::Error(message) if message.contains("unsupported bitfield type")
    ));

    apply_command_async(&db, &["json.set", "not-string-bits", "$", "{\"a\":1}"]).await;
    for args in [
        &["getbit", "not-string-bits", "0"][..],
        &["setbit", "not-string-bits", "0", "1"][..],
        &["bitcount", "not-string-bits"][..],
        &["bitpos", "not-string-bits", "1"][..],
        &["bitfield", "not-string-bits", "get", "u1", "0"][..],
    ] {
        assert!(
            matches!(apply_command_async(&db, args).await, Frame::Error(message) if message.contains("wrong kind")),
            "{args:?}"
        );
    }
}

#[test]
fn hyperloglog_compat_commands_track_exact_unique_members() {
    let db = test_db();

    assert!(matches!(
        apply_command(&db, &["pfadd", "hll-a", "a", "b", "a"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["pfcount", "hll-a"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["pfadd", "hll-b", "b", "c"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["pfmerge", "hll-out", "hll-a", "hll-b"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["pfcount", "hll-out"]),
        Frame::Integer(3)
    ));
}

#[test]
fn geo_commands_store_coordinates_and_search_radius() {
    let db = test_db();

    assert!(matches!(
        apply_command(
            &db,
            &["geoadd", "cities", "13.361389", "38.115556", "palermo"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(
            &db,
            &["geoadd", "cities", "15.087269", "37.502669", "catania"]
        ),
        Frame::Integer(1)
    ));

    let pos = apply_command(&db, &["geopos", "cities", "palermo"]);
    assert!(matches!(
        pos,
        Frame::Array(items) if matches!(items.first(), Some(Frame::Array(coords)) if coords.len() == 2)
    ));
    assert!(matches!(
        apply_command(&db, &["geodist", "cities", "palermo", "catania", "km"]),
        Frame::BulkString(_)
    ));
    let geohash = apply_command(&db, &["geohash", "cities", "palermo"]);
    assert!(
        matches!(
            &geohash,
            Frame::Array(items) if matches!(items.first(), Some(Frame::BulkString(hash)) if hash == b"sqc8b49rny0")
        ),
        "{}",
        geohash.to_string()
    );
    assert!(matches!(
        apply_command(&db, &["zscore", "cities", "palermo"]),
        Frame::BulkString(score) if score == b"3479099956230698"
    ));

    let legacy_radius = apply_command(
        &db,
        &[
            "georadius",
            "cities",
            "13.361389",
            "38.115556",
            "200",
            "km",
            "withdist",
        ],
    );
    assert!(matches!(
        legacy_radius,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::Array(parts) if matches!(parts.first(), Some(Frame::BulkString(name)) if name == b"palermo")))
    ));

    let legacy_radius_by_member = apply_command(
        &db,
        &[
            "georadiusbymember",
            "cities",
            "palermo",
            "200",
            "km",
            "withcoord",
        ],
    );
    assert!(matches!(
        legacy_radius_by_member,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::Array(parts) if matches!(parts.first(), Some(Frame::BulkString(name)) if name == b"palermo")))
    ));

    let nearby = apply_command(
        &db,
        &[
            "geosearch",
            "cities",
            "frommember",
            "palermo",
            "byradius",
            "200",
            "km",
            "withdist",
        ],
    );
    assert!(matches!(
        nearby,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::Array(parts) if matches!(parts.first(), Some(Frame::BulkString(name)) if name == b"palermo")))
    ));

    let rich = apply_command(
        &db,
        &[
            "geosearch",
            "cities",
            "frommember",
            "palermo",
            "byradius",
            "200",
            "km",
            "withdist",
            "withhash",
            "withcoord",
            "asc",
            "count",
            "1",
        ],
    );
    assert!(matches!(
        rich,
        Frame::Array(items)
            if matches!(items.first(), Some(Frame::Array(parts))
                if matches!(parts.first(), Some(Frame::BulkString(name)) if name == b"palermo")
                    && matches!(parts.get(1), Some(Frame::BulkString(_)))
                    && matches!(parts.get(2), Some(Frame::Integer(_)))
                    && matches!(parts.get(3), Some(Frame::Array(coords)) if coords.len() == 2))
    ));

    let by_box = apply_command(
        &db,
        &[
            "geosearch",
            "cities",
            "fromlonlat",
            "13.361389",
            "38.115556",
            "bybox",
            "20",
            "20",
            "km",
        ],
    );
    assert!(matches!(
        by_box,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::BulkString(name) if name == b"palermo"))
    ));

    assert!(matches!(
        apply_command(
            &db,
            &[
                "geosearchstore",
                "nearby",
                "cities",
                "frommember",
                "palermo",
                "byradius",
                "200",
                "km",
            ],
        ),
        Frame::Integer(value) if value >= 1
    ));
    assert!(matches!(
        apply_command(
            &db,
            &[
                "geosearchstore",
                "nearby-dist",
                "cities",
                "frommember",
                "palermo",
                "byradius",
                "200",
                "km",
                "storedist",
            ],
        ),
        Frame::Integer(value) if value >= 1
    ));
    assert!(matches!(
        apply_command(&db, &["geoadd", "cities", "nx", "12", "37", "palermo"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply_command(
            &db,
            &["geoadd", "cities", "xx", "ch", "12", "37", "palermo"]
        ),
        Frame::Integer(1)
    ));
}
