mod support;

use support::*;

#[test]
fn compatibility_surface_handles_common_unknown_command_families() {
    let client = Command::parse_from_frame(frame_args(&["client", "trackinginfo"])).unwrap();
    let client = match client {
        Command::Client(command) => command.apply().unwrap(),
        other => panic!("expected Client, got {}", other.name()),
    };
    assert!(matches!(
        client,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::BulkString(value) if value == b"flags"))
    ));

    let hello = Command::parse_from_frame(frame_args(&["hello", "2"])).unwrap();
    let hello = match hello {
        Command::Unknown(command) => command.apply().unwrap(),
        other => panic!("expected Unknown, got {}", other.name()),
    };
    assert!(matches!(
        hello,
        Frame::Array(items) if items.iter().any(|item| matches!(item, Frame::BulkString(value) if value == b"standalone"))
    ));

    let acl = Command::parse_from_frame(frame_args(&["acl", "whoami"])).unwrap();
    let acl = match acl {
        Command::Unknown(command) => command.apply().unwrap(),
        other => panic!("expected Unknown, got {}", other.name()),
    };
    assert!(matches!(acl, Frame::BulkString(value) if value == b"default"));

    let pubsub = Command::parse_from_frame(frame_args(&["pubsub", "numsub", "events"])).unwrap();
    let pubsub = match pubsub {
        Command::Unknown(command) => command.apply().unwrap(),
        other => panic!("expected Unknown, got {}", other.name()),
    };
    assert!(matches!(
        pubsub,
        Frame::Array(items)
            if matches!(items.first(), Some(Frame::BulkString(channel)) if channel == b"events")
                && matches!(items.get(1), Some(Frame::Integer(0)))
    ));
}

#[test]
fn unknown_command_compat_surface_covers_all_subcommand_shapes() {
    assert!(Command::parse_from_frame(Frame::Array(Vec::new())).is_err());

    let parsed = Command::parse_from_frame(frame_args(&["doesnotexist", "a", "b"])).unwrap();
    let unknown = match parsed {
        Command::Unknown(command) => command,
        other => panic!("expected Unknown, got {}", other.name()),
    };
    assert_eq!(unknown.command_name(), "doesnotexist");
    assert_eq!(unknown.args(), &["a".to_string(), "b".to_string()]);
    assert!(matches!(
        unknown.apply().unwrap(),
        Frame::Error(message) if message.contains("unknown command")
    ));

    for args in [
        &["quit"][..],
        &["reset"][..],
        &["asking"][..],
        &["readonly"][..],
        &["readwrite"][..],
    ] {
        match Command::parse_from_frame(frame_args(args)).unwrap() {
            Command::Unknown(command) => assert!(matches!(command.apply().unwrap(), Frame::Ok)),
            other => panic!("expected Unknown, got {}", other.name()),
        }
    }

    for args in [
        &["command"][..],
        &["command", "docs"][..],
        &["command", "info"][..],
        &["memory", "stats"][..],
        &["memory", "help"][..],
        &["latency", "help"][..],
        &["slowlog", "get"][..],
        &["module", "list"][..],
        &["pubsub", "channels"][..],
        &["pubsub", "shardchannels"][..],
        &["pubsub", "shardnumsub"][..],
        &["pubsub", "help"][..],
    ] {
        match Command::parse_from_frame(frame_args(args)).unwrap() {
            Command::Unknown(command) => {
                assert!(matches!(command.apply().unwrap(), Frame::Array(_)))
            }
            other => panic!("expected Unknown, got {}", other.name()),
        }
    }

    for args in [
        &["command", "count"][..],
        &["memory", "usage", "k"][..],
        &["pubsub", "numpat"][..],
        &["publish", "events", "payload"][..],
        &["spublish", "events", "payload"][..],
        &["cluster", "keyslot", "k"][..],
    ] {
        match Command::parse_from_frame(frame_args(args)).unwrap() {
            Command::Unknown(command) => {
                assert!(matches!(command.apply().unwrap(), Frame::Integer(_)))
            }
            other => panic!("expected Unknown, got {}", other.name()),
        }
    }

    for args in [
        &["time"][..],
        &["hello", "3"][..],
        &["acl", "list"][..],
        &["acl", "users"][..],
        &["acl", "cat"][..],
        &["acl", "help"][..],
        &["cluster", "slots"][..],
        &["cluster", "shards"][..],
        &["cluster", "help"][..],
        &["subscribe", "events", "alerts"][..],
        &["psubscribe", "ev*"][..],
        &["ssubscribe", "events"][..],
        &["unsubscribe", "events"][..],
        &["punsubscribe"][..],
        &["sunsubscribe", "events"][..],
    ] {
        match Command::parse_from_frame(frame_args(args)).unwrap() {
            Command::Unknown(command) => {
                assert!(matches!(command.apply().unwrap(), Frame::Array(_)))
            }
            other => panic!("expected Unknown, got {}", other.name()),
        }
    }

    for args in [&["cluster", "info"][..], &["cluster", "nodes"][..]] {
        match Command::parse_from_frame(frame_args(args)).unwrap() {
            Command::Unknown(command) => {
                assert!(matches!(command.apply().unwrap(), Frame::BulkString(_)))
            }
            other => panic!("expected Unknown, got {}", other.name()),
        }
    }

    match Command::parse_from_frame(frame_args(&["memory"])).unwrap() {
        Command::Unknown(command) => assert!(matches!(command.apply().unwrap(), Frame::Ok)),
        other => panic!("expected Unknown, got {}", other.name()),
    }
    match Command::parse_from_frame(frame_args(&["acl", "setuser", "default"])).unwrap() {
        Command::Unknown(command) => assert!(matches!(command.apply().unwrap(), Frame::Ok)),
        other => panic!("expected Unknown, got {}", other.name()),
    }
    match Command::parse_from_frame(frame_args(&["cluster", "meet", "127.0.0.1", "1"])).unwrap() {
        Command::Unknown(command) => {
            assert!(
                matches!(command.apply().unwrap(), Frame::Error(message) if message.contains("disabled"))
            )
        }
        other => panic!("expected Unknown, got {}", other.name()),
    }
}

#[test]
fn command_dispatch_names_and_aof_flags_cover_wide_command_surface() {
    let cases: Vec<(Vec<&str>, &str)> = vec![
        (vec!["auth", "secret"], "AUTH"),
        (vec!["bitcount", "bits"], "BITCOUNT"),
        (vec!["bitfield", "bits", "get", "u1", "0"], "BITFIELD"),
        (vec!["bitfield_ro", "bits", "get", "u1", "0"], "BITFIELD_RO"),
        (vec!["bitop", "and", "out", "a", "b"], "BITOP"),
        (vec!["bitpos", "bits", "1"], "BITPOS"),
        (vec!["copy", "src", "dst"], "COPY"),
        (vec!["del", "key"], "DEL"),
        (vec!["expire", "key", "10"], "EXPIRE"),
        (vec!["expiretime", "key"], "EXPIRETIME"),
        (vec!["flushall"], "FLUSHALL"),
        (vec!["flushdb"], "FLUSHDB"),
        (vec!["getrange", "key", "0", "-1"], "GETRANGE"),
        (vec!["substr", "key", "0", "-1"], "SUBSTR"),
        (vec!["getdel", "key"], "GETDEL"),
        (vec!["getex", "key"], "GETEX"),
        (vec!["get", "key"], "GET"),
        (vec!["lcs", "a", "b"], "LCS"),
        (vec!["geoadd", "geo", "1", "1", "member"], "GEOADD"),
        (vec!["geodist", "geo", "a", "b"], "GEODIST"),
        (vec!["geohash", "geo", "a"], "GEOHASH"),
        (vec!["geopos", "geo", "a"], "GEOPOS"),
        (vec!["georadius", "geo", "1", "1", "1", "km"], "GEORADIUS"),
        (
            vec!["georadiusbymember", "geo", "a", "1", "km"],
            "GEORADIUSBYMEMBER",
        ),
        (
            vec!["geosearch", "geo", "frommember", "a", "byradius", "1", "km"],
            "GEOSEARCH",
        ),
        (
            vec![
                "geosearchstore",
                "dst",
                "geo",
                "frommember",
                "a",
                "byradius",
                "1",
                "km",
            ],
            "GEOSEARCHSTORE",
        ),
        (vec!["getbit", "bits", "0"], "GETBIT"),
        (vec!["ping"], "PING"),
        (vec!["pfadd", "hll", "a"], "PFADD"),
        (vec!["pfcount", "hll"], "PFCOUNT"),
        (vec!["pfmerge", "out", "hll"], "PFMERGE"),
        (vec!["pttl", "key"], "PTTL"),
        (vec!["type", "key"], "TYPE"),
        (vec!["select", "0"], "SELECT"),
        (vec!["set", "key", "value"], "SET"),
        (vec!["setbit", "bits", "0", "1"], "SETBIT"),
        (vec!["setex", "key", "1", "value"], "SETEX"),
        (vec!["setnx", "key", "value"], "SETNX"),
        (vec!["setrange", "key", "0", "value"], "SETRANGE"),
        (vec!["ttl", "key"], "TTL"),
        (vec!["randomkey"], "RANDOMKEY"),
        (vec!["rename", "src", "dst"], "RENAME"),
        (vec!["renamenx", "src", "dst"], "RENAMENX"),
        (vec!["exists", "key"], "EXISTS"),
        (vec!["strlen", "key"], "STRLEN"),
        (vec!["mset", "a", "1", "b", "2"], "MSET"),
        (
            vec!["msetex", "2", "a", "1", "b", "2", "px", "1000"],
            "MSETEX",
        ),
        (vec!["msetnx", "a", "1", "b", "2"], "MSETNX"),
        (vec!["mget", "a", "b"], "MGET"),
        (vec!["append", "key", "value"], "APPEND"),
        (vec!["dbsize"], "DBSIZE"),
        (vec!["hset", "h", "f", "v"], "HSET"),
        (vec!["hexpire", "h", "10", "fields", "1", "f"], "HEXPIRE"),
        (
            vec!["hexpireat", "h", "10", "fields", "1", "f"],
            "HEXPIREAT",
        ),
        (vec!["hexpiretime", "h", "fields", "1", "f"], "HEXPIRETIME"),
        (vec!["hget", "h", "f"], "HGET"),
        (vec!["hgetdel", "h", "fields", "1", "f"], "HGETDEL"),
        (vec!["hgetex", "h", "persist", "fields", "1", "f"], "HGETEX"),
        (vec!["hincrby", "h", "f", "1"], "HINCRBY"),
        (vec!["hincrbyfloat", "h", "f", "1.5"], "HINCRBYFLOAT"),
        (vec!["hmset", "h", "f", "v"], "HMSET"),
        (vec!["hpersist", "h", "fields", "1", "f"], "HPERSIST"),
        (vec!["hpexpire", "h", "10", "fields", "1", "f"], "HPEXPIRE"),
        (
            vec!["hpexpireat", "h", "10", "fields", "1", "f"],
            "HPEXPIREAT",
        ),
        (
            vec!["hpexpiretime", "h", "fields", "1", "f"],
            "HPEXPIRETIME",
        ),
        (vec!["hpttl", "h", "fields", "1", "f"], "HPTTL"),
        (vec!["hdel", "h", "f"], "HDEL"),
        (vec!["hexists", "h", "f"], "HEXISTS"),
        (vec!["hstrlen", "h", "f"], "HSTRLEN"),
        (vec!["keys", "*"], "KEYS"),
        (vec!["hmget", "h", "f"], "HMGET"),
        (vec!["hlen", "h"], "HLEN"),
        (vec!["hscan", "h", "0"], "HSCAN"),
        (vec!["hrandfield", "h"], "HRANDFIELD"),
        (vec!["hgetall", "h"], "HGETALL"),
        (
            vec!["hsetex", "h", "px", "1000", "fields", "1", "f", "v"],
            "HSETEX",
        ),
        (vec!["hsetnx", "h", "f", "v"], "HSETNX"),
        (vec!["httl", "h", "fields", "1", "f"], "HTTL"),
        (vec!["hkeys", "h"], "HKEYS"),
        (vec!["hvals", "h"], "HVALS"),
        (vec!["blmove", "a", "b", "left", "right", "0"], "BLMOVE"),
        (vec!["blpop", "a", "0"], "BLPOP"),
        (vec!["brpop", "a", "0"], "BRPOP"),
        (vec!["brpoplpush", "a", "b", "0"], "BRPOPLPUSH"),
        (vec!["persist", "key"], "PERSIST"),
        (vec!["lindex", "list", "0"], "LINDEX"),
        (
            vec!["linsert", "list", "before", "pivot", "value"],
            "LINSERT",
        ),
        (vec!["lmove", "a", "b", "left", "right"], "LMOVE"),
        (vec!["rpop", "list"], "RPOP"),
        (vec!["rpoplpush", "a", "b"], "RPOPLPUSH"),
        (vec!["lpop", "list"], "LPOP"),
        (vec!["lpos", "list", "value"], "LPOS"),
        (vec!["lrem", "list", "0", "value"], "LREM"),
        (vec!["llen", "list"], "LLEN"),
        (vec!["rpush", "list", "value"], "RPUSH"),
        (vec!["lmpop", "1", "list", "left"], "LMPOP"),
        (vec!["lpush", "list", "value"], "LPUSH"),
        (vec!["lpushx", "list", "value"], "LPUSHX"),
        (vec!["rpushx", "list", "value"], "RPUSHX"),
        (vec!["incr", "key"], "INCR"),
        (vec!["decr", "key"], "DECR"),
        (vec!["incrby", "key", "1"], "INCRBY"),
        (vec!["incrbyfloat", "key", "1.5"], "INCRBYFLOAT"),
        (vec!["decrby", "key", "1"], "DECRBY"),
        (vec!["echo", "value"], "ECHO"),
        (vec!["lset", "list", "0", "value"], "LSET"),
        (vec!["ltrim", "list", "0", "-1"], "LTRIM"),
        (vec!["lrange", "list", "0", "-1"], "LRANGE"),
        (vec!["multi"], "MULTI"),
        (vec!["exec"], "EXEC"),
        (vec!["discard"], "DISCARD"),
        (vec!["watch", "key"], "WATCH"),
        (vec!["unwatch"], "UNWATCH"),
        (vec!["scan", "0"], "SCAN"),
        (vec!["sscan", "set", "0"], "SSCAN"),
        (vec!["zscan", "z", "0"], "ZSCAN"),
        (vec!["touch", "key"], "TOUCH"),
        (vec!["unlink", "key"], "UNLINK"),
        (vec!["json.set", "json", "$", "{\"a\":1}"], "JSON.SET"),
        (vec!["json.get", "json"], "JSON.GET"),
        (vec!["json.del", "json"], "JSON.DEL"),
        (vec!["json.type", "json"], "JSON.TYPE"),
        (vec!["vadd", "v", "values", "2", "1", "2", "e"], "VADD"),
        (vec!["vsim", "v", "values", "2", "1", "2"], "VSIM"),
        (vec!["vrem", "v", "e"], "VREM"),
        (vec!["vcard", "v"], "VCARD"),
        (vec!["vdim", "v"], "VDIM"),
        (vec!["vemb", "v", "e"], "VEMB"),
        (vec!["vgetattr", "v", "e"], "VGETATTR"),
        (vec!["vsetattr", "v", "e", "{\"x\":1}"], "VSETATTR"),
        (vec!["vinfo", "v"], "VINFO"),
        (vec!["vrandmember", "v"], "VRANDMEMBER"),
        (vec!["vlinks", "v", "e"], "VLINKS"),
    ];

    for (args, expected_name) in cases {
        let command = Command::parse_from_frame(frame_args(&args)).unwrap_or_else(|err| {
            panic!("failed to parse {args:?}: {err}");
        });
        assert_eq!(command.name(), expected_name, "{args:?}");
        let _ = command.propagate_aof_if_needed();
    }
}

#[test]
fn newly_supported_redis_commands_are_dispatched_to_kv_engine() {
    let db = test_db();

    assert!(matches!(
        apply_command(
            &db,
            &["msetex", "2", "s1", "abcdef", "s2", "azced", "px", "10000"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["lcs", "s1", "s2", "len"]),
        Frame::Integer(3)
    ));

    assert!(matches!(
        apply_command(
            &db,
            &["hsetex", "h", "px", "10000", "fields", "1", "f", "v"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["httl", "h", "fields", "1", "f"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(ttl)) if *ttl > 0)
    ));
    assert!(matches!(
        apply_command(&db, &["hgetex", "h", "persist", "fields", "1", "f"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::BulkString(value)) if value == b"v")
    ));
    assert!(matches!(
        apply_command(&db, &["hpersist", "h", "fields", "1", "f"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(-1)))
    ));
    assert!(matches!(
        apply_command(&db, &["hexpire", "h", "10", "fields", "1", "f"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::Integer(1)))
    ));
    assert!(matches!(
        apply_command(&db, &["hgetdel", "h", "fields", "1", "f"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::BulkString(value)) if value == b"v")
    ));

    assert!(matches!(
        apply_command(&db, &["rpush", "list", "a", "b", "a", "c"]),
        Frame::Integer(4)
    ));
    assert!(matches!(
        apply_command(&db, &["lrem", "list", "1", "a"]),
        Frame::Integer(1)
    ));

    assert!(matches!(
        apply_command(&db, &["sadd", "sa", "a", "b"]),
        Frame::Integer(2)
    ));
    assert!(matches!(
        apply_command(&db, &["sadd", "sb", "b"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["sintercard", "2", "sa", "sb"]),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply_command(&db, &["smove", "sa", "sb", "a"]),
        Frame::Integer(1)
    ));

    assert!(matches!(
        apply_command(&db, &["zadd", "z", "1", "a", "2", "b", "3", "c"]),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply_command(&db, &["zrandmember", "z", "2", "withscores"]),
        Frame::Array(values) if values.len() == 4
    ));
    assert!(matches!(
        apply_command(&db, &["zrevrangebyscore", "z", "3", "1"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::BulkString(value)) if value == b"c")
    ));
}

#[test]
fn new_stream_commands_are_dispatched_to_kv_engine() {
    let db = test_db();
    let id = match apply_command(&db, &["xadd", "xs", "1-0", "f", "v"]) {
        Frame::BulkString(value) => String::from_utf8(value).unwrap(),
        other => panic!("unexpected XADD frame: {}", other),
    };
    assert!(matches!(
        apply_command(&db, &["xgroup", "create", "xs", "g", "0-0"]),
        Frame::Ok | Frame::SimpleString(_)
    ));
    let _ = apply_command(
        &db,
        &["xreadgroup", "group", "g", "c", "streams", "xs", ">"],
    );
    assert!(matches!(
        apply_command(&db, &["xackdel", "xs", "g", "ids", "1", &id]),
        Frame::Array(values)
            if matches!(values.as_slice(), [Frame::Integer(1)])
    ));
    assert!(matches!(
        apply_command(&db, &["xadd", "xs", "2-0", "f", "v2"]),
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_command(&db, &["xsetid", "xs", "2-0"]),
        Frame::Ok
    ));
    assert!(matches!(
        apply_command(&db, &["xdelex", "xs", "ids", "1", "2-0"]),
        Frame::Array(values)
            if matches!(values.as_slice(), [Frame::Integer(1)])
    ));
    assert!(matches!(
        apply_command(&db, &["xcfgset", "xs", "max-deleted-entry-id", "0-0"]),
        Frame::Error(_)
    ));
}

#[test]
#[ignore = "requires local TCP socket creation, which is denied in the current sandbox"]
fn move_command_moves_key_across_databases_in_shared_kv_engine() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let args = test_args_with_databases(2);
    let db_manager = Arc::new(rt.block_on(DatabaseManager::new_async(args.clone())));
    let session_manager = Arc::new(SessionManager::new());
    let command_executor = Arc::new(CommandExecutor::new(1, 8).unwrap());
    let wasm_registry = Arc::new(WasmRegistry::new());
    let (server_stream, _client_stream) = rt.block_on(connected_streams());
    let mut handler = Handler::new(
        db_manager.clone(),
        session_manager,
        command_executor,
        wasm_registry,
        server_stream,
        args,
    );

    db_manager.get_db(0).insert(
        "shared-key".to_string(),
        Structure::String("value".to_string()),
    );

    let frame = Move::parse_from_frame(frame_args(&["move", "shared-key", "1"]))
        .unwrap()
        .apply_sync(&handler)
        .unwrap();

    assert!(matches!(frame, Frame::Integer(1)));
    assert!(db_manager.get_db(0).get("shared-key").is_none());
    assert!(matches!(
        db_manager.get_db(1).get("shared-key"),
        Some(Structure::String(value)) if value == "value"
    ));

    handler.change_db(1).unwrap();
    let same_db_frame = Move::parse_from_frame(frame_args(&["move", "shared-key", "1"]))
        .unwrap()
        .apply_sync(&handler)
        .unwrap();
    assert!(matches!(same_db_frame, Frame::Integer(0)));
}
