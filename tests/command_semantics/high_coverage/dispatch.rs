#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_db_dispatch_matrix_covers_remaining_command_families() {
    let db = test_db("command-semantics-async-matrix");

    for args in [
        &["SET", "s1", "abcdef"][..],
        &["SETEX", "s2", "20", "v"],
        &["SETNX", "s3", "v"],
        &["PSETEX", "s4", "20000", "v"],
        &["MSET", "m1", "1", "m2", "2"],
        &["MGET", "m1", "m2", "missing"],
        &["MSETNX", "m3", "3", "m4", "4"],
        &["MSETEX", "2", "m5", "5", "m6", "6", "EX", "20"],
        &["APPEND", "s1", "g"],
        &["SETRANGE", "s1", "2", "ZZ"],
        &["SETBIT", "bits", "7", "1"],
        &["GETBIT", "bits", "7"],
        &["BITCOUNT", "bits"],
        &["BITFIELD", "bits", "INCRBY", "i8", "0", "1"],
        &["BITFIELD_RO", "bits", "GET", "i8", "0"],
        &["BITPOS", "bits", "1"],
        &["BITOP", "AND", "bits-dst", "bits"],
        &["GET", "s1"],
        &["GETRANGE", "s1", "0", "-1"],
        &["GETSET", "s1", "new"],
        &["GETEX", "s1", "PERSIST"],
        &["STRLEN", "s1"],
        &["INCR", "n"],
        &["INCRBY", "n", "2"],
        &["DECR", "n"],
        &["DECRBY", "n", "1"],
        &["INCRBYFLOAT", "nf", "1.5"],
        &["LCS", "s1", "s2"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["EXPIRE", "s1", "20"][..],
        &["EXPIREAT", "s1", "4102444800"],
        &["PEXPIRE", "s1", "20000"],
        &["PEXPIREAT", "s1", "4102444800000"],
        &["EXPIRETIME", "s1"],
        &["PEXPIRETIME", "s1"],
        &["TTL", "s1"],
        &["PTTL", "s1"],
        &["PERSIST", "s1"],
        &["TOUCH", "s1", "missing"],
        &["EXISTS", "s1", "missing"],
        &["TYPE", "s1"],
        &["KEYS", "*"],
        &["SCAN", "0", "COUNT", "5"],
        &["RANDOMKEY"],
        &["DBSIZE"],
        &["COPY", "s1", "s-copy", "REPLACE"],
        &["RENAME", "s-copy", "s-renamed"],
        &["RENAMENX", "s-renamed", "s-renamed2"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["HSET", "h", "a", "1", "b", "2"][..],
        &["HSETNX", "h", "c", "3"],
        &["HMSET", "h", "d", "4"],
        &["HGET", "h", "a"],
        &["HMGET", "h", "a", "missing"],
        &["HGETALL", "h"],
        &["HKEYS", "h"],
        &["HVALS", "h"],
        &["HLEN", "h"],
        &["HEXISTS", "h", "a"],
        &["HSTRLEN", "h", "a"],
        &["HINCRBY", "h", "n", "1"],
        &["HINCRBYFLOAT", "h", "f", "1.25"],
        &["HRANDFIELD", "h", "2", "WITHVALUES"],
        &["HSCAN", "h", "0", "COUNT", "5"],
        &["HSETEX", "h", "EX", "20", "FIELDS", "1", "ttl", "v"],
        &["HGETEX", "h", "PERSIST", "FIELDS", "1", "ttl"],
        &["HEXPIRE", "h", "20", "FIELDS", "1", "a"],
        &["HPEXPIRE", "h", "20000", "FIELDS", "1", "b"],
        &["HEXPIREAT", "h", "4102444800", "FIELDS", "1", "c"],
        &["HPEXPIREAT", "h", "4102444800000", "FIELDS", "1", "d"],
        &["HTTL", "h", "FIELDS", "1", "a"],
        &["HPTTL", "h", "FIELDS", "1", "b"],
        &["HEXPIRETIME", "h", "FIELDS", "1", "c"],
        &["HPEXPIRETIME", "h", "FIELDS", "1", "d"],
        &["HPERSIST", "h", "FIELDS", "1", "a"],
        &["HGETDEL", "h", "FIELDS", "1", "missing"],
        &["HDEL", "h", "missing"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["RPUSH", "list", "a", "b", "c"][..],
        &["LPUSH", "list", "z"],
        &["LPUSHX", "list", "y"],
        &["RPUSHX", "list", "d"],
        &["LLEN", "list"],
        &["LINDEX", "list", "0"],
        &["LRANGE", "list", "0", "-1"],
        &["LPOS", "list", "a"],
        &["LSET", "list", "0", "x"],
        &["LINSERT", "list", "AFTER", "x", "after"],
        &["LREM", "list", "0", "missing"],
        &["LMOVE", "list", "list2", "LEFT", "RIGHT"],
        &["RPOPLPUSH", "list", "list2"],
        &["LPOP", "list"],
        &["RPOP", "list"],
        &["LTRIM", "list", "0", "-1"],
        &["LMPOP", "1", "list2", "LEFT", "COUNT", "1"],
        &["BLMPOP", "0", "1", "list2", "LEFT", "COUNT", "1"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["SADD", "set", "a", "b", "c"][..],
        &["SCARD", "set"],
        &["SISMEMBER", "set", "a"],
        &["SMISMEMBER", "set", "a", "x"],
        &["SMEMBERS", "set"],
        &["SRANDMEMBER", "set", "2"],
        &["SPOP", "set", "1"],
        &["SADD", "set2", "b", "c", "d"],
        &["SDIFF", "set2", "set"],
        &["SDIFFSTORE", "set-diff", "set2", "set"],
        &["SINTER", "set", "set2"],
        &["SINTERCARD", "2", "set", "set2"],
        &["SINTERSTORE", "set-inter", "set", "set2"],
        &["SUNION", "set", "set2"],
        &["SUNIONSTORE", "set-union", "set", "set2"],
        &["SMOVE", "set2", "set", "d"],
        &["SSCAN", "set", "0", "COUNT", "5"],
        &["SREM", "set", "missing"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["ZADD", "z", "1", "a", "2", "b", "3", "c"][..],
        &["ZCARD", "z"],
        &["ZCOUNT", "z", "-inf", "+inf"],
        &["ZINCRBY", "z", "1", "a"],
        &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
        &["ZRANGEBYLEX", "z", "-", "+"],
        &["ZRANGEBYSCORE", "z", "-inf", "+inf"],
        &["ZREVRANGE", "z", "0", "-1"],
        &["ZREVRANGEBYLEX", "z", "+", "-"],
        &["ZREVRANGEBYSCORE", "z", "+inf", "-inf"],
        &["ZRANK", "z", "a"],
        &["ZREVRANK", "z", "a"],
        &["ZSCORE", "z", "a"],
        &["ZMSCORE", "z", "a", "missing"],
        &["ZRANDMEMBER", "z", "2", "WITHSCORES"],
        &["ZLEXCOUNT", "z", "-", "+"],
        &["ZSCAN", "z", "0", "COUNT", "5"],
        &["ZADD", "z2", "2", "b", "4", "d"],
        &["ZDIFF", "1", "z"],
        &["ZDIFFSTORE", "zdiff", "1", "z"],
        &["ZINTER", "2", "z", "z2"],
        &["ZINTERCARD", "2", "z", "z2", "LIMIT", "10"],
        &["ZINTERSTORE", "zinter", "2", "z", "z2"],
        &["ZUNION", "2", "z", "z2"],
        &["ZUNIONSTORE", "zunion", "2", "z", "z2"],
        &["ZRANGESTORE", "zstore", "z", "0", "-1"],
        &["ZPOPMIN", "z", "1"],
        &["ZPOPMAX", "z", "1"],
        &["ZADD", "zblock", "1", "a"],
        &["BZPOPMIN", "zblock", "0"],
        &["ZADD", "zblock", "1", "a"],
        &["BZPOPMAX", "zblock", "0"],
        &["ZADD", "zmpop", "1", "a", "2", "b"],
        &["ZMPOP", "1", "zmpop", "MIN", "COUNT", "1"],
        &["ZADD", "bzmpop", "1", "a"],
        &["BZMPOP", "0", "1", "bzmpop", "MAX", "COUNT", "1"],
        &["ZREM", "z", "missing"],
        &["ZREMRANGEBYLEX", "z", "-", "+"],
        &["ZREMRANGEBYRANK", "z2", "0", "0"],
        &["ZREMRANGEBYSCORE", "z2", "-inf", "+inf"],
    ] {
        expect_async_ok(&db, args).await;
    }

    for args in [
        &["PFADD", "hll", "a", "b"][..],
        &["PFCOUNT", "hll"],
        &["PFADD", "hll2", "c"],
        &["PFMERGE", "hll-merged", "hll", "hll2"],
        &["GEOADD", "geo", "13.361389", "38.115556", "Palermo"],
        &["GEODIST", "geo", "Palermo", "Palermo", "km"],
        &["GEOHASH", "geo", "Palermo"],
        &["GEOPOS", "geo", "Palermo"],
        &[
            "GEOSEARCH",
            "geo",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
        ],
        &[
            "GEOSEARCHSTORE",
            "geo-dst",
            "geo",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
        ],
        &["GEORADIUS", "geo", "15", "37", "200", "km"],
        &["GEORADIUSBYMEMBER", "geo", "Palermo", "200", "km"],
        &["JSON.SET", "json", "$", r#"{"a":1,"b":[1,2]}"#],
        &["JSON.GET", "json", "$.a"],
        &["JSON.TYPE", "json", "$.b"],
        &["JSON.DEL", "json", "$.a"],
    ] {
        expect_async_ok(&db, args).await;
    }

    assert!(matches!(
        expect_async_ok(&db, &["FLUSHDB"]).await,
        Frame::Ok
    ));
}

