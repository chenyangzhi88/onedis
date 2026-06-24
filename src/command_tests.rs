#[cfg(test)]
mod tests {
    use super::*;

    fn frame_args(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::BulkString(arg.as_bytes().to_vec()))
                .collect(),
        )
    }

    #[test]
    fn parse_dispatch_covers_aliases_and_extension_command_families() {
        let known_commands = [
            "BITFIELD_RO",
            "FT.CREATE",
            "FT._LIST",
            "FT.DROPINDEX",
            "FT.ALTER",
            "FT.ALIASADD",
            "FT.ALIASUPDATE",
            "FT.ALIASDEL",
            "FT.CONFIG",
            "FT.INFO",
            "FT.SEARCH",
            "FT.HYBRID",
            "FT.AGGREGATE",
            "FT.CURSOR",
            "FT.PROFILE",
            "FT.EXPLAIN",
            "FT.EXPLAINCLI",
            "FT.TAGVALS",
            "FT.DICTADD",
            "FT.DICTDEL",
            "FT.DICTDUMP",
            "FT.SPELLCHECK",
            "FT.SUGADD",
            "FT.SUGDEL",
            "FT.SUGGET",
            "FT.SUGLEN",
            "FT.SYNDUMP",
            "FT.SYNUPDATE",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER_RO",
            "SUBSTR",
            "XACK",
            "XACKDEL",
            "XADD",
            "XAUTOCLAIM",
            "XCFGSET",
            "XCLAIM",
            "XDEL",
            "XDELEX",
            "XGROUP",
            "XINFO",
            "XLEN",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREADGROUP",
            "XREVRANGE",
            "XSETID",
            "XTRIM",
            "EVAL",
            "EVALSHA",
            "EVAL_RO",
            "EVALSHA_RO",
            "SCRIPT",
            "WASM.LOAD",
            "WASM.CALL",
            "WASM.CALL_RO",
            "WASM.DEL",
            "WASM.SCAN",
            "WASM.LIST",
            "FUNCTION",
            "FCALL",
            "FCALL_RO",
        ];

        for command_name in known_commands {
            let parsed = Command::parse_from_frame(frame_args(&[command_name]));
            if let Ok(command) = parsed {
                assert_ne!(command.name(), "UNKNOWN", "{command_name}");
            }
        }
    }

    #[test]
    fn parse_dispatch_reports_empty_and_unknown_commands() {
        assert!(Command::parse_from_frame(Frame::Array(Vec::new())).is_err());
        assert!(matches!(
            Command::parse_from_frame(frame_args(&["definitely-not-redis"])).unwrap(),
            Command::Unknown(_)
        ));
    }

    #[test]
    fn command_name_and_aof_flags_cover_late_dispatch_variants() {
        let cases: &[(&[&str], &str, bool)] = &[
            (&["CLIENT", "HELP"], "CLIENT", false),
            (&["CONFIG", "HELP"], "CONFIG", false),
            (&["SADD", "s", "a"], "SADD", true),
            (&["SISMEMBER", "s", "a"], "SISMEMBER", false),
            (&["SMEMBERS", "s"], "SMEMBERS", false),
            (&["SCARD", "s"], "SCARD", false),
            (&["SDIFF", "s"], "SDIFF", false),
            (&["SDIFFSTORE", "dst", "s"], "SDIFFSTORE", true),
            (&["SINTER", "s"], "SINTER", false),
            (&["SINTERCARD", "1", "s"], "SINTERCARD", false),
            (&["SINTERSTORE", "dst", "s"], "SINTERSTORE", true),
            (&["SMOVE", "s", "dst", "a"], "SMOVE", true),
            (&["SPOP", "s"], "SPOP", true),
            (&["SREM", "s", "a"], "SREM", true),
            (&["FLUSHALL"], "FLUSHALL", true),
            (
                &["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"],
                "FT.CREATE",
                true,
            ),
            (&["FT._LIST"], "FT._LIST", false),
            (&["FT.DROPINDEX", "idx"], "FT.DROPINDEX", false),
            (
                &["FT.ALTER", "idx", "SCHEMA", "ADD", "body", "TEXT"],
                "FT.ALTER",
                false,
            ),
            (&["FT.ALIASADD", "alias", "idx"], "FT.ALIASADD", false),
            (&["FT.ALIASUPDATE", "alias", "idx"], "FT.ALIASUPDATE", false),
            (&["FT.ALIASDEL", "alias"], "FT.ALIASDEL", false),
            (&["FT.CONFIG", "GET", "DEFAULT_DIALECT"], "FT.CONFIG", false),
            (&["FT.INFO", "idx"], "FT.INFO", false),
            (&["FT.SEARCH", "idx", "*"], "FT.SEARCH", false),
            (
                &[
                    "FT.HYBRID",
                    "idx",
                    "*=>[KNN 1 @v $BLOB]",
                    "PARAMS",
                    "2",
                    "BLOB",
                    "[1,0]",
                ],
                "FT.HYBRID",
                false,
            ),
            (&["FT.AGGREGATE", "idx", "*"], "FT.AGGREGATE", false),
            (&["FT.CURSOR", "READ", "idx", "1"], "FT.CURSOR", false),
            (
                &["FT.PROFILE", "idx", "SEARCH", "QUERY", "*", "NOCONTENT"],
                "FT.PROFILE",
                false,
            ),
            (&["FT.EXPLAIN", "idx", "*"], "FT.EXPLAIN", false),
            (&["FT.TAGVALS", "idx", "tag"], "FT.TAGVALS", false),
            (&["FT.DICTADD", "dict", "term"], "FT.DICT", false),
            (&["FT.SPELLCHECK", "idx", "term"], "FT.SPELLCHECK", false),
            (&["FT.SUGADD", "key", "term", "1"], "FT.SUG", false),
            (&["FT.SYNUPDATE", "idx", "grp", "term"], "FT.SYN", false),
            (&["LPUSHX", "list", "v"], "LPUSHX", true),
            (&["RPUSHX", "list", "v"], "RPUSHX", true),
            (&["DECR", "n"], "DECR", true),
            (&["INCR", "n"], "INCR", true),
            (&["INCRBYFLOAT", "n", "1.5"], "INCRBYFLOAT", true),
            (&["LSET", "list", "0", "v"], "LSET", true),
            (&["LTRIM", "list", "0", "-1"], "LTRIM", true),
            (&["SUNION", "a", "b"], "SUNION", false),
            (&["SUNIONSTORE", "dst", "a", "b"], "SUNIONSTORE", true),
            (&["BLMPOP", "0", "1", "list", "LEFT"], "BLMPOP", true),
            (&["BZMPOP", "0", "1", "z", "MIN"], "BZMPOP", true),
            (&["BZPOPMAX", "z", "0"], "BZPOPMAX", true),
            (&["BZPOPMIN", "z", "0"], "BZPOPMIN", true),
            (&["ZCOUNT", "z", "-inf", "+inf"], "ZCOUNT", false),
            (&["ZADD", "z", "1", "a"], "ZADD", true),
            (&["ZDIFF", "1", "z"], "ZDIFF", false),
            (&["ZDIFFSTORE", "dst", "1", "z"], "ZDIFFSTORE", true),
            (&["ZINCRBY", "z", "1", "a"], "ZINCRBY", true),
            (&["ZINTER", "1", "z"], "ZINTER", false),
            (&["ZINTERCARD", "1", "z"], "ZINTERCARD", false),
            (&["ZINTERSTORE", "dst", "1", "z"], "ZINTERSTORE", true),
            (&["ZLEXCOUNT", "z", "-", "+"], "ZLEXCOUNT", false),
            (&["ZMPOP", "1", "z", "MIN"], "ZMPOP", true),
            (&["ZSCORE", "z", "a"], "ZSCORE", false),
            (&["ZCARD", "z"], "ZCARD", false),
            (&["ZPOPMAX", "z"], "ZPOPMAX", true),
            (&["ZPOPMIN", "z"], "ZPOPMIN", true),
            (&["ZRANDMEMBER", "z"], "ZRANDMEMBER", false),
            (&["ZRANK", "z", "a"], "ZRANK", false),
            (&["ZREM", "z", "a"], "ZREM", true),
            (&["ZREMRANGEBYLEX", "z", "-", "+"], "ZREMRANGEBYLEX", true),
            (&["ZREMRANGEBYRANK", "z", "0", "1"], "ZREMRANGEBYRANK", true),
            (
                &["ZREMRANGEBYSCORE", "z", "-inf", "+inf"],
                "ZREMRANGEBYSCORE",
                true,
            ),
            (&["ZRANGE", "z", "0", "-1"], "ZRANGE", false),
            (&["ZRANGEBYLEX", "z", "-", "+"], "ZRANGEBYLEX", false),
            (&["ZRANGESTORE", "dst", "z", "0", "-1"], "ZRANGESTORE", true),
            (&["ZREVRANGE", "z", "0", "-1"], "ZREVRANGE", false),
            (&["ZREVRANGEBYLEX", "z", "+", "-"], "ZREVRANGEBYLEX", false),
            (
                &["ZREVRANGEBYSCORE", "z", "+inf", "-inf"],
                "ZREVRANGEBYSCORE",
                false,
            ),
            (&["ZREVRANK", "z", "a"], "ZREVRANK", false),
            (
                &["ZRANGEBYSCORE", "z", "-inf", "+inf"],
                "ZRANGEBYSCORE",
                false,
            ),
            (&["ZSCAN", "z", "0"], "ZSCAN", false),
            (&["ZMSCORE", "z", "a"], "ZMSCORE", false),
            (&["ZUNION", "1", "z"], "ZUNION", false),
            (&["ZUNIONSTORE", "dst", "1", "z"], "ZUNIONSTORE", true),
            (&["XADD", "s", "*", "f", "v"], "XADD", true),
            (&["XACK", "s", "g", "1-0"], "XACK", true),
            (&["XACKDEL", "s", "g", "1-0"], "XACKDEL", true),
            (
                &["XAUTOCLAIM", "s", "g", "c", "0", "0-0"],
                "XAUTOCLAIM",
                true,
            ),
            (&["XCFGSET", "s", "MAX-DELETED-ID", "0-0"], "XCFGSET", false),
            (&["XCLAIM", "s", "g", "c", "0", "1-0"], "XCLAIM", true),
            (&["XDEL", "s", "1-0"], "XDEL", true),
            (&["XDELEX", "s", "1-0"], "XDELEX", true),
            (&["XGROUP", "CREATE", "s", "g", "$"], "XGROUP", true),
            (&["XINFO", "STREAM", "s"], "XINFO", false),
            (&["XLEN", "s"], "XLEN", false),
            (&["XPENDING", "s", "g"], "XPENDING", false),
            (&["XRANGE", "s", "-", "+"], "XRANGE", false),
            (&["XREAD", "STREAMS", "s", "0-0"], "XREAD", false),
            (
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
                "XREADGROUP",
                true,
            ),
            (&["XREVRANGE", "s", "+", "-"], "XREVRANGE", false),
            (&["XSETID", "s", "0-0"], "XSETID", true),
            (&["XTRIM", "s", "MAXLEN", "10"], "XTRIM", true),
            (&["DECRBY", "n", "2"], "DECRBY", true),
            (&["ECHO", "hello"], "ECHO", false),
            (&["EXPIREAT", "k", "1"], "EXPIREAT", true),
            (&["RANDOMKEY"], "RANDOMKEY", false),
            (&["PEXPIREAT", "k", "1"], "PEXPIREAT", true),
            (&["PEXPIRETIME", "k"], "PEXPIRETIME", false),
            (&["PEXPIRE", "k", "1"], "PEXPIRE", true),
            (&["PSETEX", "k", "1", "v"], "PSETEX", true),
            (&["LRANGE", "list", "0", "-1"], "LRANGE", false),
            (&["BGSAVE"], "BGSAVE", false),
            (&["SAVE"], "SAVE", false),
            (&["GETSET", "k", "v"], "GETSET", true),
            (&["INFO"], "INFO", false),
            (&["MOVE", "k", "1"], "MOVE", true),
            (&["SMISMEMBER", "s", "a"], "SMISMEMBER", false),
            (&["SRANDMEMBER", "s"], "SRANDMEMBER", false),
            (&["SSCAN", "s", "0"], "SSCAN", false),
            (&["TOUCH", "k"], "TOUCH", false),
            (&["UNLINK", "k"], "UNLINK", true),
            (&["MULTI"], "MULTI", false),
            (&["DISCARD"], "DISCARD", false),
            (&["EXEC"], "EXEC", false),
            (&["WATCH", "k"], "WATCH", false),
            (&["UNWATCH"], "UNWATCH", false),
            (&["VADD", "v", "VALUES", "2", "1", "0", "m"], "VADD", true),
            (&["VSIM", "v", "VALUES", "2", "1", "0"], "VSIM", false),
            (&["VREM", "v", "m"], "VREM", true),
            (&["VCARD", "v"], "VCARD", false),
            (&["VDIM", "v"], "VDIM", false),
            (&["VEMB", "v", "m"], "VEMB", false),
            (&["VGETATTR", "v", "m"], "VGETATTR", false),
            (&["VSETATTR", "v", "m", "{}"], "VSETATTR", true),
            (&["VINFO", "v"], "VINFO", false),
            (&["VRANDMEMBER", "v"], "VRANDMEMBER", false),
            (&["VLINKS", "v", "m"], "VLINKS", false),
            (&["JSON.SET", "j", "$", "{}"], "JSON.SET", true),
            (&["JSON.GET", "j"], "JSON.GET", false),
            (&["JSON.DEL", "j"], "JSON.DEL", true),
            (&["JSON.TYPE", "j"], "JSON.TYPE", false),
            (&["EVAL", "return 1", "0"], "LUA", true),
            (&["WASM.LIST"], "WASM", false),
        ];

        for (args, expected_name, expected_aof) in cases {
            let command = Command::parse_from_frame(frame_args(args))
                .unwrap_or_else(|err| panic!("{args:?} failed to parse: {err}"));
            assert_eq!(command.name(), *expected_name, "{args:?}");
            assert_eq!(command.propagate_aof_if_needed(), *expected_aof, "{args:?}");
        }

        let unsupported = Command::FtUnsupported(
            FtUnsupported::parse_from_frame(frame_args(&["FT.DEBUG", "idx"])).unwrap(),
        );
        assert_eq!(unsupported.name(), "FT.UNSUPPORTED");
        assert!(!unsupported.propagate_aof_if_needed());

        let unknown = Command::parse_from_frame(frame_args(&["NOTACOMMAND"])).unwrap();
        assert_eq!(unknown.name(), "UNKNOWN");
        assert!(!unknown.propagate_aof_if_needed());
    }
}
