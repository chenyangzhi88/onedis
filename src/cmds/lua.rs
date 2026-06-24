use anyhow::{Error, Result};

use crate::{
    frame::Frame,
    lua::{LuaEval, lua_registry},
    store::db::Db,
};

include!("lua/command_types.rs");
include!("lua/parsing.rs");
include!("lua/script_parsing.rs");
include!("lua/apply.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        db::Db,
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    };
    use std::{path::PathBuf, sync::Arc, time::SystemTime};

    fn frame(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn test_db(prefix: &str) -> Db {
        let unique = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"))
            .join(format!("{prefix}-{unique}"));
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    #[test]
    fn lua_parser_covers_eval_evalsha_script_and_errors() {
        match LuaCommand::parse_from_frame(frame(&[
            "EVAL",
            "return KEYS[1] .. ARGV[1]",
            "1",
            "k",
            "a",
        ]))
        .unwrap()
        {
            LuaCommand::Eval(eval) => {
                assert_eq!(eval.script, "return KEYS[1] .. ARGV[1]");
                assert_eq!(eval.keys, vec!["k".to_string()]);
                assert_eq!(eval.args, vec!["a".to_string()]);
                assert!(!eval.read_only);
            }
            _ => panic!("expected EVAL"),
        }
        match LuaCommand::parse_from_frame(frame(&["EVAL_RO", "return KEYS[1]", "1", "k"])).unwrap()
        {
            LuaCommand::Eval(eval) => assert!(eval.read_only),
            _ => panic!("expected EVAL_RO"),
        }
        match LuaCommand::parse_from_frame(frame(&["EVALSHA_RO", "abc", "0", "arg"])).unwrap() {
            LuaCommand::EvalSha {
                sha,
                keys,
                args,
                read_only,
            } => {
                assert_eq!(sha, "abc");
                assert!(keys.is_empty());
                assert_eq!(args, vec!["arg".to_string()]);
                assert!(read_only);
            }
            _ => panic!("expected EVALSHA_RO"),
        }

        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "LOAD", "return 1"])).unwrap(),
            LuaCommand::ScriptLoad(_)
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "EXISTS", "a", "b"])).unwrap(),
            LuaCommand::ScriptExists(shas) if shas == vec!["a".to_string(), "b".to_string()]
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH"])).unwrap(),
            LuaCommand::ScriptFlush
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH", "ASYNC"])).unwrap(),
            LuaCommand::ScriptFlush
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "KILL"])).unwrap(),
            LuaCommand::ScriptKill
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "DEBUG", "YES"])).unwrap(),
            LuaCommand::ScriptDebug
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "HELP"])).unwrap(),
            LuaCommand::ScriptHelp
        ));

        for args in [
            vec!["EVAL", "return 1"],
            vec!["EVAL", "return 1", "bad"],
            vec!["EVAL", "return 1", "2", "only-one-key"],
            vec!["EVALSHA"],
            vec!["SCRIPT"],
            vec!["SCRIPT", "LOAD"],
            vec!["SCRIPT", "EXISTS"],
            vec!["SCRIPT", "FLUSH", "BAD"],
            vec!["SCRIPT", "FLUSH", "SYNC", "extra"],
            vec!["SCRIPT", "KILL", "extra"],
            vec!["SCRIPT", "DEBUG"],
            vec!["SCRIPT", "DEBUG", "BAD"],
            vec!["SCRIPT", "DEBUG", "YES", "extra"],
            vec!["SCRIPT", "HELP", "extra"],
            vec!["SCRIPT", "NOPE"],
            vec!["UNKNOWN"],
        ] {
            assert!(
                LuaCommand::parse_from_frame(frame(&args)).is_err(),
                "{args:?} should fail"
            );
        }
    }

    #[test]
    fn lua_command_apply_covers_script_cache_and_help_paths() {
        let db = test_db("onedis-lua-cmd");
        let sha = match LuaCommand::parse_from_frame(frame(&["SCRIPT", "LOAD", "return 'cached'"]))
            .unwrap()
            .apply(&db)
            .unwrap()
        {
            Frame::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected sha bulk, got {}", other.to_string()),
        };
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "EXISTS", &sha, "missing"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(1), Frame::Integer(0)])
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["EVALSHA", &sha, "0"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::BulkString(bytes) if bytes == b"cached"
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "HELP"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Array(values) if !values.is_empty()
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "DEBUG", "NO"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Ok
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH", "SYNC"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Ok
        ));
        assert!(
            LuaCommand::parse_from_frame(frame(&["EVALSHA", &sha, "0"]))
                .unwrap()
                .apply(&db)
                .is_err()
        );
    }
}
