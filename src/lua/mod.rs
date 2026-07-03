mod redis_api;
mod registry;
mod runtime;
mod value_bridge;

pub use registry::{LUA_REGISTRY, LuaEval, LuaRegistry, lua_registry, sha1_hex};

#[cfg(test)]
use value_bridge::{format_lua_number, lua_error_to_anyhow, lua_value_to_frame};

#[cfg(test)]
mod tests {
    use super::{LuaEval, LuaRegistry, format_lua_number, lua_error_to_anyhow, sha1_hex};
    use crate::frame::Frame;
    use crate::store::db::{Db, Structure};
    use crate::store::kv_store::KvStore;
    use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
    use mlua::{Error as LuaError, Lua, Value};
    use std::sync::Arc;

    fn test_db() -> Db {
        let unique = format!(
            "onedis-lua-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target"))
            .join("onedis-test-data")
            .join(unique);
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
    fn lua_registry_cache_exists_flush_and_kill_state_are_consistent() {
        let registry = LuaRegistry::default();
        let sha = registry.load("return 1").unwrap();
        assert_eq!(sha, sha1_hex("return 1"));
        assert_eq!(registry.get(&sha).unwrap(), Some("return 1".to_string()));
        assert_eq!(
            registry
                .exists(&[sha.clone(), "missing".to_string()])
                .unwrap(),
            vec![true, false]
        );
        registry.flush().unwrap();
        assert_eq!(registry.get(&sha).unwrap(), None);
        assert!(registry.kill().unwrap_err().to_string().contains("NOTBUSY"));

        {
            let _guard = registry.begin_execution().unwrap();
            registry.kill().unwrap();
        }
        {
            let _guard = registry.begin_execution().unwrap();
            registry.note_write().unwrap();
            assert!(
                registry
                    .kill()
                    .unwrap_err()
                    .to_string()
                    .contains("UNKILLABLE")
            );
        }
        assert!(registry.begin_execution().is_ok());
    }

    #[test]
    fn lua_eval_converts_keys_args_tables_status_errors_and_numbers() {
        let db = test_db();
        let registry = LuaRegistry::default();
        let result = registry
            .eval(
                &db,
                LuaEval {
                    script: r#"
                        return {
                            KEYS[1],
                            ARGV[1],
                            redis.status_reply('QUEUED'),
                            redis.error_reply('ERR nested'),
                            redis.sha1hex('abc'),
                            true,
                            false,
                            3.75
                        }
                    "#
                    .to_string(),
                    keys: vec!["k1".to_string()],
                    args: vec!["arg1".to_string()],
                    read_only: true,
                },
            )
            .unwrap();

        let Frame::Array(values) = result else {
            panic!("expected lua array result");
        };
        assert!(matches!(&values[0], Frame::BulkString(value) if value == b"k1"));
        assert!(matches!(&values[1], Frame::BulkString(value) if value == b"arg1"));
        assert!(matches!(&values[2], Frame::SimpleString(value) if value == "QUEUED"));
        assert!(matches!(&values[3], Frame::Error(value) if value == "ERR nested"));
        assert!(
            matches!(&values[4], Frame::BulkString(value) if value == sha1_hex("abc").as_bytes())
        );
        assert!(matches!(&values[5], Frame::Integer(1)));
        assert!(matches!(&values[6], Frame::Null));
        assert!(matches!(&values[7], Frame::Integer(3)));
    }

    #[test]
    fn lua_redis_call_pcall_readonly_and_transaction_commit_semantics() {
        let db = test_db();
        let registry = LuaRegistry::default();

        let write_result = registry
            .eval(
                &db,
                LuaEval {
                    script:
                        "redis.call('set', KEYS[1], ARGV[1]); return redis.call('get', KEYS[1])"
                            .to_string(),
                    keys: vec!["lua-key".to_string()],
                    args: vec!["value".to_string()],
                    read_only: false,
                },
            )
            .unwrap();
        assert!(matches!(write_result, Frame::BulkString(value) if value == b"value"));
        assert!(matches!(
            db.get("lua-key"),
            Some(Structure::String(value)) if value == "value"
        ));

        let readonly_error = match registry.eval(
            &db,
            LuaEval {
                script: "return redis.call('set', KEYS[1], 'blocked')".to_string(),
                keys: vec!["ro-key".to_string()],
                args: vec![],
                read_only: true,
            },
        ) {
            Ok(frame) => panic!("expected read-only lua error, got {}", frame.to_string()),
            Err(err) => err,
        };
        assert!(readonly_error.to_string().contains("read-only script"));
        assert!(db.get("ro-key").is_none());

        let protected = registry
            .eval(
                &db,
                LuaEval {
                    script: r#"
                        local bad = redis.pcall('unknown-command')
                        local ro = redis.pcall('set', KEYS[1], 'still-blocked')
                        return {bad['err'] ~= nil, ro['err'] ~= nil, redis.acl_check_cmd('get', KEYS[1]), redis.acl_check_cmd()}
                    "#
                    .to_string(),
                    keys: vec!["pcall-key".to_string()],
                    args: vec![],
                    read_only: true,
                },
            )
            .unwrap();
        assert!(matches!(
            protected,
            Frame::Array(values)
                if matches!(values.as_slice(), [
                    Frame::Integer(1),
                    Frame::Integer(1),
                    Frame::Integer(1),
                    Frame::Null,
                ])
        ));
        assert!(db.get("pcall-key").is_none());
    }

    #[test]
    fn lua_private_value_converters_and_error_formatting_cover_edges() {
        assert_eq!(format_lua_number(1.0), "1");
        assert_eq!(format_lua_number(1.25), "1.25");
        let err = lua_error_to_anyhow(LuaError::RuntimeError("line1\r\nline2".to_string()));
        assert_eq!(err.to_string(), "runtime error: line1  line2");

        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set(1, "a").unwrap();
        table.set(2, 2).unwrap();
        assert!(matches!(
            super::lua_value_to_frame(Value::Table(table)).unwrap(),
            Frame::Array(values)
                if matches!(&values[0], Frame::BulkString(value) if value == b"a")
                    && matches!(&values[1], Frame::Integer(2))
        ));

        let ok = lua.create_table().unwrap();
        ok.set("ok", "DONE").unwrap();
        assert!(matches!(
            super::lua_value_to_frame(Value::Table(ok)).unwrap(),
            Frame::SimpleString(value) if value == "DONE"
        ));

        let nil = super::lua_value_to_frame(Value::Nil).unwrap();
        assert!(matches!(nil, Frame::Null));
    }
}
