use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::{Error, Result};
use mlua::{HookTriggers, Lua, Table, Value, Variadic, VmState};
use sha1_smol::Sha1;

use crate::{command::Command, frame::Frame, store::db::Db};

const DEFAULT_LUA_INSTRUCTION_BUDGET: u64 = 1_000_000;
const LUA_HOOK_INTERVAL: u32 = 1_000;

#[derive(Default)]
pub struct LuaRegistry {
    scripts: Mutex<HashMap<String, String>>,
    execution: Mutex<LuaExecutionState>,
}

#[derive(Default)]
struct LuaExecutionState {
    active: bool,
    kill_requested: bool,
    write_seen: bool,
}

struct LuaExecutionGuard<'a> {
    registry: &'a LuaRegistry,
}

impl Drop for LuaExecutionGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut execution) = self.registry.execution.lock() {
            execution.active = false;
            execution.kill_requested = false;
            execution.write_seen = false;
        }
    }
}

pub static LUA_REGISTRY: OnceLock<LuaRegistry> = OnceLock::new();

pub fn lua_registry() -> &'static LuaRegistry {
    LUA_REGISTRY.get_or_init(LuaRegistry::default)
}

pub struct LuaEval {
    pub script: String,
    pub keys: Vec<String>,
    pub args: Vec<String>,
    pub read_only: bool,
}

impl LuaRegistry {
    pub fn load(&self, script: &str) -> Result<String> {
        let sha = sha1_hex(script);
        self.scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .insert(sha.clone(), script.to_string());
        Ok(sha)
    }

    pub fn get(&self, sha: &str) -> Result<Option<String>> {
        Ok(self
            .scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .get(sha)
            .cloned())
    }

    pub fn exists(&self, shas: &[String]) -> Result<Vec<bool>> {
        let scripts = self
            .scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?;
        Ok(shas.iter().map(|sha| scripts.contains_key(sha)).collect())
    }

    pub fn flush(&self) -> Result<()> {
        self.scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .clear();
        Ok(())
    }

    pub fn kill(&self) -> Result<()> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if !execution.active {
            return Err(Error::msg("NOTBUSY No scripts in execution right now."));
        }
        if execution.write_seen {
            return Err(Error::msg(
                "UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.",
            ));
        }
        execution.kill_requested = true;
        Ok(())
    }

    pub fn eval(&self, db: &Db, eval: LuaEval) -> Result<Frame> {
        self.load(&eval.script)?;
        let _guard = self.begin_execution()?;
        let txn_db = Arc::new(db.transactional_view()?);
        let result = match run_lua_script(txn_db.clone(), &eval) {
            Ok(result) => result,
            Err(err) => {
                txn_db.discard_transaction();
                return Err(err);
            }
        };
        if eval.read_only {
            txn_db.discard_transaction();
        } else {
            txn_db.commit_transaction()?;
        }
        Ok(result)
    }

    fn begin_execution(&self) -> Result<LuaExecutionGuard<'_>> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if execution.active {
            return Err(Error::msg(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            ));
        }
        execution.active = true;
        execution.kill_requested = false;
        execution.write_seen = false;
        Ok(LuaExecutionGuard { registry: self })
    }

    fn note_write(&self) -> Result<()> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if execution.active {
            execution.write_seen = true;
        }
        Ok(())
    }

    fn kill_requested(&self) -> Result<bool> {
        Ok(self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?
            .kill_requested)
    }
}

pub fn sha1_hex(script: &str) -> String {
    let mut sha = Sha1::new();
    sha.update(script.as_bytes());
    sha.digest().to_string()
}

fn run_lua_script(db: Arc<Db>, eval: &LuaEval) -> Result<Frame> {
    let lua = Lua::new();
    install_instruction_budget(&lua)?;
    install_safe_globals(&lua)?;
    install_keys_and_args(&lua, &eval.keys, &eval.args)?;
    install_redis_api(&lua, db, eval.read_only)?;
    let value = lua
        .load(&eval.script)
        .eval::<Value>()
        .map_err(lua_error_to_anyhow)?;
    lua_value_to_frame(value)
}

fn install_instruction_budget(lua: &Lua) -> Result<()> {
    let budget = std::env::var("ONEDIS_LUA_INSTRUCTION_BUDGET")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LUA_INSTRUCTION_BUDGET);
    let remaining = Arc::new(Mutex::new(budget));
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(LUA_HOOK_INTERVAL),
        move |_, _| {
            let mut remaining = remaining
                .lock()
                .map_err(|_| mlua::Error::runtime("ERR lua hook lock poisoned"))?;
            if *remaining <= LUA_HOOK_INTERVAL as u64 {
                return Err(mlua::Error::runtime(
                    "ERR Lua script exceeded instruction limit",
                ));
            }
            if lua_registry()
                .kill_requested()
                .map_err(|err| mlua::Error::runtime(err.to_string()))?
            {
                return Err(mlua::Error::runtime(
                    "ERR Script killed by user with SCRIPT KILL.",
                ));
            }
            *remaining -= LUA_HOOK_INTERVAL as u64;
            Ok(VmState::Continue)
        },
    )?;
    Ok(())
}

fn install_safe_globals(lua: &Lua) -> Result<()> {
    let globals = lua.globals();
    for name in ["dofile", "loadfile", "require", "module", "collectgarbage"] {
        globals.set(name, Value::Nil)?;
    }
    for name in ["io", "os", "package", "debug"] {
        globals.set(name, Value::Nil)?;
    }
    Ok(())
}

fn install_keys_and_args(lua: &Lua, keys: &[String], args: &[String]) -> Result<()> {
    lua.globals().set("KEYS", strings_table(lua, keys)?)?;
    lua.globals().set("ARGV", strings_table(lua, args)?)?;
    Ok(())
}

fn strings_table(lua: &Lua, values: &[String]) -> Result<Table> {
    let table = lua.create_table()?;
    for (idx, value) in values.iter().enumerate() {
        table.set(idx + 1, value.as_str())?;
    }
    Ok(table)
}

fn install_redis_api(lua: &Lua, db: Arc<Db>, read_only: bool) -> Result<()> {
    let redis = lua.create_table()?;
    redis.set("LOG_DEBUG", 0)?;
    redis.set("LOG_VERBOSE", 1)?;
    redis.set("LOG_NOTICE", 2)?;
    redis.set("LOG_WARNING", 3)?;
    redis.set("REDIS_VERSION", env!("CARGO_PKG_VERSION"))?;
    redis.set("REDIS_VERSION_NUM", 0)?;
    let call_db = db.clone();
    redis.set(
        "call",
        lua.create_function(move |lua, args: Variadic<Value>| {
            redis_call(lua, call_db.clone(), args, read_only, false)
        })?,
    )?;
    let pcall_db = db.clone();
    redis.set(
        "pcall",
        lua.create_function(move |lua, args: Variadic<Value>| {
            redis_call(lua, pcall_db.clone(), args, read_only, true)
        })?,
    )?;
    redis.set(
        "status_reply",
        lua.create_function(|lua, message: String| status_table(lua, &message))?,
    )?;
    redis.set(
        "error_reply",
        lua.create_function(|lua, message: String| error_table(lua, &message))?,
    )?;
    redis.set(
        "sha1hex",
        lua.create_function(|_, message: String| Ok(sha1_hex(&message)))?,
    )?;
    redis.set(
        "acl_check_cmd",
        lua.create_function(move |lua, args: Variadic<Value>| {
            Ok(command_frame_from_lua(lua, args)
                .and_then(|frame| {
                    Command::parse_from_frame(frame)
                        .map_err(|err| mlua::Error::runtime(err.to_string()))
                })
                .is_ok())
        })?,
    )?;
    redis.set("log", lua.create_function(|_, _: Variadic<Value>| Ok(()))?)?;
    redis.set("setresp", lua.create_function(|_, _: Value| Ok(()))?)?;
    redis.set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.set(
        "breakpoint",
        lua.create_function(|_, _: Variadic<Value>| Ok(()))?,
    )?;
    redis.set(
        "debug",
        lua.create_function(|_, _: Variadic<Value>| Ok(()))?,
    )?;
    lua.globals().set("redis", redis)?;
    Ok(())
}

fn redis_call(
    lua: &Lua,
    db: Arc<Db>,
    args: Variadic<Value>,
    read_only: bool,
    protected: bool,
) -> mlua::Result<Value> {
    let frame = match command_frame_from_lua(lua, args) {
        Ok(frame) => frame,
        Err(err) if protected => return error_table(lua, &err.to_string()).map(Value::Table),
        Err(err) => return Err(mlua::Error::runtime(err.to_string())),
    };
    let command = match Command::parse_from_frame(frame) {
        Ok(command) => command,
        Err(err) if protected => return error_table(lua, &err.to_string()).map(Value::Table),
        Err(err) => return Err(mlua::Error::runtime(err.to_string())),
    };
    let is_write = command.propagate_aof_if_needed();
    if read_only && is_write {
        let err = "ERR write command is not allowed from read-only script";
        if protected {
            return error_table(lua, err).map(Value::Table);
        }
        return Err(mlua::Error::runtime(err));
    }
    if is_write {
        lua_registry()
            .note_write()
            .map_err(|err| mlua::Error::runtime(err.to_string()))?;
    }
    match db.handle_command(command) {
        Ok(Frame::Error(err)) if protected => error_table(lua, &err).map(Value::Table),
        Ok(Frame::Error(err)) => Err(mlua::Error::runtime(err)),
        Ok(frame) => frame_to_lua_value(lua, frame),
        Err(err) if protected => error_table(lua, &err.to_string()).map(Value::Table),
        Err(err) => Err(mlua::Error::runtime(err.to_string())),
    }
}

fn command_frame_from_lua(lua: &Lua, args: Variadic<Value>) -> mlua::Result<Frame> {
    if args.is_empty() {
        return Err(mlua::Error::runtime(
            "ERR wrong number of arguments for redis.call",
        ));
    }
    let mut frames = Vec::with_capacity(args.len());
    for value in args {
        frames.push(lua_arg_to_frame(lua, value)?);
    }
    Ok(Frame::Array(frames))
}

fn lua_arg_to_frame(lua: &Lua, value: Value) -> mlua::Result<Frame> {
    match value {
        Value::String(text) => Ok(Frame::BulkString(text.as_bytes().to_vec())),
        Value::Integer(value) => Ok(Frame::bulk_string(value.to_string())),
        Value::Number(value) => Ok(Frame::bulk_string(format_lua_number(value))),
        Value::Boolean(value) => Ok(Frame::bulk_string(if value { "1" } else { "0" })),
        Value::Nil => Ok(Frame::BulkString(Vec::new())),
        other => {
            let text = lua
                .globals()
                .get::<mlua::Function>("tostring")?
                .call::<String>(other)?;
            Ok(Frame::bulk_string(text))
        }
    }
}

fn frame_to_lua_value(lua: &Lua, frame: Frame) -> mlua::Result<Value> {
    match frame {
        Frame::Ok => status_table(lua, "OK").map(Value::Table),
        Frame::SimpleString(text) => status_table(lua, &text).map(Value::Table),
        Frame::Error(text) => error_table(lua, &text).map(Value::Table),
        Frame::Integer(value) => Ok(Value::Integer(value)),
        Frame::BulkString(bytes) => Ok(Value::String(lua.create_string(&bytes)?)),
        Frame::Null => Ok(Value::Boolean(false)),
        Frame::Array(values) => {
            let table = lua.create_table()?;
            for (idx, value) in values.into_iter().enumerate() {
                table.set(idx + 1, frame_to_lua_value(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
        Frame::RDBFile(bytes) => Ok(Value::String(lua.create_string(&bytes)?)),
    }
}

fn lua_value_to_frame(value: Value) -> Result<Frame> {
    match value {
        Value::Nil => Ok(Frame::Null),
        Value::Boolean(true) => Ok(Frame::Integer(1)),
        Value::Boolean(false) => Ok(Frame::Null),
        Value::Integer(value) => Ok(Frame::Integer(value)),
        Value::Number(value) => Ok(Frame::Integer(value as i64)),
        Value::String(text) => Ok(Frame::BulkString(text.as_bytes().to_vec())),
        Value::Table(table) => table_to_frame(table),
        _ => Ok(Frame::Null),
    }
}

fn table_to_frame(table: Table) -> Result<Frame> {
    if let Ok(err) = table.get::<String>("err") {
        return Ok(Frame::Error(err));
    }
    if let Ok(ok) = table.get::<String>("ok") {
        if ok.eq_ignore_ascii_case("OK") {
            return Ok(Frame::Ok);
        }
        return Ok(Frame::SimpleString(ok));
    }
    let len = table.len()? as usize;
    let mut frames = Vec::with_capacity(len);
    for idx in 1..=len {
        frames.push(lua_value_to_frame(table.get::<Value>(idx)?)?);
    }
    Ok(Frame::Array(frames))
}

fn status_table(lua: &Lua, message: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("ok", message)?;
    Ok(table)
}

fn error_table(lua: &Lua, message: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("err", message)?;
    Ok(table)
}

fn lua_error_to_anyhow(error: mlua::Error) -> Error {
    Error::msg(error.to_string().replace(['\r', '\n'], " "))
}

fn format_lua_number(value: f64) -> String {
    let text = format!("{value:.17}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

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
