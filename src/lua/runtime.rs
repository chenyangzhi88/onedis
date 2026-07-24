use std::sync::{Arc, Mutex};

use anyhow::Result;
use mlua::{HookTriggers, Lua, Table, Value, VmState};

use crate::{frame::Frame, store::db::Db};

use super::{
    LuaCommandAuthorizer,
    redis_api::install_redis_api,
    registry::{LuaEval, lua_registry},
    value_bridge::{lua_error_to_anyhow, lua_value_to_frame},
};

const DEFAULT_LUA_INSTRUCTION_BUDGET: u64 = 1_000_000;
const LUA_HOOK_INTERVAL: u32 = 1_000;

pub(super) fn run_lua_script(
    db: Arc<Db>,
    eval: &LuaEval,
    authorizer: Option<LuaCommandAuthorizer>,
) -> Result<Frame> {
    let lua = Lua::new();
    install_instruction_budget(&lua)?;
    install_safe_globals(&lua)?;
    install_keys_and_args(&lua, &eval.keys, &eval.args)?;
    install_redis_api(&lua, db, eval.read_only, authorizer)?;
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
