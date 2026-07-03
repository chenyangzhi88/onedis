use std::sync::Arc;

use anyhow::Result;
use mlua::{Lua, Value, Variadic};

use crate::{command::Command, frame::Frame, store::db::Db};

use super::{
    registry::{lua_registry, sha1_hex},
    value_bridge::{command_frame_from_lua, error_table, frame_to_lua_value, status_table},
};

pub(super) fn install_redis_api(lua: &Lua, db: Arc<Db>, read_only: bool) -> Result<()> {
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
    match crate::command_dispatch::handle_command(&db, command) {
        Ok(Frame::Error(err)) if protected => error_table(lua, &err).map(Value::Table),
        Ok(Frame::Error(err)) => Err(mlua::Error::runtime(err)),
        Ok(frame) => frame_to_lua_value(lua, frame),
        Err(err) if protected => error_table(lua, &err.to_string()).map(Value::Table),
        Err(err) => Err(mlua::Error::runtime(err.to_string())),
    }
}
