use anyhow::{Error, Result};
use mlua::{Lua, Table, Value, Variadic};

use crate::frame::Frame;

pub(super) fn command_frame_from_lua(lua: &Lua, args: Variadic<Value>) -> mlua::Result<Frame> {
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

pub(super) fn frame_to_lua_value(lua: &Lua, frame: Frame) -> mlua::Result<Value> {
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

pub(crate) fn lua_value_to_frame(value: Value) -> Result<Frame> {
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

pub(super) fn status_table(lua: &Lua, message: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("ok", message)?;
    Ok(table)
}

pub(super) fn error_table(lua: &Lua, message: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("err", message)?;
    Ok(table)
}

pub(crate) fn lua_error_to_anyhow(error: mlua::Error) -> Error {
    Error::msg(error.to_string().replace(['\r', '\n'], " "))
}

pub(crate) fn format_lua_number(value: f64) -> String {
    let text = format!("{value:.17}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}
