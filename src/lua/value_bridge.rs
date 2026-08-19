use std::collections::HashSet;

use anyhow::{Error, Result};
use mlua::{Lua, Table, Value, Variadic};

use crate::frame::{
    Frame, MAX_ARRAY_ELEMENTS, MAX_ARRAY_NESTING_DEPTH, MAX_FRAME_BYTES, MAX_FRAME_NODES,
};

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
        Frame::Boolean(value) => Ok(Value::Boolean(value)),
        Frame::Double(value) => Ok(Value::Number(value)),
        Frame::BigNumber(value) => Ok(Value::String(lua.create_string(&value)?)),
        Frame::BlobError(bytes) => {
            error_table(lua, &String::from_utf8_lossy(&bytes)).map(Value::Table)
        }
        Frame::VerbatimString { data, .. } => Ok(Value::String(lua.create_string(&data)?)),
        Frame::Array(values) | Frame::Set(values) | Frame::Push(values) => {
            let table = lua.create_table()?;
            for (idx, value) in values.into_iter().enumerate() {
                table.set(idx + 1, frame_to_lua_value(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
        Frame::Map(entries) => {
            let table = lua.create_table()?;
            for (key, value) in entries {
                table.set(
                    frame_to_lua_value(lua, key)?,
                    frame_to_lua_value(lua, value)?,
                )?;
            }
            Ok(Value::Table(table))
        }
        Frame::Attribute { data, .. } => frame_to_lua_value(lua, *data),
    }
}

pub(crate) fn lua_value_to_frame(value: Value) -> Result<Frame> {
    let mut context = LuaFrameContext {
        active_tables: HashSet::new(),
        nodes: 0,
        bytes: 0,
    };
    lua_value_to_frame_inner(value, &mut context, 0)
}

struct LuaFrameContext {
    active_tables: HashSet<usize>,
    nodes: usize,
    bytes: usize,
}

fn lua_value_to_frame_inner(
    value: Value,
    context: &mut LuaFrameContext,
    depth: usize,
) -> Result<Frame> {
    context.nodes = context
        .nodes
        .checked_add(1)
        .filter(|nodes| *nodes <= MAX_FRAME_NODES)
        .ok_or_else(lua_response_limit_error)?;
    match value {
        Value::Nil => Ok(Frame::Null),
        Value::Boolean(true) => Ok(Frame::Integer(1)),
        Value::Boolean(false) => Ok(Frame::Null),
        Value::Integer(value) => Ok(Frame::Integer(value)),
        Value::Number(value) => Ok(Frame::Integer(value as i64)),
        Value::String(text) => {
            let bytes = text.as_bytes().to_vec();
            context.bytes = context
                .bytes
                .checked_add(bytes.len().saturating_add(32))
                .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
                .ok_or_else(lua_response_limit_error)?;
            Ok(Frame::BulkString(bytes))
        }
        Value::Table(table) => table_to_frame(table, context, depth),
        _ => Ok(Frame::Null),
    }
}

fn table_to_frame(table: Table, context: &mut LuaFrameContext, depth: usize) -> Result<Frame> {
    if depth >= MAX_ARRAY_NESTING_DEPTH {
        return Err(lua_response_limit_error());
    }
    if let Ok(err) = table.get::<String>("err") {
        context.bytes = context
            .bytes
            .checked_add(err.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(lua_response_limit_error)?;
        return Ok(Frame::Error(err));
    }
    if let Ok(ok) = table.get::<String>("ok") {
        context.bytes = context
            .bytes
            .checked_add(ok.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(lua_response_limit_error)?;
        if ok.eq_ignore_ascii_case("OK") {
            return Ok(Frame::Ok);
        }
        return Ok(Frame::SimpleString(ok));
    }
    let pointer = table.to_pointer() as usize;
    if !context.active_tables.insert(pointer) {
        return Err(Error::msg("ERR Lua table contains a cycle"));
    }
    let len = usize::try_from(table.len()?).map_err(|_| lua_response_limit_error())?;
    if len > MAX_ARRAY_ELEMENTS {
        context.active_tables.remove(&pointer);
        return Err(lua_response_limit_error());
    }
    let mut frames = Vec::with_capacity(len);
    for idx in 1..=len {
        match lua_value_to_frame_inner(table.get::<Value>(idx)?, context, depth + 1) {
            Ok(frame) => frames.push(frame),
            Err(error) => {
                context.active_tables.remove(&pointer);
                return Err(error);
            }
        }
    }
    context.active_tables.remove(&pointer);
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
    let text = value.to_string();
    if text == "-0" { "0".to_string() } else { text }
}

fn lua_response_limit_error() -> Error {
    Error::msg("ERR Lua response exceeds configured limit")
}
