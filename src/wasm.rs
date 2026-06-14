use std::sync::Arc;

use anyhow::{Error, Result};
use dashmap::DashMap;
use wasmtime::{
    Caller, Config, Engine, Extern, Instance, Linker, Module, ResourceLimiter, Store, Val, ValType,
};

use crate::store::db::{Db, SetCondition, SetExpiration, SetOutcome};

const DEFAULT_WASM_FUEL: u64 = 10_000_000;
const DEFAULT_WASM_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const MAX_WASM_MODULE_BYTES: usize = 16 * 1024 * 1024;
const WASM_SCAN_KEY_OFFSET: usize = 0;
const WASM_SCAN_VALUE_OFFSET: usize = 64 * 1024;
const WASM_SCAN_MAX_FIELD_BYTES: usize = 64 * 1024;
const WASM_ARG_OFFSET: usize = 4096;
const WASM_ARG_MAX_TOTAL_BYTES: usize = 256 * 1024;

const WASM_OK_NIL: i32 = -1;
const WASM_ERR_MEMORY: i32 = -2;
const WASM_ERR_READONLY: i32 = -3;
const WASM_ERR_UNSUPPORTED: i32 = -5;
const WASM_ERR_DB: i32 = -6;

#[derive(Clone)]
pub struct WasmRegistry {
    engine: Engine,
    modules: DashMap<String, Module>,
    fuel_per_call: u64,
    max_memory_bytes: usize,
}

impl WasmRegistry {
    pub fn new() -> Self {
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config).expect("failed to create wasm engine");
        let fuel_per_call = std::env::var("ONEDIS_WASM_FUEL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_WASM_FUEL);
        let max_memory_bytes = std::env::var("ONEDIS_WASM_MAX_MEMORY_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WASM_MAX_MEMORY_BYTES);
        Self {
            engine,
            modules: DashMap::new(),
            fuel_per_call,
            max_memory_bytes,
        }
    }

    pub fn load(&self, name: &str, bytes: &[u8]) -> Result<()> {
        validate_name(name)?;
        if bytes.len() > MAX_WASM_MODULE_BYTES {
            return Err(Error::msg("ERR wasm module is too large"));
        }
        let module = Module::new(&self.engine, bytes)
            .map_err(|error| Error::msg(format!("ERR wasm compile failed: {error}")))?;
        validate_imports(&module)?;
        self.modules.insert(name.to_string(), module);
        Ok(())
    }

    pub fn delete(&self, name: &str) -> bool {
        self.modules.remove(name).is_some()
    }

    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .modules
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        names.sort();
        names
    }

    pub async fn call(
        &self,
        db: Arc<Db>,
        name: &str,
        function: &str,
        args: &[String],
        read_only: bool,
    ) -> Result<Vec<WasmValue>> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| Error::msg("ERR wasm module not found"))?
            .clone();
        let mut store = Store::new(
            &self.engine,
            WasmHostContext {
                db,
                read_only,
                host_error: false,
                limits: WasmLimits::new(self.max_memory_bytes),
            },
        );
        store.limiter(|context| &mut context.limits);
        store.set_fuel(self.fuel_per_call)?;
        let linker = host_linker(&self.engine)?;
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm instantiate failed: {error}")))?;
        let func = instance
            .get_func(&mut store, function)
            .ok_or_else(|| Error::msg("ERR wasm function not found"))?;
        let func_type = func.ty(&store);
        let params = func_type.params().collect::<Vec<_>>();
        let results = func_type.results();
        let inputs = prepare_call_inputs(&mut store, &instance, &params, args)?;
        let mut outputs = results
            .map(|ty| {
                Val::default_for_ty(&ty)
                    .ok_or_else(|| Error::msg("ERR wasm result type is not supported"))
            })
            .collect::<Result<Vec<_>>>()?;
        func.call_async(&mut store, &inputs, &mut outputs)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm call failed: {error}")))?;
        if store.data().host_error {
            return Err(Error::msg("ERR wasm host function failed"));
        }
        outputs.into_iter().map(WasmValue::from_val).collect()
    }

    pub async fn scan(
        &self,
        db: Arc<Db>,
        name: &str,
        function: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| Error::msg("ERR wasm module not found"))?
            .clone();
        let mut store = Store::new(
            &self.engine,
            WasmHostContext {
                db: db.clone(),
                read_only: true,
                host_error: false,
                limits: WasmLimits::new(self.max_memory_bytes),
            },
        );
        store.limiter(|context| &mut context.limits);
        store.set_fuel(self.fuel_per_call)?;
        let linker = host_linker(&self.engine)?;
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm instantiate failed: {error}")))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::msg("ERR wasm module must export memory for scan"))?;
        let func = instance
            .get_func(&mut store, function)
            .ok_or_else(|| Error::msg("ERR wasm function not found"))?;
        let func_type = func.ty(&store);
        let params = func_type.params().collect::<Vec<_>>();
        let results = func_type.results().collect::<Vec<_>>();
        if !matches!(
            params.as_slice(),
            [ValType::I32, ValType::I32, ValType::I32, ValType::I32]
        ) || !matches!(results.as_slice(), [ValType::I32])
        {
            return Err(Error::msg(
                "ERR wasm scan function must be (i32,i32,i32,i32)->i32",
            ));
        }

        let rows = db.scan_string_prefix_async(prefix, limit).await;
        let mut matched = Vec::new();
        for (key, value) in rows {
            if key.len() > WASM_SCAN_MAX_FIELD_BYTES || value.len() > WASM_SCAN_MAX_FIELD_BYTES {
                continue;
            }
            memory
                .write(&mut store, WASM_SCAN_KEY_OFFSET, key.as_bytes())
                .map_err(|_| Error::msg("ERR wasm scan key does not fit in memory"))?;
            memory
                .write(&mut store, WASM_SCAN_VALUE_OFFSET, &value)
                .map_err(|_| Error::msg("ERR wasm scan value does not fit in memory"))?;
            let inputs = [
                Val::I32(WASM_SCAN_KEY_OFFSET as i32),
                Val::I32(key.len() as i32),
                Val::I32(WASM_SCAN_VALUE_OFFSET as i32),
                Val::I32(value.len() as i32),
            ];
            let mut outputs = [Val::I32(0)];
            func.call_async(&mut store, &inputs, &mut outputs)
                .await
                .map_err(|error| Error::msg(format!("ERR wasm scan call failed: {error}")))?;
            if matches!(outputs[0], Val::I32(value) if value != 0) {
                matched.push(key);
            }
        }
        Ok(matched)
    }
}

impl Default for WasmRegistry {
    fn default() -> Self {
        Self::new()
    }
}

struct WasmHostContext {
    db: Arc<Db>,
    read_only: bool,
    host_error: bool,
    limits: WasmLimits,
}

struct WasmLimits {
    max_memory_bytes: usize,
}

impl WasmLimits {
    fn new(max_memory_bytes: usize) -> Self {
        Self { max_memory_bytes }
    }
}

impl ResourceLimiter for WasmLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= self.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= 1024)
    }

    fn instances(&self) -> usize {
        4
    }

    fn tables(&self) -> usize {
        4
    }

    fn memories(&self) -> usize {
        4
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WasmValue {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl WasmValue {
    fn from_val(value: Val) -> Result<Self> {
        match value {
            Val::I32(value) => Ok(Self::I32(value)),
            Val::I64(value) => Ok(Self::I64(value)),
            Val::F32(value) => Ok(Self::F32(f32::from_bits(value))),
            Val::F64(value) => Ok(Self::F64(f64::from_bits(value))),
            Val::V128(_)
            | Val::FuncRef(_)
            | Val::ExternRef(_)
            | Val::AnyRef(_)
            | Val::ExnRef(_)
            | Val::ContRef(_) => Err(Error::msg(
                "ERR wasm reference return values are not supported",
            )),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::I32(_) => "i32",
            Self::I64(_) => "i64",
            Self::F32(_) => "f32",
            Self::F64(_) => "f64",
        }
    }

    pub fn value_string(&self) -> String {
        match self {
            Self::I32(value) => value.to_string(),
            Self::I64(value) => value.to_string(),
            Self::F32(value) => value.to_string(),
            Self::F64(value) => value.to_string(),
        }
    }
}

fn host_linker(engine: &Engine) -> Result<Linker<WasmHostContext>> {
    let mut linker = Linker::new(engine);
    linker.func_wrap_async(
        "onedis",
        "redis_get",
        |caller, (key_ptr, key_len, out_ptr, out_cap): (i32, i32, i32, i32)| {
            Box::new(
                async move { host_redis_get(caller, key_ptr, key_len, out_ptr, out_cap).await },
            )
        },
    )?;
    linker.func_wrap_async(
        "onedis",
        "redis_set",
        |caller, (key_ptr, key_len, value_ptr, value_len): (i32, i32, i32, i32)| {
            Box::new(
                async move { host_redis_set(caller, key_ptr, key_len, value_ptr, value_len).await },
            )
        },
    )?;
    linker.func_wrap_async(
        "onedis",
        "redis_del",
        |caller, (key_ptr, key_len): (i32, i32)| {
            Box::new(async move { host_redis_del(caller, key_ptr, key_len).await })
        },
    )?;
    linker.func_wrap_async(
        "onedis",
        "redis_hget",
        |caller,
         (key_ptr, key_len, field_ptr, field_len, out_ptr, out_cap): (
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
        )| {
            Box::new(async move {
                host_redis_hget(
                    caller, key_ptr, key_len, field_ptr, field_len, out_ptr, out_cap,
                )
                .await
            })
        },
    )?;
    linker.func_wrap_async(
        "onedis",
        "redis_hset",
        |caller,
         (key_ptr, key_len, field_ptr, field_len, value_ptr, value_len): (
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
        )| {
            Box::new(async move {
                host_redis_hset(
                    caller, key_ptr, key_len, field_ptr, field_len, value_ptr, value_len,
                )
                .await
            })
        },
    )?;
    linker.func_wrap_async(
        "onedis",
        "redis_call",
        |caller,
         (cmd_ptr, cmd_len, args_ptr, args_len, out_ptr, out_cap): (
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
        )| {
            Box::new(async move {
                host_redis_call(
                    caller, cmd_ptr, cmd_len, args_ptr, args_len, out_ptr, out_cap,
                )
                .await
            })
        },
    )?;
    Ok(linker)
}

async fn host_redis_get(
    mut caller: Caller<'_, WasmHostContext>,
    key_ptr: i32,
    key_len: i32,
    out_ptr: i32,
    out_cap: i32,
) -> i32 {
    let Some(key) = read_guest_string(&mut caller, key_ptr, key_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let db = caller.data().db.clone();
    match db.get_string_bytes_async(&key).await {
        Ok(Some(value)) => write_guest_bytes(&mut caller, out_ptr, out_cap, &value),
        Ok(None) => WASM_OK_NIL,
        Err(_) => host_error(&mut caller, WASM_ERR_DB),
    }
}

async fn host_redis_set(
    mut caller: Caller<'_, WasmHostContext>,
    key_ptr: i32,
    key_len: i32,
    value_ptr: i32,
    value_len: i32,
) -> i32 {
    if caller.data().read_only {
        return host_error(&mut caller, WASM_ERR_READONLY);
    }
    let Some(key) = read_guest_string(&mut caller, key_ptr, key_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let Some(value) = read_guest_bytes(&mut caller, value_ptr, value_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let db = caller.data().db.clone();
    match db
        .set_string_bytes_async(
            key,
            value,
            SetExpiration::Clear,
            SetCondition::Always,
            false,
        )
        .await
    {
        Ok(SetOutcome::Set { .. }) => 1,
        Ok(SetOutcome::NotSet) => 0,
        Err(_) => host_error(&mut caller, WASM_ERR_DB),
    }
}

async fn host_redis_del(
    mut caller: Caller<'_, WasmHostContext>,
    key_ptr: i32,
    key_len: i32,
) -> i32 {
    if caller.data().read_only {
        return host_error(&mut caller, WASM_ERR_READONLY);
    }
    let Some(key) = read_guest_string(&mut caller, key_ptr, key_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let db = caller.data().db.clone();
    i32::from(db.delete_key_async(&key).await)
}

async fn host_redis_hget(
    mut caller: Caller<'_, WasmHostContext>,
    key_ptr: i32,
    key_len: i32,
    field_ptr: i32,
    field_len: i32,
    out_ptr: i32,
    out_cap: i32,
) -> i32 {
    let Some(key) = read_guest_string(&mut caller, key_ptr, key_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let Some(field) = read_guest_string(&mut caller, field_ptr, field_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let db = caller.data().db.clone();
    match db.hash_get_async(&key, &field).await {
        Ok(Some(value)) => write_guest_bytes(&mut caller, out_ptr, out_cap, value.as_bytes()),
        Ok(None) => WASM_OK_NIL,
        Err(_) => host_error(&mut caller, WASM_ERR_DB),
    }
}

async fn host_redis_hset(
    mut caller: Caller<'_, WasmHostContext>,
    key_ptr: i32,
    key_len: i32,
    field_ptr: i32,
    field_len: i32,
    value_ptr: i32,
    value_len: i32,
) -> i32 {
    if caller.data().read_only {
        return host_error(&mut caller, WASM_ERR_READONLY);
    }
    let Some(key) = read_guest_string(&mut caller, key_ptr, key_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let Some(field) = read_guest_string(&mut caller, field_ptr, field_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let Some(value) = read_guest_string(&mut caller, value_ptr, value_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let db = caller.data().db.clone();
    match db.hash_set_async(&key, &field, &value).await {
        Ok(added) => added as i32,
        Err(_) => host_error(&mut caller, WASM_ERR_DB),
    }
}

async fn host_redis_call(
    mut caller: Caller<'_, WasmHostContext>,
    cmd_ptr: i32,
    cmd_len: i32,
    args_ptr: i32,
    args_len: i32,
    out_ptr: i32,
    out_cap: i32,
) -> i32 {
    let Some(command) = read_guest_string(&mut caller, cmd_ptr, cmd_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let Some(args_blob) = read_guest_bytes(&mut caller, args_ptr, args_len) else {
        return host_error(&mut caller, WASM_ERR_MEMORY);
    };
    let args = split_nul_args(&args_blob);
    match command.to_ascii_uppercase().as_str() {
        "GET" if args.len() == 1 => {
            let db = caller.data().db.clone();
            match db.get_string_bytes_async(&args[0]).await {
                Ok(Some(value)) => write_guest_bytes(&mut caller, out_ptr, out_cap, &value),
                Ok(None) => WASM_OK_NIL,
                Err(_) => host_error(&mut caller, WASM_ERR_DB),
            }
        }
        "SET" if args.len() == 2 => {
            if caller.data().read_only {
                return host_error(&mut caller, WASM_ERR_READONLY);
            }
            let db = caller.data().db.clone();
            match db
                .set_string_bytes_async(
                    args[0].clone(),
                    args[1].as_bytes().to_vec(),
                    SetExpiration::Clear,
                    SetCondition::Always,
                    false,
                )
                .await
            {
                Ok(SetOutcome::Set { .. }) => 1,
                Ok(SetOutcome::NotSet) => 0,
                Err(_) => host_error(&mut caller, WASM_ERR_DB),
            }
        }
        "DEL" if args.len() == 1 => {
            if caller.data().read_only {
                return host_error(&mut caller, WASM_ERR_READONLY);
            }
            let db = caller.data().db.clone();
            i32::from(db.delete_key_async(&args[0]).await)
        }
        "HGET" if args.len() == 2 => {
            let db = caller.data().db.clone();
            match db.hash_get_async(&args[0], &args[1]).await {
                Ok(Some(value)) => {
                    write_guest_bytes(&mut caller, out_ptr, out_cap, value.as_bytes())
                }
                Ok(None) => WASM_OK_NIL,
                Err(_) => host_error(&mut caller, WASM_ERR_DB),
            }
        }
        "HSET" if args.len() == 3 => {
            if caller.data().read_only {
                return host_error(&mut caller, WASM_ERR_READONLY);
            }
            let db = caller.data().db.clone();
            match db.hash_set_async(&args[0], &args[1], &args[2]).await {
                Ok(added) => added as i32,
                Err(_) => host_error(&mut caller, WASM_ERR_DB),
            }
        }
        _ => host_error(&mut caller, WASM_ERR_UNSUPPORTED),
    }
}

fn host_error(caller: &mut Caller<'_, WasmHostContext>, code: i32) -> i32 {
    caller.data_mut().host_error = true;
    code
}

fn read_guest_string(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    len: i32,
) -> Option<String> {
    String::from_utf8(read_guest_bytes(caller, ptr, len)?).ok()
}

fn read_guest_bytes(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let memory = caller.get_export("memory")?.into_memory()?;
    let mut bytes = vec![0; len as usize];
    memory.read(&*caller, ptr as usize, &mut bytes).ok()?;
    Some(bytes)
}

fn write_guest_bytes(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    cap: i32,
    bytes: &[u8],
) -> i32 {
    if ptr < 0 || cap < 0 || bytes.len() > cap as usize {
        return host_error(caller, WASM_ERR_MEMORY);
    }
    let Some(memory) = caller.get_export("memory").and_then(Extern::into_memory) else {
        return host_error(caller, WASM_ERR_MEMORY);
    };
    match memory.write(&mut *caller, ptr as usize, bytes) {
        Ok(()) => bytes.len() as i32,
        Err(_) => host_error(caller, WASM_ERR_MEMORY),
    }
}

fn split_nul_args(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .filter_map(|part| String::from_utf8(part.to_vec()).ok())
        .collect()
}

fn validate_imports(module: &Module) -> Result<()> {
    for import in module.imports() {
        let module_name = import.module();
        let name = import.name();
        if module_name != "onedis" || !is_allowed_host_import(name) {
            return Err(Error::msg(format!(
                "ERR unsupported wasm import '{}.{}'",
                module_name, name
            )));
        }
    }
    Ok(())
}

fn is_allowed_host_import(name: &str) -> bool {
    matches!(
        name,
        "redis_get" | "redis_set" | "redis_del" | "redis_hget" | "redis_hset" | "redis_call"
    )
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(Error::msg("ERR invalid wasm module name"));
    }
    Ok(())
}

fn prepare_call_inputs(
    store: &mut Store<WasmHostContext>,
    instance: &Instance,
    params: &[ValType],
    args: &[String],
) -> Result<Vec<Val>> {
    if params.len() == args.len() {
        return args
            .iter()
            .zip(params)
            .map(|(arg, ty)| parse_wasm_arg(arg, ty.clone()))
            .collect::<Result<Vec<_>>>();
    }

    if params.len() == args.len().saturating_mul(2)
        && params.iter().all(|ty| matches!(ty, ValType::I32))
    {
        return prepare_string_pair_inputs(store, instance, args);
    }

    Err(Error::msg(format!(
        "ERR wasm function expects {} numeric arguments or {} string arguments, got {}",
        params.len(),
        params.len() / 2,
        args.len()
    )))
}

fn prepare_string_pair_inputs(
    store: &mut Store<WasmHostContext>,
    instance: &Instance,
    args: &[String],
) -> Result<Vec<Val>> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| Error::msg("ERR wasm module must export memory for string arguments"))?;
    let mut offset = WASM_ARG_OFFSET;
    let mut total = 0usize;
    let mut inputs = Vec::with_capacity(args.len() * 2);
    for arg in args {
        let bytes = arg.as_bytes();
        total = total.saturating_add(bytes.len());
        if total > WASM_ARG_MAX_TOTAL_BYTES {
            return Err(Error::msg("ERR wasm call arguments are too large"));
        }
        memory
            .write(&mut *store, offset, bytes)
            .map_err(|_| Error::msg("ERR wasm call argument does not fit in memory"))?;
        inputs.push(Val::I32(offset as i32));
        inputs.push(Val::I32(bytes.len() as i32));
        offset = offset.saturating_add(bytes.len()).saturating_add(1);
    }
    Ok(inputs)
}

fn parse_wasm_arg(value: &str, ty: ValType) -> Result<Val> {
    match ty {
        ValType::I32 => value
            .parse::<i32>()
            .map(Val::I32)
            .map_err(|_| Error::msg("ERR invalid i32 wasm argument")),
        ValType::I64 => value
            .parse::<i64>()
            .map(Val::I64)
            .map_err(|_| Error::msg("ERR invalid i64 wasm argument")),
        ValType::F32 => value
            .parse::<f32>()
            .map(|value| Val::F32(value.to_bits()))
            .map_err(|_| Error::msg("ERR invalid f32 wasm argument")),
        ValType::F64 => value
            .parse::<f64>()
            .map(|value| Val::F64(value.to_bits()))
            .map_err(|_| Error::msg("ERR invalid f64 wasm argument")),
        ValType::V128 | ValType::Ref(_) => Err(Error::msg(
            "ERR wasm non-scalar arguments are not supported",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    };
    use std::sync::Arc;

    fn add_i64_module() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7e,
            0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64,
            0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x7c, 0x0b,
        ]
    }

    fn wasm_leb(mut value: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }

    fn wasm_name(name: &str) -> Vec<u8> {
        let mut bytes = wasm_leb(name.len() as u32);
        bytes.extend_from_slice(name.as_bytes());
        bytes
    }

    fn wasm_vec(items: Vec<Vec<u8>>) -> Vec<u8> {
        let mut bytes = wasm_leb(items.len() as u32);
        for item in items {
            bytes.extend(item);
        }
        bytes
    }

    fn wasm_section(id: u8, payload: Vec<u8>) -> Vec<u8> {
        let mut bytes = vec![id];
        bytes.extend(wasm_leb(payload.len() as u32));
        bytes.extend(payload);
        bytes
    }

    fn wasm_func_type(params: &[u8], results: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x60];
        bytes.extend(wasm_leb(params.len() as u32));
        bytes.extend_from_slice(params);
        bytes.extend(wasm_leb(results.len() as u32));
        bytes.extend_from_slice(results);
        bytes
    }

    fn wasm_import_func(name: &str, type_idx: u32) -> Vec<u8> {
        let mut bytes = wasm_name("onedis");
        bytes.extend(wasm_name(name));
        bytes.push(0x00);
        bytes.extend(wasm_leb(type_idx));
        bytes
    }

    fn wasm_export_func(name: &str, func_idx: u32) -> Vec<u8> {
        let mut bytes = wasm_name(name);
        bytes.push(0x00);
        bytes.extend(wasm_leb(func_idx));
        bytes
    }

    fn wasm_i32_const(value: i32) -> Vec<u8> {
        let mut bytes = vec![0x41];
        let mut remaining = value;
        loop {
            let byte = (remaining as u8) & 0x7f;
            remaining >>= 7;
            let done =
                (remaining == 0 && (byte & 0x40) == 0) || (remaining == -1 && (byte & 0x40) != 0);
            bytes.push(if done { byte } else { byte | 0x80 });
            if done {
                break;
            }
        }
        bytes
    }

    fn wasm_call(func_idx: u32) -> Vec<u8> {
        let mut bytes = vec![0x10];
        bytes.extend(wasm_leb(func_idx));
        bytes
    }

    fn wasm_body(instructions: Vec<u8>) -> Vec<u8> {
        let mut body = vec![0x00];
        body.extend(instructions);
        body.push(0x0b);
        let mut bytes = wasm_leb(body.len() as u32);
        bytes.extend(body);
        bytes
    }

    fn wasm_data(offset: u32, data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend(wasm_i32_const(offset as i32));
        bytes.push(0x0b);
        bytes.extend(wasm_leb(data.len() as u32));
        bytes.extend_from_slice(data);
        bytes
    }

    fn host_import_module() -> Vec<u8> {
        const I32: u8 = 0x7f;
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        module.extend(wasm_section(
            1,
            wasm_vec(vec![
                wasm_func_type(&[I32, I32, I32, I32], &[I32]),
                wasm_func_type(&[I32, I32], &[I32]),
                wasm_func_type(&[I32, I32, I32, I32, I32, I32], &[I32]),
                wasm_func_type(&[], &[I32]),
                wasm_func_type(&[I32, I32, I32, I32], &[I32]),
            ]),
        ));
        module.extend(wasm_section(
            2,
            wasm_vec(vec![
                wasm_import_func("redis_get", 0),
                wasm_import_func("redis_set", 0),
                wasm_import_func("redis_del", 1),
                wasm_import_func("redis_hget", 2),
                wasm_import_func("redis_hset", 2),
                wasm_import_func("redis_call", 2),
            ]),
        ));
        module.extend(wasm_section(
            3,
            wasm_vec(vec![
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(4),
            ]),
        ));
        module.extend(wasm_section(5, wasm_vec(vec![vec![0x00, 0x02]])));
        let mut memory_export = wasm_name("memory");
        memory_export.push(0x02);
        memory_export.extend(wasm_leb(0));
        module.extend(wasm_section(
            7,
            wasm_vec(vec![
                memory_export,
                wasm_export_func("set_key", 6),
                wasm_export_func("get_key", 7),
                wasm_export_func("get_key_tiny_cap", 8),
                wasm_export_func("del_key", 9),
                wasm_export_func("hset_field", 10),
                wasm_export_func("hget_field", 11),
                wasm_export_func("call_set", 12),
                wasm_export_func("call_get", 13),
                wasm_export_func("call_unknown", 14),
                wasm_export_func("scan_accept", 15),
            ]),
        ));

        let call4 = |func_idx: u32, a: i32, b: i32, c: i32, d: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_i32_const(c));
            body.extend(wasm_i32_const(d));
            body.extend(wasm_call(func_idx));
            body
        };
        let call2 = |func_idx: u32, a: i32, b: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_call(func_idx));
            body
        };
        let call6 = |func_idx: u32, a: i32, b: i32, c: i32, d: i32, e: i32, f: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_i32_const(c));
            body.extend(wasm_i32_const(d));
            body.extend(wasm_i32_const(e));
            body.extend(wasm_i32_const(f));
            body.extend(wasm_call(func_idx));
            body
        };
        module.extend(wasm_section(
            10,
            wasm_vec(vec![
                wasm_body(call4(1, 0, 4, 16, 6)),
                wasm_body(call4(0, 0, 4, 256, 64)),
                wasm_body(call4(0, 0, 4, 256, 1)),
                wasm_body(call2(2, 0, 4)),
                wasm_body(call6(4, 0, 4, 32, 5, 48, 6)),
                wasm_body(call6(3, 0, 4, 32, 5, 256, 64)),
                wasm_body(call6(5, 80, 3, 128, 20, 256, 64)),
                wasm_body(call6(5, 84, 3, 160, 9, 256, 64)),
                wasm_body(call6(5, 88, 4, 180, 0, 256, 64)),
                wasm_body(wasm_i32_const(1)),
            ]),
        ));
        module.extend(wasm_section(
            11,
            wasm_vec(vec![
                wasm_data(0, b"wkey"),
                wasm_data(16, b"wvalue"),
                wasm_data(32, b"field"),
                wasm_data(48, b"hvalue"),
                wasm_data(80, b"SET"),
                wasm_data(84, b"GET"),
                wasm_data(88, b"NOPE"),
                wasm_data(128, b"call-key\0call-value\0"),
                wasm_data(160, b"call-key\0"),
            ]),
        ));
        module
    }

    fn test_db() -> Arc<Db> {
        let unique = format!(
            "onedis-wasm-test-{}",
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
        Arc::new(Db::new(0, store, version_counter, ttl_manager))
    }

    #[tokio::test]
    async fn wasm_registry_loads_and_calls_i64_function() {
        let registry = WasmRegistry::new();
        registry.load("math", &add_i64_module()).unwrap();
        let result = registry
            .call(
                test_db(),
                "math",
                "add",
                &["40".to_string(), "2".to_string()],
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, vec![WasmValue::I64(42)]);
    }

    #[tokio::test]
    async fn wasm_host_imports_drive_string_hash_call_scan_and_readonly_edges() {
        let registry = WasmRegistry::new();
        registry
            .load("host", &host_import_module())
            .unwrap_or_else(|err| panic!("{err:#}"));
        let db = test_db();

        assert_eq!(
            registry
                .call(db.clone(), "host", "set_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            db.get_string_bytes_async("wkey").await.unwrap(),
            Some(b"wvalue".to_vec())
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "get_key", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(6)]
        );
        assert!(
            registry
                .call(db.clone(), "host", "get_key_tiny_cap", &[], true)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(db.clone(), "host", "set_key", &[], true)
                .await
                .is_err()
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "del_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(db.get_string_bytes_async("wkey").await.unwrap(), None);

        assert_eq!(
            registry
                .call(db.clone(), "host", "hset_field", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "hget_field", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(6)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "call_set", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "call_get", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(10)]
        );
        assert!(
            registry
                .call(db.clone(), "host", "call_unknown", &[], true)
                .await
                .is_err()
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "del_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert!(db.hash_get_async("wkey", "field").await.unwrap().is_none());

        db.insert_string_ref("scan:1", "one");
        db.insert_string_ref("scan:2", "two");
        let mut matched = registry
            .scan(db.clone(), "host", "scan_accept", "scan:", 10)
            .await
            .unwrap();
        matched.sort();
        assert_eq!(matched, vec!["scan:1".to_string(), "scan:2".to_string()]);
    }

    #[test]
    fn wasm_registry_validates_names_lists_deletes_and_rejects_invalid_modules() {
        let registry = WasmRegistry::new();

        for name in ["", "bad name", "bad/slash", "bad:colon"] {
            assert!(registry.load(name, &add_i64_module()).is_err(), "{name}");
            assert!(validate_name(name).is_err(), "{name}");
        }
        for name in ["math", "math.v1", "math-v1", "math_v1"] {
            validate_name(name).unwrap();
        }

        registry.load("z", &add_i64_module()).unwrap();
        registry.load("a", &add_i64_module()).unwrap();
        assert_eq!(registry.list(), vec!["a".to_string(), "z".to_string()]);
        assert!(registry.delete("a"));
        assert!(!registry.delete("a"));
        assert_eq!(registry.list(), vec!["z".to_string()]);

        assert!(registry.load("bad", b"not wasm").is_err());
        assert!(
            registry
                .load("huge", &vec![0u8; 16 * 1024 * 1024 + 1])
                .is_err()
        );
    }

    #[test]
    fn wasm_private_helpers_cover_argument_and_value_conversion_edges() {
        assert!(is_allowed_host_import("redis_get"));
        assert!(is_allowed_host_import("redis_call"));
        assert!(!is_allowed_host_import("redis_eval"));

        assert_eq!(
            split_nul_args(b"GET\0key\0\0bad-\xff"),
            vec!["GET".to_string(), "key".to_string()]
        );

        assert!(matches!(
            parse_wasm_arg("7", ValType::I32).unwrap(),
            Val::I32(7)
        ));
        assert!(matches!(
            parse_wasm_arg("8", ValType::I64).unwrap(),
            Val::I64(8)
        ));
        assert!(matches!(
            parse_wasm_arg("1.5", ValType::F32).unwrap(),
            Val::F32(_)
        ));
        assert!(matches!(
            parse_wasm_arg("2.5", ValType::F64).unwrap(),
            Val::F64(_)
        ));
        assert!(parse_wasm_arg("bad", ValType::I32).is_err());
        assert!(parse_wasm_arg("bad", ValType::I64).is_err());
        assert!(parse_wasm_arg("bad", ValType::F32).is_err());
        assert!(parse_wasm_arg("bad", ValType::F64).is_err());

        let values = [
            WasmValue::I32(1),
            WasmValue::I64(2),
            WasmValue::F32(3.5),
            WasmValue::F64(4.5),
        ];
        assert_eq!(values[0].type_name(), "i32");
        assert_eq!(values[1].type_name(), "i64");
        assert_eq!(values[2].type_name(), "f32");
        assert_eq!(values[3].type_name(), "f64");
        assert_eq!(values[0].value_string(), "1");
        assert_eq!(values[1].value_string(), "2");
        assert_eq!(values[2].value_string(), "3.5");
        assert_eq!(values[3].value_string(), "4.5");

        assert_eq!(WasmValue::from_val(Val::I32(9)).unwrap(), WasmValue::I32(9));
        assert_eq!(
            WasmValue::from_val(Val::I64(10)).unwrap(),
            WasmValue::I64(10)
        );
        assert!(matches!(
            WasmValue::from_val(Val::F32(1.25f32.to_bits())).unwrap(),
            WasmValue::F32(value) if value == 1.25
        ));
        assert!(matches!(
            WasmValue::from_val(Val::F64(2.25f64.to_bits())).unwrap(),
            WasmValue::F64(value) if value == 2.25
        ));
    }

    #[tokio::test]
    async fn wasm_call_and_scan_report_missing_function_argument_and_signature_errors() {
        let registry = WasmRegistry::new();
        registry.load("math", &add_i64_module()).unwrap();

        assert!(
            registry
                .call(test_db(), "missing", "add", &[], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(test_db(), "math", "missing", &[], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(test_db(), "math", "add", &["1".to_string()], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(
                    test_db(),
                    "math",
                    "add",
                    &["bad".to_string(), "2".to_string()],
                    false,
                )
                .await
                .is_err()
        );
        assert!(
            registry
                .scan(test_db(), "missing", "filter", "", 10)
                .await
                .is_err()
        );
        assert!(
            registry
                .scan(test_db(), "math", "add", "", 10)
                .await
                .is_err()
        );
    }

    #[test]
    fn wasm_limits_and_import_validation_cover_resource_edges() {
        let mut limits = WasmLimits::new(128);
        assert!(limits.memory_growing(0, 128, None).unwrap());
        assert!(!limits.memory_growing(0, 129, None).unwrap());
        assert!(limits.table_growing(0, 1024, None).unwrap());
        assert!(!limits.table_growing(0, 1025, None).unwrap());
        assert_eq!(limits.instances(), 4);
        assert_eq!(limits.tables(), 4);
        assert_eq!(limits.memories(), 4);

        let registry = WasmRegistry::new();
        let allowed_import = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
            0x02, 0x14, 0x01, 0x06, b'o', b'n', b'e', b'd', b'i', b's', 0x09, b'r', b'e', b'd',
            b'i', b's', b'_', b'g', b'e', b't', 0x00, 0x00,
        ];
        registry.load("allowed_import", &allowed_import).unwrap();

        let bad_import = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
            0x02, 0x09, 0x01, 0x03, b'b', b'a', b'd', 0x01, b'x', 0x00, 0x00,
        ];
        assert!(registry.load("bad_import", &bad_import).is_err());
    }
}
