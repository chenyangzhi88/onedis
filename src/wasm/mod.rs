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

mod guest_memory;
mod host_imports;
mod registry;
mod runtime_types;
mod validation_inputs;

use guest_memory::{
    host_error, read_guest_bytes, read_guest_string, split_nul_args, write_guest_bytes,
};
use host_imports::host_linker;
pub use registry::WasmRegistry;
pub use runtime_types::WasmValue;
use runtime_types::{WasmHostContext, WasmLimits};
#[cfg(test)]
use validation_inputs::{is_allowed_host_import, parse_wasm_arg};
use validation_inputs::{prepare_call_inputs, validate_imports, validate_name};

#[cfg(test)]
mod tests;
