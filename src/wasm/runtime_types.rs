use super::*;

pub(super) struct WasmHostContext {
    pub(super) db: Arc<Db>,
    pub(super) read_only: bool,
    pub(super) host_error: bool,
    pub(super) limits: WasmLimits,
}

pub(super) struct WasmLimits {
    max_memory_bytes: usize,
}

impl WasmLimits {
    pub(super) fn new(max_memory_bytes: usize) -> Self {
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
    pub(super) fn from_val(value: Val) -> Result<Self> {
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
