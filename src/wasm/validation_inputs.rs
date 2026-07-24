use super::*;

pub(super) fn validate_imports(module: &Module) -> Result<()> {
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

pub(super) fn is_allowed_host_import(name: &str) -> bool {
    matches!(
        name,
        "redis_get" | "redis_set" | "redis_del" | "redis_hget" | "redis_hset" | "redis_call"
    )
}

pub(super) fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(Error::msg("ERR invalid wasm module name"));
    }
    Ok(())
}

pub(super) fn prepare_call_inputs(
    store: &mut Store<WasmHostContext>,
    instance: &Instance,
    params: &[ValType],
    args: &[Vec<u8>],
) -> Result<Vec<Val>> {
    if params.len() == args.len() {
        return args
            .iter()
            .zip(params)
            .map(|(arg, ty)| parse_wasm_arg_bytes(arg, ty.clone()))
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
    args: &[Vec<u8>],
) -> Result<Vec<Val>> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| Error::msg("ERR wasm module must export memory for string arguments"))?;
    let mut offset = WASM_ARG_OFFSET;
    let mut total = 0usize;
    let mut inputs = Vec::with_capacity(args.len() * 2);
    for arg in args {
        total = total
            .checked_add(arg.len())
            .ok_or_else(|| Error::msg("ERR wasm call arguments are too large"))?;
        if total > WASM_ARG_MAX_TOTAL_BYTES {
            return Err(Error::msg("ERR wasm call arguments are too large"));
        }
        memory
            .write(&mut *store, offset, arg)
            .map_err(|_| Error::msg("ERR wasm call argument does not fit in memory"))?;
        inputs.push(Val::I32(offset as i32));
        inputs.push(Val::I32(arg.len() as i32));
        offset = offset
            .checked_add(arg.len())
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(|| Error::msg("ERR wasm call arguments are too large"))?;
    }
    Ok(inputs)
}

#[cfg(test)]
pub(super) fn parse_wasm_arg(value: &str, ty: ValType) -> Result<Val> {
    parse_wasm_arg_bytes(value.as_bytes(), ty)
}

fn parse_wasm_arg_bytes(value: &[u8], ty: ValType) -> Result<Val> {
    let value = std::str::from_utf8(value)
        .map_err(|_| Error::msg("ERR wasm numeric argument is not valid UTF-8"))?;
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
