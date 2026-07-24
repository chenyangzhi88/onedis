use super::*;

pub(super) fn host_error(caller: &mut Caller<'_, WasmHostContext>, code: i32) -> i32 {
    caller.data_mut().host_error = true;
    code
}

pub(super) fn read_guest_string(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    len: i32,
) -> Option<String> {
    String::from_utf8(read_guest_bytes(caller, ptr, len)?).ok()
}

pub(super) fn read_guest_bytes(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let memory = caller.get_export("memory")?.into_memory()?;
    let ptr = ptr as usize;
    let len = len as usize;
    if len > WASM_HOST_MAX_IO_BYTES
        || ptr
            .checked_add(len)
            .is_none_or(|end| end > memory.data_size(&*caller))
    {
        return None;
    }
    let mut bytes = vec![0; len];
    memory.read(&*caller, ptr, &mut bytes).ok()?;
    Some(bytes)
}

pub(super) fn write_guest_bytes(
    caller: &mut Caller<'_, WasmHostContext>,
    ptr: i32,
    cap: i32,
    bytes: &[u8],
) -> i32 {
    if ptr < 0 || cap < 0 || bytes.len() > WASM_HOST_MAX_IO_BYTES || bytes.len() > cap as usize {
        return host_error(caller, WASM_ERR_MEMORY);
    }
    let Some(memory) = caller.get_export("memory").and_then(Extern::into_memory) else {
        return host_error(caller, WASM_ERR_MEMORY);
    };
    let ptr = ptr as usize;
    if ptr
        .checked_add(bytes.len())
        .is_none_or(|end| end > memory.data_size(&*caller))
    {
        return host_error(caller, WASM_ERR_MEMORY);
    }
    match memory.write(&mut *caller, ptr, bytes) {
        Ok(()) => bytes.len() as i32,
        Err(_) => host_error(caller, WASM_ERR_MEMORY),
    }
}

pub(super) fn split_nul_args(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut arg_count = bytes.split(|byte| *byte == 0).count();
    if bytes.last().is_none_or(|byte| *byte == 0) {
        arg_count = arg_count.saturating_sub(1);
    }
    if arg_count > WASM_HOST_MAX_CALL_ARGS {
        return None;
    }
    let mut args = bytes
        .split(|byte| *byte == 0)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    if args.last().is_some_and(Vec::is_empty) {
        args.pop();
    }
    Some(args)
}
