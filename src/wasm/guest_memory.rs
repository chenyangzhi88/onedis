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
    let mut bytes = vec![0; len as usize];
    memory.read(&*caller, ptr as usize, &mut bytes).ok()?;
    Some(bytes)
}

pub(super) fn write_guest_bytes(
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

pub(super) fn split_nul_args(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .filter_map(|part| String::from_utf8(part.to_vec()).ok())
        .collect()
}
