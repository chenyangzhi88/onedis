use super::*;

pub(super) fn host_linker(engine: &Engine) -> Result<Linker<WasmHostContext>> {
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
