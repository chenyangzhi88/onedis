impl Handler {
    async fn handle_borrowed_read_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 16);
        for args in commands {
            let command = args.first().copied().unwrap_or_default();
            if command.eq_ignore_ascii_case(b"GET") {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'get' command");
                    continue;
                }
                match db.get_string_entry_raw_bytes_async(args[1]).await {
                    Ok(Some(raw)) => {
                        let value = decode_string_bytes_slice(&raw).unwrap_or_default();
                        append_bulk_string(&mut out, value);
                    }
                    Ok(None) => append_null(&mut out),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"MGET") {
                if args.len() < 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'mget' command");
                    continue;
                }
                append_array_len(&mut out, args.len().saturating_sub(1));
                for key_bytes in &args[1..] {
                    match db.get_string_entry_raw_bytes_async(key_bytes).await {
                        Ok(Some(raw)) => {
                            let value = decode_string_bytes_slice(&raw).unwrap_or_default();
                            append_bulk_string(&mut out, value);
                        }
                        Ok(None) | Err(_) => append_null(&mut out),
                    }
                }
            } else if command.eq_ignore_ascii_case(b"EXISTS") {
                if args.len() < 2 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'exists' command",
                    );
                    continue;
                }
                let mut count = 0i64;
                for key_bytes in &args[1..] {
                    let Ok(key) = std::str::from_utf8(key_bytes) else {
                        continue;
                    };
                    if db.exists_readonly(key) {
                        count += 1;
                    }
                }
                append_integer(&mut out, count);
            } else if command.eq_ignore_ascii_case(b"TTL") || command.eq_ignore_ascii_case(b"PTTL")
            {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for ttl command");
                    continue;
                }
                let Ok(key) = std::str::from_utf8(args[1]) else {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                };
                let millis = db.ttl_millis_readonly(key);
                let value = if command.eq_ignore_ascii_case(b"TTL") && millis >= 0 {
                    millis / 1000
                } else {
                    millis
                };
                append_integer(&mut out, value);
            } else if command.eq_ignore_ascii_case(b"STRLEN") {
                if args.len() != 2 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'strlen' command",
                    );
                    continue;
                }
                let Ok(key) = std::str::from_utf8(args[1]) else {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                };
                match db.get_string_bytes(key) {
                    Ok(Some(value)) => append_integer(&mut out, value.len() as i64),
                    Ok(None) => append_integer(&mut out, 0),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"TYPE") {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'type' command");
                    continue;
                }
                let Ok(key) = std::str::from_utf8(args[1]) else {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                };
                append_simple_string(&mut out, db.type_name_readonly(key));
            }
        }
        out
    }
}
