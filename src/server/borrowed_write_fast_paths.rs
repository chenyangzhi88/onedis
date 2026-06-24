impl Handler {
    async fn handle_borrowed_set_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 5);
        let mut entries = Vec::with_capacity(commands.len());
        for args in commands {
            let Ok(key) = std::str::from_utf8(args[1]) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                continue;
            };
            entries.push((key, args[2]));
        }
        if !entries.is_empty() {
            db.insert_string_bytes_refs_async(&entries).await;
        }
        for _ in entries {
            out.extend_from_slice(b"+OK\r\n");
        }
        out
    }

    async fn handle_borrowed_set_byte_commands<'a>(
        &self,
        commands: Vec<(&'a [u8], &'a [u8])>,
    ) -> Vec<u8> {
        let db = self.session.get_db().clone();
        if !commands.is_empty() {
            db.insert_string_byte_keys_async(&commands).await;
        }
        let mut out = Vec::with_capacity(commands.len() * 5);
        for _ in commands {
            out.extend_from_slice(b"+OK\r\n");
        }
        out
    }

    async fn handle_borrowed_hset_commands<'a>(
        &self,
        commands: Vec<(&'a [u8], &'a [u8], &'a [u8])>,
    ) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 4);
        for (key_bytes, field_bytes, value_bytes) in commands {
            let Ok(key) = std::str::from_utf8(key_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                continue;
            };
            let Ok(field) = std::str::from_utf8(field_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 hash field");
                continue;
            };
            let Ok(value) = std::str::from_utf8(value_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 hash value");
                continue;
            };
            match db.hash_set_async(key, field, value).await {
                Ok(added) => append_integer(&mut out, i64::from(added)),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_list_push_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 16);
        let mut index = 0;
        while index < commands.len() {
            let args = &commands[index];
            let command = args.first().copied().unwrap_or_default();
            let is_left = command.eq_ignore_ascii_case(b"LPUSH");
            let Ok(key) = std::str::from_utf8(args[1]) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                index += 1;
                continue;
            };

            let mut value_count_by_command = Vec::new();
            let mut values = Vec::new();
            while index < commands.len() {
                let candidate = &commands[index];
                let candidate_command = candidate.first().copied().unwrap_or_default();
                let candidate_is_left = candidate_command.eq_ignore_ascii_case(b"LPUSH");
                if candidate_is_left != is_left || candidate[1] != args[1] {
                    break;
                }
                if std::str::from_utf8(candidate[1]).is_err() {
                    break;
                }
                value_count_by_command.push(candidate.len().saturating_sub(2));
                values.extend_from_slice(&candidate[2..]);
                index += 1;
            }

            let result = if is_left {
                db.list_push_left_bytes_async(key, &values, false).await
            } else {
                db.list_push_right_bytes_async(key, &values, false).await
            };
            match result {
                Ok(final_len) => {
                    let total_values = values.len();
                    let mut len = final_len.saturating_sub(total_values);
                    for value_count in value_count_by_command {
                        len = len.saturating_add(value_count);
                        append_integer(&mut out, len as i64);
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for _ in value_count_by_command {
                        append_error(&mut out, &error);
                    }
                }
            }
        }
        out
    }

    async fn handle_borrowed_lrange_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let ops = commands
            .into_iter()
            .map(|args| {
                let Ok(key) = std::str::from_utf8(args[1]) else {
                    return BorrowedLrangeOp::Error("ERR invalid UTF-8 key".to_string());
                };
                let Some(start) = parse_i64_ascii(args[2]) else {
                    return BorrowedLrangeOp::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    );
                };
                let Some(stop) = parse_i64_ascii(args[3]) else {
                    return BorrowedLrangeOp::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    );
                };
                BorrowedLrangeOp::Command {
                    key: key.to_string(),
                    start,
                    stop,
                }
            })
            .collect();
        match self
            .command_executor
            .execute(async move { encode_borrowed_lrange_ops(db, ops).await })
            .await
        {
            Ok(out) => out,
            Err(error) => Frame::Error(error.to_string()).as_bytes(),
        }
    }
}
