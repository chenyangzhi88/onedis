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
        commands: Vec<BorrowedHsetCommand<'a>>,
    ) -> Vec<u8> {
        const MAX_HSET_COMMANDS_PER_WRITE: usize = 256;

        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 4);
        let mut index = 0;
        while index < commands.len() {
            let (key_bytes, command_fields) = &commands[index];
            let Ok(key) = std::str::from_utf8(key_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                index += 1;
                continue;
            };
            if command_fields
                .iter()
                .any(|(field, _)| std::str::from_utf8(field).is_err())
            {
                append_error(&mut out, "ERR invalid UTF-8 hash field");
                index += 1;
                continue;
            }
            let mut fields = Vec::new();
            let mut field_count_by_command = Vec::new();
            while index < commands.len() && commands[index].0 == *key_bytes {
                let candidate_fields = &commands[index].1;
                if !fields.is_empty()
                    && fields.len() + candidate_fields.len() > MAX_HSET_COMMANDS_PER_WRITE
                {
                    break;
                }
                let Some(decoded_fields) = candidate_fields
                    .iter()
                    .map(|(field, value)| {
                        std::str::from_utf8(field)
                            .ok()
                            .map(|field| (field, *value))
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    break;
                };
                field_count_by_command.push(decoded_fields.len());
                fields.extend(decoded_fields);
                index += 1;
            }

            match db.hash_set_ordered_bytes_async(key, &fields).await {
                Ok(added) => {
                    let mut added = added.into_iter();
                    for field_count in field_count_by_command {
                        let count = added.by_ref().take(field_count).filter(|added| *added).count();
                        append_integer(&mut out, count as i64);
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for _ in field_count_by_command {
                        append_error(&mut out, &error);
                    }
                }
            }
        }
        out
    }

    async fn handle_borrowed_hdel_commands<'a>(
        &self,
        commands: Vec<BorrowedHdelCommand<'a>>,
    ) -> Vec<u8> {
        const MAX_HDEL_FIELDS_PER_WRITE: usize = 256;

        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 4);
        let mut index = 0;
        while index < commands.len() {
            let (key_bytes, command_fields) = &commands[index];
            let Ok(key) = std::str::from_utf8(key_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                index += 1;
                continue;
            };
            if command_fields
                .iter()
                .any(|field| std::str::from_utf8(field).is_err())
            {
                append_error(&mut out, "ERR invalid UTF-8 hash field");
                index += 1;
                continue;
            }

            let mut fields = Vec::new();
            let mut field_count_by_command = Vec::new();
            while index < commands.len() && commands[index].0 == *key_bytes {
                let candidate_fields = &commands[index].1;
                if !fields.is_empty()
                    && fields.len() + candidate_fields.len() > MAX_HDEL_FIELDS_PER_WRITE
                {
                    break;
                }
                let Some(decoded_fields) = candidate_fields
                    .iter()
                    .map(|field| std::str::from_utf8(field).ok())
                    .collect::<Option<Vec<_>>>()
                else {
                    break;
                };
                field_count_by_command.push(decoded_fields.len());
                fields.extend(decoded_fields);
                index += 1;
            }

            match db.hash_delete_ordered_refs_async(key, &fields).await {
                Ok(deleted) => {
                    let mut deleted = deleted.into_iter();
                    for field_count in field_count_by_command {
                        let count = deleted
                            .by_ref()
                            .take(field_count)
                            .filter(|deleted| *deleted)
                            .count();
                        append_integer(&mut out, count as i64);
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for _ in field_count_by_command {
                        append_error(&mut out, &error);
                    }
                }
            }
        }
        out
    }

    async fn handle_borrowed_hgetdel_commands<'a>(
        &self,
        commands: Vec<BorrowedHdelCommand<'a>>,
    ) -> Vec<u8> {
        const MAX_HGETDEL_FIELDS_PER_WRITE: usize = 256;

        let db = self.session.get_db().clone();
        let mut out = Vec::new();
        let mut index = 0;
        while index < commands.len() {
            let (key_bytes, command_fields) = &commands[index];
            let Ok(key) = std::str::from_utf8(key_bytes) else {
                append_error(&mut out, "ERR invalid UTF-8 key");
                index += 1;
                continue;
            };
            if command_fields
                .iter()
                .any(|field| std::str::from_utf8(field).is_err())
            {
                append_error(&mut out, "ERR invalid UTF-8 hash field");
                index += 1;
                continue;
            }

            let mut fields = Vec::new();
            let mut field_count_by_command = Vec::new();
            while index < commands.len() && commands[index].0 == *key_bytes {
                let candidate_fields = &commands[index].1;
                if !fields.is_empty()
                    && fields.len() + candidate_fields.len() > MAX_HGETDEL_FIELDS_PER_WRITE
                {
                    break;
                }
                let Some(decoded_fields) = candidate_fields
                    .iter()
                    .map(|field| std::str::from_utf8(field).ok().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
                else {
                    break;
                };
                field_count_by_command.push(decoded_fields.len());
                fields.extend(decoded_fields);
                index += 1;
            }

            match db.hash_get_del_bytes_async(key, &fields).await {
                Ok(values) => {
                    let mut values = values.into_iter();
                    for field_count in field_count_by_command {
                        append_array_len(&mut out, field_count);
                        for value in values.by_ref().take(field_count) {
                            if let Some(value) = value {
                                append_bulk_string(&mut out, &value);
                            } else {
                                append_null(&mut out);
                            }
                        }
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for _ in field_count_by_command {
                        append_error(&mut out, &error);
                    }
                }
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
