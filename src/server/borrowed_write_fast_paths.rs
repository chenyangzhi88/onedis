impl Handler {
    async fn handle_borrowed_zset_add_commands(
        &self,
        commands: &[BorrowedZsetAddCommand<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .zset_add_many_merged_async(commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(added) => append_integer(&mut out, added as i64),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_bitop_commands(
        &self,
        commands: &[BorrowedBitopCommand<'_>],
    ) -> Vec<u8> {
        let replies = if let [(operation, destination, sources)] = commands {
            let sources = sources
                .iter()
                .map(|source| (*source).to_string())
                .collect::<Vec<_>>();
            vec![self
                .session
                .get_db()
                .string_bitop_async(operation, destination, &sources)
                .await]
        } else {
            self.session
                .get_db()
                .string_bitop_batch_async(commands)
                .await
        };
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(len) => append_integer(&mut out, len as i64),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_key_delete_commands(
        &self,
        commands: &[BorrowedKeyDeleteCommand<'_>],
    ) -> Vec<u8> {
        let keys = commands
            .iter()
            .map(|(_, keys)| keys.clone())
            .collect::<Vec<_>>();
        let replies = self
            .session
            .get_db()
            .delete_key_commands_batch_async(&keys)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            append_integer(&mut out, reply as i64);
        }
        out
    }

    async fn handle_borrowed_zset_increment_commands(
        &self,
        commands: &[BorrowedZsetIncrementCommand<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .zset_increment_many_merged_async(commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 16);
        for reply in replies {
            match reply {
                Ok(score) => append_bulk_string(&mut out, score.to_string().as_bytes()),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_xdel_commands(
        &self,
        commands: &[BorrowedStreamDeleteCommand<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .stream_delete_batch_async(commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(deleted) => append_integer(&mut out, deleted as i64),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_zset_pop_commands(
        &self,
        commands: &[BorrowedZsetPopCommand<'_>],
    ) -> Vec<u8> {
        let replies = self.session.get_db().zset_pop_batch_async(commands).await;
        let mut out = Vec::new();
        for reply in replies {
            match reply {
                Ok(entries) => {
                    let response_bytes = entries.iter().try_fold(32usize, |bytes, (member, score)| {
                        bytes
                            .checked_add(member.len())?
                            .checked_add(score.to_string().len())?
                            .checked_add(64)
                            .filter(|bytes| *bytes <= crate::frame::MAX_FRAME_BYTES)
                    });
                    if response_bytes.is_none() {
                        append_error(&mut out, "ERR response exceeds configured limit");
                        continue;
                    }
                    append_array_len(&mut out, entries.len().saturating_mul(2));
                    for (member, score) in entries {
                        append_bulk_string(&mut out, member.as_bytes());
                        append_bulk_string(&mut out, score.to_string().as_bytes());
                    }
                }
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_list_pop_commands(
        &self,
        commands: &[BorrowedListPopCommand<'_>],
    ) -> Vec<u8> {
        let db_commands = commands
            .iter()
            .map(|(key, left, count)| (*key, *left, count.unwrap_or(1)))
            .collect::<Vec<_>>();
        let replies = self
            .session
            .get_db()
            .list_pop_many_merged_async(&db_commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 16);
        for ((_, _, count), reply) in commands.iter().zip(replies) {
            match (count, reply) {
                (None, Ok(values)) => {
                    if let Some(value) = values.into_iter().next() {
                        append_bulk_string(&mut out, &value);
                    } else {
                        append_null(&mut out);
                    }
                }
                (Some(_), Ok(values)) => {
                    append_array_len(&mut out, values.len());
                    for value in values {
                        append_bulk_string(&mut out, &value);
                    }
                }
                (_, Err(error)) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_set_mutations(
        &self,
        mutations: &[SetBatchMutation<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .apply_set_batch_mutations_async(mutations)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(changed) => append_integer(&mut out, changed as i64),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_pfadd_commands(
        &self,
        commands: &[BorrowedHllAddCommand<'_>],
    ) -> Vec<u8> {
        let replies = self.session.get_db().hll_add_batch_async(commands).await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(changed) => append_integer(&mut out, i64::from(changed)),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_root_json_set_commands(
        &self,
        commands: &[BorrowedRootJsonSetCommand<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .json_set_root_batch_async(commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 8);
        for reply in replies {
            match reply {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_xadd_commands(
        &self,
        commands: &[BorrowedStreamAddCommand<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .stream_add_many_merged_async(commands)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 24);
        for reply in replies {
            match reply {
                Ok(id) => append_bulk_string(&mut out, id.to_redis_id().as_bytes()),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_key_expiration_mutations(
        &self,
        mutations: &[KeyExpirationBatchMutation<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .apply_key_expiration_batch_async(mutations)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 4);
        for reply in replies {
            match reply {
                Ok(value) => append_integer(&mut out, value),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

    async fn handle_borrowed_string_mutations(
        &self,
        mutations: &[StringBatchMutation<'_>],
    ) -> Vec<u8> {
        let replies = self
            .session
            .get_db()
            .apply_string_batch_mutations_async(mutations)
            .await;
        let mut out = Vec::with_capacity(replies.len() * 16);
        for reply in replies {
            match reply {
                Ok(StringBatchReply::Bulk(Some(value))) => append_bulk_string(&mut out, &value),
                Ok(StringBatchReply::Bulk(None)) => append_null(&mut out),
                Ok(StringBatchReply::Integer(value)) => append_integer(&mut out, value),
                Ok(StringBatchReply::Ok) => out.extend_from_slice(b"+OK\r\n"),
                Err(error) => append_error(&mut out, &error.to_string()),
            }
        }
        out
    }

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

    async fn handle_borrowed_collection_read_commands(
        &self,
        commands: Vec<Vec<&[u8]>>,
    ) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let ops = commands
            .into_iter()
            .map(|args| {
                let Ok(key) = std::str::from_utf8(args[1]) else {
                    return BorrowedCollectionReadOp::Error("ERR invalid UTF-8 key".to_string());
                };
                if args[0].eq_ignore_ascii_case(b"SMEMBERS") {
                    BorrowedCollectionReadOp::SetMembers(key.to_string())
                } else if args[0].eq_ignore_ascii_case(b"HGETALL") {
                    BorrowedCollectionReadOp::HashGetAll(key.to_string())
                } else if args[0].eq_ignore_ascii_case(b"HKEYS") {
                    BorrowedCollectionReadOp::HashKeys(key.to_string())
                } else {
                    BorrowedCollectionReadOp::HashValues(key.to_string())
                }
            })
            .collect();
        match self
            .command_executor
            .execute(async move { encode_borrowed_collection_read_ops(db, ops).await })
            .await
        {
            Ok(out) => out,
            Err(error) => Frame::Error(error.to_string()).as_bytes(),
        }
    }
}
