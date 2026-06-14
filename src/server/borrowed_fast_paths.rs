impl Handler {
    fn handle_borrowed_read_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut out = Vec::with_capacity(commands.len() * 16);
        for args in commands {
            let command = args.first().copied().unwrap_or_default();
            if command.eq_ignore_ascii_case(b"GET") {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'get' command");
                    continue;
                }
                match db.get_string_entry_raw_bytes(args[1]) {
                    Ok(Some(raw)) => {
                        let value = decode_string_bytes_slice(&raw).unwrap_or_default();
                        append_bulk_string(&mut out, value);
                    }
                    Ok(None) => append_null(&mut out),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"MGET") {
                append_array_len(&mut out, args.len().saturating_sub(1));
                for key_bytes in &args[1..] {
                    match db.get_string_entry_raw_bytes(key_bytes) {
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

    /// 执行事务中的所有命令
    #[allow(dead_code)]
    fn execute_transaction(&mut self) -> Result<Frame, Error> {
        if !self.session.is_in_transaction() {
            return Ok(Frame::Error("ERR EXEC without MULTI".to_string()));
        }

        if self.session.is_transaction_dirty() {
            self.transaction_db = None;
            self.clear_transaction_and_watches();
            return Ok(Frame::Error(
                "EXECABORT Transaction discarded because of previous errors".to_string(),
            ));
        }

        let transaction_frames = self.session.get_transaction_frames().clone();
        let mut parsed_commands = Vec::with_capacity(transaction_frames.len());

        for frame in &transaction_frames {
            match Command::parse_from_frame(frame.clone()) {
                Ok(command) => parsed_commands.push(command),
                Err(error) => {
                    self.clear_transaction_and_watches();
                    self.transaction_db = None;
                    return Ok(Frame::Error(format!(
                        "EXECABORT Transaction discarded because command parsing failed: {}",
                        error
                    )));
                }
            }
        }

        let Some(txn_db) = self.transaction_db.as_ref() else {
            self.clear_transaction_and_watches();
            return Ok(Frame::Error("ERR transaction state is missing".to_string()));
        };
        if self.watched_keys_modified() {
            self.transaction_db = None;
            self.clear_transaction_and_watches();
            return Ok(Frame::Null);
        }
        let frame =
            Self::execute_transaction_commands(txn_db, parsed_commands, self.args.databases);
        self.transaction_db = None;
        self.clear_transaction_and_watches();
        frame
    }

    async fn execute_transaction_async(&mut self) -> Result<Frame, Error> {
        if !self.session.is_in_transaction() {
            return Ok(Frame::Error("ERR EXEC without MULTI".to_string()));
        }

        if self.session.is_transaction_dirty() {
            self.transaction_db = None;
            self.clear_transaction_and_watches();
            return Ok(Frame::Error(
                "EXECABORT Transaction discarded because of previous errors".to_string(),
            ));
        }

        let transaction_frames = self.session.get_transaction_frames().clone();
        let mut parsed_commands = Vec::with_capacity(transaction_frames.len());

        for frame in &transaction_frames {
            match Command::parse_from_frame(frame.clone()) {
                Ok(command) => parsed_commands.push(command),
                Err(error) => {
                    self.clear_transaction_and_watches();
                    self.transaction_db = None;
                    return Ok(Frame::Error(format!(
                        "EXECABORT Transaction discarded because command parsing failed: {}",
                        error
                    )));
                }
            }
        }

        let Some(txn_db) = self.transaction_db.as_ref() else {
            self.clear_transaction_and_watches();
            return Ok(Frame::Error("ERR transaction state is missing".to_string()));
        };
        if self.watched_keys_modified() {
            self.transaction_db = None;
            self.clear_transaction_and_watches();
            return Ok(Frame::Null);
        }
        let frame =
            Self::execute_transaction_commands_async(txn_db, parsed_commands, self.args.databases)
                .await;
        self.transaction_db = None;
        self.clear_transaction_and_watches();
        frame
    }

    fn queue_transaction_frame(&mut self, frame: Frame, response_bytes: &mut Vec<u8>) {
        match Command::parse_from_frame(frame.clone()) {
            Ok(command) if Self::can_queue_transaction_command(&command) => {
                self.session.add_transaction_frame(frame);
                response_bytes.extend(Frame::SimpleString("QUEUED".to_string()).as_bytes());
            }
            Ok(command) => {
                self.session.mark_transaction_dirty();
                response_bytes.extend(
                    Frame::Error(format!(
                        "ERR command '{}' is not allowed in MULTI",
                        command.name()
                    ))
                    .as_bytes(),
                );
            }
            Err(error) => {
                self.session.mark_transaction_dirty();
                response_bytes.extend(Frame::Error(error.to_string()).as_bytes());
            }
        }
    }

    #[allow(dead_code)]
    fn execute_transaction_commands(
        txn_db: &crate::store::db::Db,
        parsed_commands: Vec<Command>,
        database_count: usize,
    ) -> Result<Frame, Error> {
        let mut results = Vec::new();
        for command in parsed_commands {
            if !Self::can_queue_transaction_command(&command) {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command '{}' is not allowed in MULTI",
                    command.name()
                )));
            }
            if let Command::Move(r#move) = &command
                && database_count <= r#move.get_db_index()
            {
                return Ok(Frame::Error(
                    "EXECABORT Transaction discarded because command failed: ERR DB index is out of range"
                        .to_string(),
                ));
            }
            if let Command::Copy(copy) = &command
                && copy
                    .db_index()
                    .is_some_and(|db_index| database_count <= db_index)
            {
                return Ok(Frame::Error(
                    "EXECABORT Transaction discarded because command failed: ERR DB index is out of range"
                        .to_string(),
                ));
            }

            let frame = match txn_db.handle_command(command) {
                Ok(frame) => frame,
                Err(error) => {
                    return Ok(Frame::Error(format!(
                        "EXECABORT Transaction discarded because command failed: {}",
                        error
                    )));
                }
            };
            if let Frame::Error(error) = frame {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command failed: {}",
                    error
                )));
            }
            results.push(frame);
        }

        if let Err(error) = txn_db.commit_transaction() {
            return Ok(Frame::Error(format!(
                "EXECABORT Transaction discarded because commit failed: {}",
                error
            )));
        }
        Ok(Frame::Array(results))
    }

    async fn execute_transaction_commands_async(
        txn_db: &crate::store::db::Db,
        parsed_commands: Vec<Command>,
        database_count: usize,
    ) -> Result<Frame, Error> {
        let mut results = Vec::new();
        for command in parsed_commands {
            if !Self::can_queue_transaction_command(&command) {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command '{}' is not allowed in MULTI",
                    command.name()
                )));
            }
            if let Command::Move(r#move) = &command
                && database_count <= r#move.get_db_index()
            {
                return Ok(Frame::Error(
                    "EXECABORT Transaction discarded because command failed: ERR DB index is out of range"
                        .to_string(),
                ));
            }
            if let Command::Copy(copy) = &command
                && copy
                    .db_index()
                    .is_some_and(|db_index| database_count <= db_index)
            {
                return Ok(Frame::Error(
                    "EXECABORT Transaction discarded because command failed: ERR DB index is out of range"
                        .to_string(),
                ));
            }

            let frame = match txn_db.handle_command_async(command).await {
                Ok(frame) => frame,
                Err(error) => {
                    return Ok(Frame::Error(format!(
                        "EXECABORT Transaction discarded because command failed: {}",
                        error
                    )));
                }
            };
            if let Frame::Error(error) = frame {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command failed: {}",
                    error
                )));
            }
            results.push(frame);
        }

        if let Err(error) = txn_db.commit_transaction_async().await {
            return Ok(Frame::Error(format!(
                "EXECABORT Transaction discarded because commit failed: {}",
                error
            )));
        }
        Ok(Frame::Array(results))
    }

    fn can_queue_transaction_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Append(_)
                | Command::Copy(_)
                | Command::Dbsize(_)
                | Command::Decr(_)
                | Command::Decrby(_)
                | Command::Del(_)
                | Command::Exists(_)
                | Command::Expire(_)
                | Command::ExpireAt(_)
                | Command::ExpireTime(_)
                | Command::Flushdb(_)
                | Command::Get(_)
                | Command::GetDel(_)
                | Command::GetEx(_)
                | Command::GetRange(_)
                | Command::GetSet(_)
                | Command::Hdel(_)
                | Command::Hexists(_)
                | Command::Hget(_)
                | Command::Hgetall(_)
                | Command::Hincrby(_)
                | Command::HincrbyFloat(_)
                | Command::Hkeys(_)
                | Command::Hlen(_)
                | Command::Hmget(_)
                | Command::Hmset(_)
                | Command::Hscan(_)
                | Command::Hrandfield(_)
                | Command::Hset(_)
                | Command::Hsetnx(_)
                | Command::Hstrlen(_)
                | Command::Hvals(_)
                | Command::Incr(_)
                | Command::Incrby(_)
                | Command::IncrbyFloat(_)
                | Command::Keys(_)
                | Command::Lindex(_)
                | Command::Linsert(_)
                | Command::Llen(_)
                | Command::Lmove(_)
                | Command::Lmpop(_)
                | Command::Lpop(_)
                | Command::Lpos(_)
                | Command::Lpush(_)
                | Command::Lpushx(_)
                | Command::Lrange(_)
                | Command::Lset(_)
                | Command::Ltrim(_)
                | Command::Move(_)
                | Command::Mget(_)
                | Command::Mset(_)
                | Command::Msetnx(_)
                | Command::Persist(_)
                | Command::Pexpire(_)
                | Command::PexpireAt(_)
                | Command::PexpireTime(_)
                | Command::Psetex(_)
                | Command::Pttl(_)
                | Command::RandomKey(_)
                | Command::Rename(_)
                | Command::Renamenx(_)
                | Command::Rpop(_)
                | Command::Rpoplpush(_)
                | Command::Rpush(_)
                | Command::Rpushx(_)
                | Command::Sadd(_)
                | Command::Scard(_)
                | Command::Scan(_)
                | Command::Sdiff(_)
                | Command::Sdiffstore(_)
                | Command::Set(_)
                | Command::SetRange(_)
                | Command::Setex(_)
                | Command::Setnx(_)
                | Command::Sinter(_)
                | Command::Sismember(_)
                | Command::Sinterstore(_)
                | Command::Smismember(_)
                | Command::Smembers(_)
                | Command::Spop(_)
                | Command::Srandmember(_)
                | Command::Srem(_)
                | Command::Sscan(_)
                | Command::Strlen(_)
                | Command::Sunion(_)
                | Command::Sunionstore(_)
                | Command::Ttl(_)
                | Command::Touch(_)
                | Command::Type(_)
                | Command::Unlink(_)
                | Command::Xack(_)
                | Command::Xadd(_)
                | Command::Xautoclaim(_)
                | Command::Xclaim(_)
                | Command::Xdel(_)
                | Command::Xgroup(_)
                | Command::Xinfo(_)
                | Command::Xlen(_)
                | Command::Xpending(_)
                | Command::Xrange(_)
                | Command::Xread(_)
                | Command::Xreadgroup(_)
                | Command::Xrevrange(_)
                | Command::Xtrim(_)
                | Command::Zadd(_)
                | Command::Zcard(_)
                | Command::Zcount(_)
                | Command::Zincrby(_)
                | Command::Zmscore(_)
                | Command::Zrange(_)
                | Command::Zrangebyscore(_)
                | Command::Zrangestore(_)
                | Command::Zrank(_)
                | Command::Zrem(_)
                | Command::Zremrangebyrank(_)
                | Command::Zremrangebyscore(_)
                | Command::Zrevrange(_)
                | Command::Zrevrank(_)
                | Command::Zscan(_)
                | Command::Zscore(_)
        )
    }

    // 事务相关方法
    pub fn start_transaction(&mut self) -> Result<(), Error> {
        self.session.start_transaction();
        let db = self.session.get_db().clone();
        self.transaction_db = Some(db.transactional_view()?);
        Ok(())
    }

    pub fn is_in_transaction(&self) -> bool {
        self.session.is_in_transaction()
    }

    pub fn add_transaction_frame(&mut self, frame: Frame) {
        self.session.add_transaction_frame(frame);
    }

    pub fn get_transaction_frames(&self) -> Vec<Frame> {
        self.session.get_transaction_frames().clone()
    }

    pub fn clear_transaction(&mut self) {
        self.clear_transaction_and_watches();
        self.transaction_db = None;
    }

    pub fn watch_keys(&mut self, keys: Vec<String>) -> Result<(), Error> {
        if self.session.is_in_transaction() {
            return Err(Error::msg("ERR WATCH inside MULTI is not allowed"));
        }
        let db_index = self.session.get_current_db();
        let db = self.session.get_db().clone();
        for key in keys {
            let (key_version, db_version) = db.watch_version_snapshot(&key);
            self.session.watch_key(WatchedKey {
                db_index,
                key,
                key_version,
                db_version,
            });
        }
        Ok(())
    }

    pub fn clear_watches(&mut self) {
        self.session.clear_watches();
    }

    fn clear_transaction_and_watches(&mut self) {
        self.session.clear_transaction();
        self.session.clear_watches();
    }

    fn watched_keys_modified(&self) -> bool {
        self.session.watched_keys().iter().any(|watched| {
            let db = self.db_manager.get_db(watched.db_index);
            db.watch_version_changed(&watched.key, watched.key_version, watched.db_version)
        })
    }


}
