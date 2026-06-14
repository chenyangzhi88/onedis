impl Handler {
    pub fn new(
        db_manager: Arc<DatabaseManager>,
        session_manager: Arc<SessionManager>,
        command_executor: Arc<CommandExecutor>,
        wasm_registry: Arc<WasmRegistry>,
        stream: TcpStream,
        args: Arc<ResolvedArgs>,
    ) -> Self {
        let args_ref = args.as_ref();
        let certification = args_ref.requirepass.is_none();
        let db = db_manager.get_db(0);
        let connection = Connection::new(stream);
        let session = Session::new(certification, db);
        session_manager.create_session(session.clone());

        Handler {
            session,
            connection,
            session_manager,
            db_manager,
            command_executor,
            wasm_registry,
            args,
            transaction_db: None,
        }
    }

    pub fn login(&mut self, username: Option<&str>, input_requirepass: &str) -> Result<(), Error> {
        if let Some(username) = username {
            if self
                .session_manager
                .acl_authenticate(username, input_requirepass)
            {
                self.session.set_user(username.to_string());
                self.session.set_certification(true);
                self.session_manager.update_session(self.session.clone());
                return Ok(());
            }
            return Err(Error::msg(
                "WRONGPASS invalid username-password pair or user is disabled.",
            ));
        }
        if let Some(ref requirepass) = self.args.requirepass {
            if requirepass == input_requirepass {
                self.session.set_certification(true);
                self.session_manager.update_session(self.session.clone());
                return Ok(());
            }
            return Err(Error::msg("ERR invalid password"));
        } else {
            Ok(())
        }
    }

    pub fn change_db(&mut self, idx: usize) -> Result<(), Error> {
        if self.args.databases - 1 < idx {
            return Err(Error::msg("ERR DB index is out of range"));
        }
        self.session.set_current_db(idx);
        self.session.set_db(self.db_manager.get_db(idx));
        Ok(())
    }

    /// Handling client connections
    pub async fn handle(&mut self) {
        loop {
            log::debug!("Waiting for bytes");
            let bytes = match self.connection.read_bytes().await {
                Ok(bytes) => bytes,
                Err(_e) => {
                    self.session_manager.remove_session(self.session.get_id());
                    return;
                }
            };

            if let Some(response_bytes) = self.try_handle_ping_fast_batch(bytes.as_slice()) {
                self.connection.write_bytes(response_bytes).await;
                continue;
            }
            if let Some(response_bytes) =
                self.try_handle_borrowed_fast_batch(bytes.as_slice()).await
            {
                self.connection.write_bytes(response_bytes).await;
                continue;
            }

            // 解析可能的多个粘连命令帧
            let frames = match Frame::parse_multiple_frames(bytes.as_slice()) {
                Ok(frames) => frames,
                Err(e) => {
                    log::error!("Failed to parse multiple frames: {:?}", e);
                    let frame = Frame::Error(format!("Failed to parse frames: {:?}", e));
                    self.connection.write_bytes(frame.as_bytes()).await;
                    continue;
                }
            };

            log::debug!(
                "Received bytes: {:?}",
                String::from_utf8_lossy(bytes.as_slice())
            );

            let mut response_bytes = Vec::new();
            for frame in frames {
                log::debug!("Received frame: {}", frame.to_string());
                if self.session.is_in_transaction() {
                    let command_name = frame.get_arg(0).unwrap_or_default().to_uppercase();
                    if command_name != "EXEC" && command_name != "DISCARD" {
                        self.queue_transaction_frame(frame, &mut response_bytes);
                        continue;
                    }
                }

                let command = match Command::parse_from_frame(frame) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        let frame = Frame::Error(e.to_string());
                        response_bytes.extend(frame.as_bytes());
                        continue;
                    }
                };
                self.session
                    .set_last_cmd(command.name().to_ascii_lowercase());
                self.session_manager.update_session(self.session.clone());

                match command {
                    Command::Auth(_) => {}
                    _ => {
                        if self.args.requirepass.is_some() {
                            if self.session.get_certification() == false {
                                let frame =
                                    Frame::Error("NOAUTH Authentication required.".to_string());
                                response_bytes.extend(frame.as_bytes());
                                continue;
                            }
                        }
                        if !self
                            .session_manager
                            .acl_allows(self.session.user(), command.name())
                        {
                            let frame = Frame::Error(format!(
                                "NOPERM this user has no permissions to run the '{}' command",
                                command.name().to_ascii_lowercase()
                            ));
                            response_bytes.extend(frame.as_bytes());
                            continue;
                        }
                    }
                };

                match self.apply_command_response_bytes(command).await {
                    Ok(bytes) => {
                        response_bytes.extend(bytes);
                    }
                    Err(e) => {
                        log::error!("Failed to receive; err = {:?}", e);
                        response_bytes.extend(Frame::Error(e.to_string()).as_bytes());
                    }
                }
            }
            if !response_bytes.is_empty() {
                self.connection.write_bytes(response_bytes).await;
            }
        }
    }

    async fn apply_command_response_bytes(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        self.session_manager
            .broadcast_monitor(self.session.get_id(), format_command_for_monitor(&command))
            .await;
        if let Some(bytes) = self.try_apply_pubsub_or_monitor(&command).await? {
            return Ok(bytes);
        }
        if let Command::Exec(_) = command {
            return self
                .execute_transaction_async()
                .await
                .map(|frame| frame.as_bytes());
        }
        if Self::is_blocking_list_command(&command) {
            return self.apply_blocking_list_command(command).await;
        }
        if Self::is_blocking_zset_command(&command) {
            return self.apply_blocking_zset_command(command).await;
        }
        if Self::is_blocking_stream_command(&command) {
            return self.apply_blocking_stream_command(command).await;
        }
        if let Command::Wasm(wasm) = command {
            let registry = self.wasm_registry.clone();
            let db = self.session.get_db().clone();
            return self
                .command_executor
                .execute(async move { wasm.apply(&registry, db).await.as_bytes() })
                .await;
        }
        if matches!(command, Command::Lrange(_)) {
            let db = self.session.get_db().clone();
            return self
                .command_executor
                .execute(
                    async move { db.handle_command_async(command).await.map(|f| f.as_bytes()) },
                )
                .await?;
        }
        if !Self::can_apply_on_worker(&command) {
            let should_notify = Self::is_list_mutating_command(&command);
            let should_notify_zset = Self::is_zset_mutating_command(&command);
            let should_notify_stream = Self::is_stream_mutating_command(&command);
            let frame = self.apply_command(command).await?;
            if should_notify && !matches!(frame, Frame::Error(_)) {
                self.db_manager.notify_list_waiters();
            }
            if should_notify_zset && !matches!(frame, Frame::Error(_)) {
                self.db_manager.notify_zset_waiters();
            }
            if should_notify_stream && !matches!(frame, Frame::Error(_)) {
                self.db_manager.notify_stream_waiters();
            }
            return Ok(frame.as_bytes());
        }
        if let Command::Move(r#move) = &command
            && self.args.databases <= r#move.get_db_index()
        {
            return Ok(Frame::Error("ERR DB index is out of range".to_string()).as_bytes());
        }
        if let Command::Copy(copy) = &command
            && copy
                .db_index()
                .is_some_and(|db_index| self.args.databases <= db_index)
        {
            return Ok(Frame::Error("ERR DB index is out of range".to_string()).as_bytes());
        }

        let db = self.session.get_db().clone();
        let direct = Self::can_apply_direct(&command);
        let should_notify = Self::is_list_mutating_command(&command);
        let should_notify_zset = Self::is_zset_mutating_command(&command);
        let should_notify_stream = Self::is_stream_mutating_command(&command);
        let frame = if direct {
            db.handle_command_async(command).await
        } else {
            db.handle_command_autocommit_async(command).await
        }?;
        if should_notify && !matches!(frame, Frame::Error(_)) {
            self.db_manager.notify_list_waiters();
        }
        if should_notify_zset && !matches!(frame, Frame::Error(_)) {
            self.db_manager.notify_zset_waiters();
        }
        if should_notify_stream && !matches!(frame, Frame::Error(_)) {
            self.db_manager.notify_stream_waiters();
        }
        Ok(frame.as_bytes())
    }

    async fn try_apply_pubsub_or_monitor(
        &mut self,
        command: &Command,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Command::Unknown(unknown) = command else {
            return Ok(None);
        };
        let name = unknown.command_name().to_ascii_uppercase();
        let args = unknown.args();
        match name.as_str() {
            "MONITOR" => {
                let writer = self.connection.shared_writer();
                self.session_manager
                    .add_monitor(self.session.get_id(), writer);
                Ok(Some(Frame::Ok.as_bytes()))
            }
            "ACL" => Ok(Some(self.apply_acl(args).as_bytes())),
            "PUBLISH" | "SPUBLISH" => {
                if args.len() != 2 {
                    return Ok(Some(
                        Frame::Error(format!(
                            "ERR wrong number of arguments for '{}' command",
                            name.to_ascii_lowercase()
                        ))
                        .as_bytes(),
                    ));
                }
                let delivered = self
                    .session_manager
                    .publish(&args[0], &args[1], name == "SPUBLISH")
                    .await;
                Ok(Some(Frame::Integer(delivered as i64).as_bytes()))
            }
            "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                let writer = self.connection.shared_writer();
                let mut frames = Vec::new();
                for (idx, channel) in args.iter().enumerate() {
                    match name.as_str() {
                        "SUBSCRIBE" => self.session_manager.register_channel(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        "PSUBSCRIBE" => self.session_manager.register_pattern(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        "SSUBSCRIBE" => self.session_manager.register_shard_channel(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        _ => {}
                    }
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::bulk_string(channel.clone()),
                            Frame::Integer((idx + 1) as i64),
                        ])
                        .as_bytes(),
                    );
                }
                Ok(Some(frames))
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" => {
                let channels = if args.is_empty() {
                    Vec::new()
                } else {
                    args.to_vec()
                };
                if channels.is_empty() {
                    self.session_manager.unsubscribe_all(self.session.get_id());
                }
                let mut frames = Vec::new();
                for channel in channels {
                    match name.as_str() {
                        "UNSUBSCRIBE" => self
                            .session_manager
                            .unregister_channel(&channel, self.session.get_id()),
                        "PUNSUBSCRIBE" => self
                            .session_manager
                            .unregister_pattern(&channel, self.session.get_id()),
                        "SUNSUBSCRIBE" => self
                            .session_manager
                            .unregister_shard_channel(&channel, self.session.get_id()),
                        _ => {}
                    }
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::bulk_string(channel),
                            Frame::Integer(0),
                        ])
                        .as_bytes(),
                    );
                }
                if frames.is_empty() {
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::Null,
                            Frame::Integer(0),
                        ])
                        .as_bytes(),
                    );
                }
                Ok(Some(frames))
            }
            "PUBSUB" => Ok(Some(self.apply_pubsub_introspection(args).as_bytes())),
            _ => Ok(None),
        }
    }

    fn apply_acl(&mut self, args: &[String]) -> Frame {
        match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
            Some("WHOAMI") => Frame::bulk_string(self.session.user().to_string()),
            Some("USERS") => Frame::Array(
                self.session_manager
                    .acl_users()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("LIST") => Frame::Array(
                self.session_manager
                    .acl_list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("SETUSER") if args.len() >= 2 => {
                match self.session_manager.acl_setuser(&args[1], &args[2..]) {
                    Ok(()) => Frame::Ok,
                    Err(err) => Frame::Error(err),
                }
            }
            Some("DELUSER") if args.len() >= 2 => {
                Frame::Integer(self.session_manager.acl_deluser(&args[1..]) as i64)
            }
            Some("CAT") => Frame::Array(Vec::new()),
            Some("HELP") => Frame::Array(vec![Frame::bulk_string("ACL SETUSER <user> [rule ...]")]),
            _ => Frame::Error("ERR syntax error".to_string()),
        }
    }

    fn apply_pubsub_introspection(&self, args: &[String]) -> Frame {
        match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
            Some("NUMSUB") => {
                let mut frames = Vec::new();
                for channel in args.iter().skip(1) {
                    frames.push(Frame::bulk_string(channel.clone()));
                    frames.push(Frame::Integer(
                        self.session_manager.channel_count(channel, false) as i64,
                    ));
                }
                Frame::Array(frames)
            }
            Some("SHARDNUMSUB") => {
                let mut frames = Vec::new();
                for channel in args.iter().skip(1) {
                    frames.push(Frame::bulk_string(channel.clone()));
                    frames.push(Frame::Integer(
                        self.session_manager.channel_count(channel, true) as i64,
                    ));
                }
                Frame::Array(frames)
            }
            Some("NUMPAT") => Frame::Integer(self.session_manager.pattern_count() as i64),
            Some("CHANNELS") => Frame::Array(
                self.session_manager
                    .channel_names(false)
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("SHARDCHANNELS") => Frame::Array(
                self.session_manager
                    .channel_names(true)
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            _ => Frame::Array(Vec::new()),
        }
    }

    async fn apply_blocking_list_command(&self, command: Command) -> Result<Vec<u8>, Error> {
        let timeout_secs = Self::blocking_list_timeout_secs(&command);
        let deadline = (timeout_secs > 0.0).then(|| {
            Instant::now() + Duration::from_micros((timeout_secs * 1_000_000.0).ceil() as u64)
        });

        loop {
            let notified = self.db_manager.list_notify().notified();
            if let Some(frame) = self.try_blocking_list_command_once(&command).await? {
                if !matches!(frame, Frame::Null | Frame::Error(_)) {
                    self.db_manager.notify_list_waiters();
                }
                return Ok(frame.as_bytes());
            }

            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(Frame::Null.as_bytes());
                    }
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Ok(Frame::Null.as_bytes());
                    }
                }
                None => notified.await,
            }
        }
    }

    async fn try_blocking_list_command_once(
        &self,
        command: &Command,
    ) -> Result<Option<Frame>, Error> {
        let db = self.session.get_db().clone();
        let txn_db = db.transactional_view()?;
        let frame = match command {
            Command::Blpop(blpop) => {
                match txn_db
                    .list_multi_pop_async(&blpop.keys, true, 1)
                    .await?
                    .and_then(|(key, mut values)| values.pop().map(|value| (key, value)))
                {
                    Some((key, value)) => Some(Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::bulk_string(value),
                    ])),
                    None => None,
                }
            }
            Command::Brpop(brpop) => match txn_db
                .list_multi_pop_async(&brpop.inner.keys, false, 1)
                .await?
                .and_then(|(key, mut values)| values.pop().map(|value| (key, value)))
            {
                Some((key, value)) => Some(Frame::Array(vec![
                    Frame::bulk_string(key),
                    Frame::bulk_string(value),
                ])),
                None => None,
            },
            Command::Brpoplpush(command) => {
                match txn_db
                    .list_move_async(&command.source, &command.destination, false, true)
                    .await?
                {
                    Some(value) => Some(Frame::bulk_string(value)),
                    None => None,
                }
            }
            Command::Blmove(command) => {
                match txn_db
                    .list_move_async(
                        &command.source,
                        &command.destination,
                        command.source_side.is_left(),
                        command.destination_side.is_left(),
                    )
                    .await?
                {
                    Some(value) => Some(Frame::bulk_string(value)),
                    None => None,
                }
            }
            Command::Blmpop(command) => {
                match txn_db
                    .list_multi_pop_async(&command.keys, command.left, command.count)
                    .await?
                {
                    Some((key, values)) => Some(Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::Array(values.into_iter().map(Frame::bulk_string).collect()),
                    ])),
                    None => None,
                }
            }
            _ => unreachable!("non blocking-list command routed to blocking list handler"),
        };
        txn_db.commit_transaction_async().await?;
        Ok(frame)
    }

    fn blocking_list_timeout_secs(command: &Command) -> f64 {
        match command {
            Command::Blpop(command) => command.timeout_secs,
            Command::Brpop(command) => command.inner.timeout_secs,
            Command::Brpoplpush(command) => command.timeout_secs,
            Command::Blmove(command) => command.timeout_secs,
            Command::Blmpop(command) => command.timeout_secs,
            _ => 0.0,
        }
    }

    fn is_blocking_list_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Blpop(_)
                | Command::Brpop(_)
                | Command::Brpoplpush(_)
                | Command::Blmove(_)
                | Command::Blmpop(_)
        )
    }

    async fn apply_blocking_zset_command(&self, command: Command) -> Result<Vec<u8>, Error> {
        let timeout_secs = Self::blocking_zset_timeout_secs(&command);
        let deadline = (timeout_secs > 0.0).then(|| {
            Instant::now() + Duration::from_micros((timeout_secs * 1_000_000.0).ceil() as u64)
        });
        loop {
            let notified = self.db_manager.zset_notify().notified();
            if let Some(frame) = self.try_blocking_zset_command_once(&command).await? {
                if !matches!(frame, Frame::Null | Frame::Error(_)) {
                    self.db_manager.notify_zset_waiters();
                }
                return Ok(frame.as_bytes());
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(Frame::Null.as_bytes());
                    }
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Ok(Frame::Null.as_bytes());
                    }
                }
                None => notified.await,
            }
        }
    }

    async fn try_blocking_zset_command_once(
        &self,
        command: &Command,
    ) -> Result<Option<Frame>, Error> {
        let db = self.session.get_db().clone();
        let txn_db = db.transactional_view()?;
        let frame = match command {
            Command::Bzpopmin(command) => {
                match txn_db
                    .zset_multi_pop_async(&command.keys, command.min, 1)
                    .await?
                {
                    Some((key, mut entries)) => {
                        let Some((member, score)) = entries.pop() else {
                            return Ok(None);
                        };
                        Some(Frame::Array(vec![
                            Frame::bulk_string(key),
                            Frame::bulk_string(member),
                            Frame::bulk_string(score.to_string()),
                        ]))
                    }
                    None => None,
                }
            }
            Command::Bzpopmax(command) => {
                let inner = &command.inner;
                match txn_db
                    .zset_multi_pop_async(&inner.keys, inner.min, 1)
                    .await?
                {
                    Some((key, mut entries)) => {
                        let Some((member, score)) = entries.pop() else {
                            return Ok(None);
                        };
                        Some(Frame::Array(vec![
                            Frame::bulk_string(key),
                            Frame::bulk_string(member),
                            Frame::bulk_string(score.to_string()),
                        ]))
                    }
                    None => None,
                }
            }
            Command::Bzmpop(command) => {
                match txn_db
                    .zset_multi_pop_async(&command.keys, command.min, command.count)
                    .await?
                {
                    Some((key, entries)) => Some(Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::Array(
                            entries
                                .into_iter()
                                .map(|(member, score)| {
                                    Frame::Array(vec![
                                        Frame::bulk_string(member),
                                        Frame::bulk_string(score.to_string()),
                                    ])
                                })
                                .collect(),
                        ),
                    ])),
                    None => None,
                }
            }
            _ => unreachable!("non blocking-zset command routed to blocking zset handler"),
        };
        txn_db.commit_transaction_async().await?;
        Ok(frame)
    }

    fn blocking_zset_timeout_secs(command: &Command) -> f64 {
        match command {
            Command::Bzpopmin(command) => command.timeout_secs,
            Command::Bzpopmax(command) => command.inner.timeout_secs,
            Command::Bzmpop(command) => command.timeout_secs,
            _ => 0.0,
        }
    }

    fn is_blocking_zset_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Bzpopmin(_) | Command::Bzpopmax(_) | Command::Bzmpop(_)
        )
    }

    async fn apply_blocking_stream_command(&self, command: Command) -> Result<Vec<u8>, Error> {
        let block_ms = Self::blocking_stream_timeout_ms(&command).unwrap_or(0);
        let deadline = (block_ms > 0).then(|| Instant::now() + Duration::from_millis(block_ms));
        loop {
            let notified = self.db_manager.stream_notify().notified();
            let frame = self.try_stream_read_once(&command).await?;
            if !matches!(frame, Frame::Null) {
                return Ok(frame.as_bytes());
            }
            match deadline {
                Some(deadline) => {
                    if Instant::now() >= deadline {
                        return Ok(Frame::Null.as_bytes());
                    }
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Ok(Frame::Null.as_bytes());
                    }
                }
                None => notified.await,
            }
        }
    }

    async fn try_stream_read_once(&self, command: &Command) -> Result<Frame, Error> {
        let db = self.session.get_db().clone();
        match command {
            Command::Xread(command) => {
                let streams = db.stream_read(&command.streams, command.count)?;
                if streams.is_empty() {
                    Ok(Frame::Null)
                } else {
                    Ok(Frame::Array(
                        streams
                            .into_iter()
                            .map(|(key, entries)| {
                                Frame::Array(vec![
                                    Frame::bulk_string(key),
                                    Frame::Array(
                                        entries
                                            .into_iter()
                                            .map(crate::cmds::stream::stream_entry_frame)
                                            .collect(),
                                    ),
                                ])
                            })
                            .collect(),
                    ))
                }
            }
            Command::Xreadgroup(command) => {
                let streams = db
                    .stream_read_group_async(
                        &command.group,
                        &command.consumer,
                        &command.streams,
                        command.count,
                        command.noack,
                    )
                    .await?;
                if streams.is_empty() {
                    Ok(Frame::Null)
                } else {
                    Ok(Frame::Array(
                        streams
                            .into_iter()
                            .map(|(key, entries)| {
                                Frame::Array(vec![
                                    Frame::bulk_string(key),
                                    Frame::Array(
                                        entries
                                            .into_iter()
                                            .map(crate::cmds::stream::stream_entry_frame)
                                            .collect(),
                                    ),
                                ])
                            })
                            .collect(),
                    ))
                }
            }
            _ => unreachable!("non blocking-stream command routed to stream handler"),
        }
    }

    fn blocking_stream_timeout_ms(command: &Command) -> Option<u64> {
        match command {
            Command::Xread(command) => command.block_ms,
            Command::Xreadgroup(command) => command.block_ms,
            _ => None,
        }
    }

    fn is_blocking_stream_command(command: &Command) -> bool {
        Self::blocking_stream_timeout_ms(command).is_some()
    }

    fn is_list_mutating_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Blmove(_)
                | Command::Blmpop(_)
                | Command::Blpop(_)
                | Command::Brpop(_)
                | Command::Brpoplpush(_)
                | Command::Linsert(_)
                | Command::Lmove(_)
                | Command::Lmpop(_)
                | Command::Lpop(_)
                | Command::Lpush(_)
                | Command::Lpushx(_)
                | Command::Lset(_)
                | Command::Ltrim(_)
                | Command::Rpop(_)
                | Command::Rpoplpush(_)
                | Command::Rpush(_)
                | Command::Rpushx(_)
        )
    }

    fn is_zset_mutating_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Bzmpop(_)
                | Command::Bzpopmax(_)
                | Command::Bzpopmin(_)
                | Command::Zadd(_)
                | Command::Zdiffstore(_)
                | Command::Zincrby(_)
                | Command::Zinterstore(_)
                | Command::Zmpop(_)
                | Command::Zpopmax(_)
                | Command::Zpopmin(_)
                | Command::Zrangestore(_)
                | Command::Zrem(_)
                | Command::Zremrangebylex(_)
                | Command::Zremrangebyrank(_)
                | Command::Zremrangebyscore(_)
                | Command::Zunionstore(_)
        )
    }

    fn is_stream_mutating_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Xadd(_) | Command::Xdel(_) | Command::Xtrim(_)
        )
    }

    /// 执行命令（直接调用 Db，无 channel 开销）
    async fn apply_command(&mut self, command: Command) -> Result<Frame, Error> {
        match command {
            Command::Auth(auth) => auth.apply(self),
            Command::Client(client) => client.apply_with_handler(self),
            Command::Config(config) => config.apply(self.args.as_ref()),
            Command::Save(_) | Command::Bgsave(_) => Ok(Frame::Ok),
            Command::Flushall(_) => {
                for db in self.db_manager.get_all_dbs() {
                    db.clear_async().await;
                }
                Ok(Frame::Ok)
            }
            Command::Move(r#move) => {
                if self.args.databases <= r#move.get_db_index() {
                    return Ok(Frame::Error("ERR DB index is out of range".to_string()));
                }
                let db = self.session.get_db().clone();
                db.handle_command_autocommit_async(Command::Move(r#move))
                    .await
            }
            Command::Copy(copy) => {
                let db = self.session.get_db().clone();
                db.handle_command_autocommit_async(Command::Copy(copy))
                    .await
            }
            Command::Exec(_) => self.execute_transaction_async().await,
            Command::Multi(multi) => multi.apply(self),
            Command::Discard(discard) => discard.apply(self),
            Command::Watch(watch) => watch.apply(self),
            Command::Unwatch(unwatch) => unwatch.apply(self),
            Command::Select(select) => select.apply(self),
            Command::Unknown(unknown) => unknown.apply(),
            Command::Ping(ping) => ping.apply(),
            Command::Echo(echo) => echo.apply(),
            _ => {
                let db = self.session.get_db().clone();
                if Self::can_apply_direct(&command) {
                    db.handle_command_async(command).await
                } else {
                    db.handle_command_autocommit_async(command).await
                }
            }
        }
    }

    fn can_apply_direct(command: &Command) -> bool {
        matches!(
            command,
            Command::Get(_)
                | Command::Mget(_)
                | Command::Type(_)
                | Command::Ttl(_)
                | Command::Pttl(_)
                | Command::ExpireTime(_)
                | Command::PexpireTime(_)
                | Command::Exists(_)
                | Command::Touch(_)
                | Command::Dbsize(_)
                | Command::Keys(_)
                | Command::Scan(_)
                | Command::Scard(_)
                | Command::Sdiff(_)
                | Command::Sinter(_)
                | Command::Sismember(_)
                | Command::Smembers(_)
                | Command::Smismember(_)
                | Command::Srandmember(_)
                | Command::Sscan(_)
                | Command::Sunion(_)
                | Command::Zcard(_)
                | Command::Zcount(_)
                | Command::Zmscore(_)
                | Command::Zrange(_)
                | Command::Zrangebyscore(_)
                | Command::Zrank(_)
                | Command::Zrevrange(_)
                | Command::Zrevrank(_)
                | Command::Zscan(_)
                | Command::Zscore(_)
                | Command::Strlen(_)
                | Command::Lindex(_)
                | Command::Linsert(_)
                | Command::Llen(_)
                | Command::Lmpop(_)
                | Command::Lpos(_)
                | Command::Lrange(_)
                | Command::Lpush(_)
                | Command::Lpushx(_)
                | Command::Lpop(_)
                | Command::Lrem(_)
                | Command::Lset(_)
                | Command::Ltrim(_)
                | Command::Rpop(_)
                | Command::Rpoplpush(_)
                | Command::Rpush(_)
                | Command::Rpushx(_)
                | Command::Set(_)
                | Command::Setex(_)
                | Command::Mset(_)
                | Command::Incr(_)
                | Command::Incrby(_)
                | Command::Decr(_)
                | Command::Decrby(_)
                | Command::Hset(_)
                | Command::Hdel(_)
                | Command::Hincrby(_)
                | Command::HincrbyFloat(_)
                | Command::Hmset(_)
                | Command::FtCreate(_)
                | Command::FtList(_)
                | Command::FtDropIndex(_)
                | Command::FtAlter(_)
                | Command::FtAliasAdd(_)
                | Command::FtAliasUpdate(_)
                | Command::FtAliasDel(_)
                | Command::FtConfig(_)
                | Command::FtInfo(_)
                | Command::FtSearch(_)
                | Command::FtHybrid(_)
                | Command::FtAggregate(_)
                | Command::FtCursor(_)
                | Command::FtProfile(_)
                | Command::FtDict(_)
                | Command::FtSpellCheck(_)
                | Command::FtSug(_)
                | Command::FtSyn(_)
                | Command::FtUnsupported(_)
                | Command::Flushdb(_)
                | Command::Copy(_)
                | Command::Move(_)
                | Command::Sadd(_)
                | Command::Sdiffstore(_)
                | Command::Sinterstore(_)
                | Command::Spop(_)
                | Command::Srem(_)
                | Command::Sunionstore(_)
                | Command::Zadd(_)
                | Command::Zincrby(_)
                | Command::Zrangestore(_)
                | Command::Zrem(_)
                | Command::Zremrangebyrank(_)
                | Command::Zremrangebyscore(_)
        )
    }

    fn can_apply_on_worker(command: &Command) -> bool {
        matches!(
            command,
            Command::Get(_)
                | Command::Mget(_)
                | Command::Type(_)
                | Command::Ttl(_)
                | Command::Pttl(_)
                | Command::ExpireTime(_)
                | Command::PexpireTime(_)
                | Command::Exists(_)
                | Command::Strlen(_)
                | Command::Dbsize(_)
                | Command::Flushdb(_)
                | Command::Keys(_)
                | Command::Scan(_)
                | Command::Lindex(_)
                | Command::Linsert(_)
                | Command::Llen(_)
                | Command::Lmpop(_)
                | Command::Lpos(_)
                | Command::Lrange(_)
                | Command::Copy(_)
                | Command::Move(_)
                | Command::Scard(_)
                | Command::Sdiff(_)
                | Command::Sdiffstore(_)
                | Command::Sinter(_)
                | Command::Sinterstore(_)
                | Command::Sismember(_)
                | Command::Smembers(_)
                | Command::Smismember(_)
                | Command::Spop(_)
                | Command::Srandmember(_)
                | Command::Sscan(_)
                | Command::Sunion(_)
                | Command::Sunionstore(_)
                | Command::Zcard(_)
                | Command::Zcount(_)
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

    fn try_handle_ping_fast_batch(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        if self.session.is_in_transaction()
            || (self.args.requirepass.is_some() && !self.session.get_certification())
        {
            return None;
        }

        if bytes.chunks_exact(6).remainder().is_empty()
            && bytes
                .chunks_exact(6)
                .all(|chunk| chunk.eq_ignore_ascii_case(b"PING\r\n"))
        {
            let count = bytes.len() / 6;
            let mut out = Vec::with_capacity(count * 7);
            for _ in 0..count {
                out.extend_from_slice(b"+PONG\r\n");
            }
            return Some(out);
        }

        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands
            .iter()
            .all(|args| args.len() == 1 && args[0].eq_ignore_ascii_case(b"PING"))
        {
            let mut out = Vec::with_capacity(commands.len() * 7);
            for _ in commands {
                out.extend_from_slice(b"+PONG\r\n");
            }
            return Some(out);
        }

        None
    }

    async fn try_handle_borrowed_fast_batch(&mut self, bytes: &[u8]) -> Option<Vec<u8>> {
        if self.session.is_in_transaction()
            || (self.args.requirepass.is_some() && !self.session.get_certification())
        {
            return None;
        }
        if let Some(commands) = parse_borrowed_plain_set_commands(bytes) {
            return Some(self.handle_borrowed_set_byte_commands(commands).await);
        }
        if let Some(commands) = parse_borrowed_plain_hset_commands(bytes) {
            return Some(self.handle_borrowed_hset_commands(commands).await);
        }
        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands.iter().all(|args| borrowed_read_supported(args)) {
            return Some(self.handle_borrowed_read_commands(commands));
        }
        if commands
            .iter()
            .all(|args| borrowed_plain_set_supported(args))
        {
            return Some(self.handle_borrowed_set_commands(commands).await);
        }
        if commands
            .iter()
            .all(|args| borrowed_list_push_supported(args))
        {
            let response = self.handle_borrowed_list_push_commands(commands).await;
            self.db_manager.notify_list_waiters();
            return Some(response);
        }
        if commands.iter().all(|args| borrowed_lrange_supported(args)) {
            return Some(self.handle_borrowed_lrange_commands(commands).await);
        }
        None
    }


}
