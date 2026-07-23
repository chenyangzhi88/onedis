impl Handler {
    const MAX_RESPONSE_BUFFER_BYTES: usize = 128 * 1024 * 1024;

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
        let peer_addr = stream
            .peer_addr()
            .map(|address| address.to_string())
            .unwrap_or_else(|_| "unknown:0".to_string());
        let local_addr = stream
            .local_addr()
            .map(|address| address.to_string())
            .unwrap_or_else(|_| "unknown:0".to_string());
        let connection = Connection::new(stream);
        let session = Session::new_with_addresses(certification, db, peer_addr, local_addr);
        session_manager.create_session(&session);
        let metrics = crate::observability::metrics::global_metrics();
        metrics.connection_opened();

        Handler {
            session,
            connection,
            session_manager,
            db_manager,
            command_executor,
            wasm_registry,
            args,
            transaction_db: None,
            metrics,
        }
    }

    pub fn login(&mut self, username: Option<&str>, input_requirepass: &str) -> Result<(), Error> {
        if let Some(username) = username {
            if username.eq_ignore_ascii_case("default")
                && let Some(requirepass) = self.args.requirepass.as_deref()
                && requirepass != input_requirepass
            {
                return Err(Error::msg(
                    "WRONGPASS invalid username-password pair or user is disabled.",
                ));
            }
            if self
                .session_manager
                .acl_authenticate(username, input_requirepass)
            {
                self.session.set_user(username.to_string());
                self.session.set_certification(true);
                self.session_manager.update_session(&self.session);
                return Ok(());
            }
            return Err(Error::msg(
                "WRONGPASS invalid username-password pair or user is disabled.",
            ));
        }
        if let Some(ref requirepass) = self.args.requirepass {
            if requirepass == input_requirepass {
                self.session.set_certification(true);
                self.session_manager.update_session(&self.session);
                return Ok(());
            }
            Err(Error::msg("ERR invalid password"))
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
        self.session_manager.update_session(&self.session);
        Ok(())
    }

    /// Handling client connections
    pub async fn handle(&mut self) {
        const PIPELINE_FLUSH_COMMANDS: usize = 32;
        const PIPELINE_FLUSH_BYTES: usize = 64 * 1024;
        const PIPELINE_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(5);

        loop {
            log::debug!("Waiting for bytes");
            let bytes = match self.connection.read_bytes().await {
                Ok(bytes) => bytes,
                Err(error) => {
                    let message = error.to_string();
                    if message.starts_with("ERR Protocol error") {
                        let response = Frame::Error(message).as_bytes();
                        self.metrics.add_output_bytes(response.len());
                        let _ = self.connection.write_bytes(response).await;
                    }
                    return;
                }
            };
            self.metrics.add_input_bytes(bytes.len());

            if let Some(response_bytes) = self.try_handle_ping_fast_batch(bytes.as_slice()) {
                self.metrics.add_output_bytes(response_bytes.len());
                if !self.connection.write_bytes(response_bytes).await {
                    return;
                }
                continue;
            }
            if let Some(response_bytes) =
                self.try_handle_borrowed_fast_batch(bytes.as_slice()).await
            {
                self.metrics.add_output_bytes(response_bytes.len());
                if !self.connection.write_bytes(response_bytes).await {
                    return;
                }
                continue;
            }

            // 解析可能的多个粘连命令帧
            let frames = match Frame::parse_multiple_frames(bytes.as_slice()) {
                Ok(frames) => frames,
                Err(e) => {
                    self.metrics.record_parse_error();
                    log::error!("Failed to parse multiple frames: {:?}", e);
                    let response = Frame::Error(format!("ERR Protocol error: {e}")).as_bytes();
                    self.metrics.add_output_bytes(response.len());
                    let _ = self.connection.write_bytes(response).await;
                    return;
                }
            };
            self.metrics.record_protocol_frames(frames.len());

            let mut response_bytes = Vec::new();
            let mut frames_since_flush = 0usize;
            let mut last_flush = std::time::Instant::now();
            let mut close_after_reply = false;
            for frame in frames {
                if !response_bytes.is_empty()
                    && (frames_since_flush >= PIPELINE_FLUSH_COMMANDS
                        || response_bytes.len() >= PIPELINE_FLUSH_BYTES
                        || last_flush.elapsed() >= PIPELINE_FLUSH_INTERVAL)
                {
                    let response = std::mem::take(&mut response_bytes);
                    self.metrics.add_output_bytes(response.len());
                    if !self.connection.write_bytes(response).await {
                        return;
                    }
                    frames_since_flush = 0;
                    last_flush = std::time::Instant::now();
                }
                frames_since_flush += 1;
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
                        self.metrics.record_parse_error();
                        let frame = Frame::Error(e.to_string());
                        response_bytes.extend(frame.as_bytes());
                        continue;
                    }
                };
                let command_name = command.name();
                let effective_command_name = command.effective_name().to_ascii_uppercase();
                self.session
                    .set_last_cmd(effective_command_name.to_ascii_lowercase());
                self.session_manager.update_session(&self.session);

                match command {
                    Command::Auth(_) => {}
                    _ => {
                        if self.args.requirepass.is_some()
                            && !self.session.get_certification()
                        {
                            self.metrics.record_rejection("noauth");
                            self.metrics.record_command(
                                command_name,
                                1,
                                Some("noauth"),
                                self.slow_command_threshold_us(),
                            );
                            let frame =
                                Frame::Error("NOAUTH Authentication required.".to_string());
                            response_bytes.extend(frame.as_bytes());
                            continue;
                        }
                        if !self
                            .session_manager
                            .acl_allows(self.session.user(), &effective_command_name)
                        {
                            self.metrics.record_rejection("noperm");
                            self.metrics.record_command(
                                command_name,
                                1,
                                Some("noperm"),
                                self.slow_command_threshold_us(),
                            );
                            let frame = Frame::Error(format!(
                                "NOPERM this user has no permissions to run the '{}' command",
                                effective_command_name.to_ascii_lowercase()
                            ));
                            response_bytes.extend(frame.as_bytes());
                            continue;
                        }
                    }
                };

                if self
                    .session_manager
                    .subscription_count(self.session.get_id())
                    > 0
                    && !Self::command_allowed_while_subscribed(&effective_command_name)
                {
                    response_bytes.extend(
                        Frame::Error(format!(
                            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                            effective_command_name.to_ascii_lowercase()
                        ))
                        .as_bytes(),
                    );
                    continue;
                }

                if effective_command_name == "QUIT" {
                    self.session_manager.broadcast_monitor(
                        self.session.get_id(),
                        format_command_name_for_monitor_context(
                            "QUIT",
                            self.session.get_current_db(),
                            self.session.peer_addr(),
                        ),
                    );
                    response_bytes.extend(Frame::Ok.as_bytes());
                    close_after_reply = true;
                    break;
                }

                let self_monitor_line = self
                    .session_manager
                    .is_monitoring(self.session.get_id())
                    .then(|| {
                        format_command_name_for_monitor_context(
                            &effective_command_name,
                            self.session.get_current_db(),
                            self.session.peer_addr(),
                        )
                    });
                let started = std::time::Instant::now();
                match self.apply_command_response_bytes(command).await {
                    Ok(bytes) => {
                        self.metrics.record_command(
                            command_name,
                            crate::observability::metrics::elapsed_us(started),
                            crate::observability::metrics::classify_error_response(&bytes),
                            self.slow_command_threshold_us(),
                        );
                        if bytes.len() > Self::MAX_RESPONSE_BUFFER_BYTES {
                            response_bytes.extend(
                                Frame::Error("ERR response exceeds configured limit".to_string())
                                    .as_bytes(),
                            );
                            close_after_reply = true;
                            break;
                        }
                        response_bytes.extend(bytes);
                    }
                    Err(e) => {
                        self.metrics.record_command(
                            command_name,
                            crate::observability::metrics::elapsed_us(started),
                            Some("internal_error"),
                            self.slow_command_threshold_us(),
                        );
                        log::error!("Failed to receive; err = {:?}", e);
                        response_bytes.extend(Frame::Error(e.to_string()).as_bytes());
                    }
                }
                self.session_manager.update_session(&self.session);
                if let Some(line) = self_monitor_line {
                    if !response_bytes.is_empty() {
                        let response = std::mem::take(&mut response_bytes);
                        self.metrics.add_output_bytes(response.len());
                        if !self.connection.write_bytes(response).await {
                            return;
                        }
                    }
                    self.session_manager
                        .broadcast_monitor_to(self.session.get_id(), line);
                    frames_since_flush = 0;
                    last_flush = std::time::Instant::now();
                }
            }
            if !response_bytes.is_empty() {
                self.metrics.add_output_bytes(response_bytes.len());
                if !self.connection.write_bytes(response_bytes).await {
                    return;
                }
            }
            if close_after_reply {
                return;
            }
        }
    }

    fn command_allowed_while_subscribed(command: &str) -> bool {
        matches!(
            command,
            "SUBSCRIBE"
                | "PSUBSCRIBE"
                | "SSUBSCRIBE"
                | "UNSUBSCRIBE"
                | "PUNSUBSCRIBE"
                | "SUNSUBSCRIBE"
                | "PING"
                | "QUIT"
        )
    }

    fn slow_command_threshold_us(&self) -> u64 {
        self.args.slow_command_threshold_ms.saturating_mul(1_000)
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        if self
            .session_manager
            .remove_session(self.session.get_id())
        {
            self.metrics.connection_closed();
        }
    }
}
