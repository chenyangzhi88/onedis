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
}
