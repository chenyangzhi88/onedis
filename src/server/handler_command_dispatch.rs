impl Handler {
    fn reset_connection_context(&mut self) -> Result<Frame, Error> {
        self.clear_transaction();
        self.session_manager
            .reset_connection_state(self.session.get_id());
        self.change_db(0)?;
        self.session.set_name(None);
        self.session.set_library_name(None);
        self.session.set_library_version(None);
        self.session.set_no_evict(false);
        self.session.set_no_touch(false);
        self.session.set_resp_version(RespVersion::Resp2);
        self.session.set_user("default".to_string());
        self.session.set_certification(
            self.session_manager.acl_user_is_nopass("default"),
        );
        self.session_manager.update_session(&self.session);
        Ok(Frame::SimpleString("RESET".to_string()))
    }

    async fn apply_command_response_bytes(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        if let Command::Unknown(unknown) = &command
            && unknown.command_name().eq_ignore_ascii_case("HELLO")
        {
            let args = unknown.args().to_vec();
            let frame = self.apply_hello(&args)?;
            return Ok(self.encode_frame(&frame));
        }
        if self.session_manager.has_monitors() {
            self.session_manager.broadcast_monitor(
                self.session.get_id(),
                format_command_name_for_monitor_context(
                    command.effective_name(),
                    self.session.get_current_db(),
                    self.session.peer_addr(),
                ),
            );
        }
        if let Some(bytes) = self.try_apply_pubsub_or_monitor(&command).await? {
            return Ok(bytes);
        }
        let command = match command {
            Command::Echo(echo) if self.session.resp_version() == RespVersion::Resp2 => {
                return Ok(echo.into_response_bytes());
            }
            Command::Echo(echo) => return Ok(self.encode_frame(&echo.apply()?)),
            command => command,
        };
        if let Command::Exec(_) = command {
            let frame = self.execute_transaction_async().await?;
            return Ok(self.encode_frame(&frame));
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
            let protocol = self.session.resp_version();
            return self
                .command_executor
                .execute(async move {
                    wasm.apply(&registry, db)
                        .await
                        .as_bytes_for_protocol(protocol)
                })
                .await;
        }
        if matches!(command, Command::Lrange(_)) {
            let db = self.session.get_db().clone();
            let protocol = self.session.resp_version();
            return self
                .command_executor
                .execute(
                    async move {
                        crate::command_dispatch::handle_command_async(&db, command)
                            .await
                            .map(|f| f.as_bytes_for_protocol(protocol))
                    },
                )
                .await?;
        }
        if !Self::can_apply_on_worker(&command) {
            let frame = self.apply_command(command).await?;
            return Ok(self.encode_frame(&frame));
        }
        if let Command::Move(r#move) = &command
            && self.args.databases <= r#move.get_db_index()
        {
            return Ok(self.encode_frame(&Frame::Error("ERR DB index is out of range".to_string())));
        }
        if let Command::Copy(copy) = &command
            && copy
                .db_index()
                .is_some_and(|db_index| self.args.databases <= db_index)
        {
            return Ok(self.encode_frame(&Frame::Error("ERR DB index is out of range".to_string())));
        }

        let db = self.session.get_db().clone();
        let direct = Self::can_apply_direct(&command);
        let frame = if direct {
            crate::command_dispatch::handle_command_async(&db, command).await
        } else {
            crate::command_dispatch::handle_command_autocommit_async(&db, command).await
        }?;
        Ok(self.encode_frame(&frame))
    }

    fn apply_hello(&mut self, args: &[String]) -> Result<Frame, Error> {
        let mut index = 0usize;
        let requested = if let Some(value) = args.first() {
            match value.as_str() {
                "2" => {
                    index = 1;
                    Some(RespVersion::Resp2)
                }
                "3" => {
                    index = 1;
                    Some(RespVersion::Resp3)
                }
                _ if value.eq_ignore_ascii_case("AUTH") || value.eq_ignore_ascii_case("SETNAME") => None,
                _ => return Err(Error::msg("NOPROTO unsupported protocol version")),
            }
        } else {
            None
        };

        let mut auth = None;
        let mut setname = None;
        while index < args.len() {
            match args[index].to_ascii_uppercase().as_str() {
                "AUTH" => {
                    if index + 2 >= args.len() || auth.is_some() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    auth = Some((args[index + 1].as_str(), args[index + 2].as_str()));
                    index += 3;
                }
                "SETNAME" => {
                    if index + 1 >= args.len() || setname.is_some() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    setname = Some(args[index + 1].as_str());
                    index += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        if let Some((username, password)) = auth {
            self.login(Some(username), password)?;
        } else if !self.session.get_certification() {
            return Err(Error::msg("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used"));
        }
        if let Some(name) = setname {
            if !name.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
                return Err(Error::msg("ERR Client names cannot contain spaces, newlines or special characters."));
            }
            self.set_client_name((!name.is_empty()).then(|| name.to_string()));
        }
        if let Some(protocol) = requested {
            self.session.set_resp_version(protocol);
        }
        self.session_manager.update_session(&self.session);

        let fields = vec![
            ("server", Frame::bulk_string("onedis")),
            ("version", Frame::bulk_string(env!("CARGO_PKG_VERSION"))),
            (
                "proto",
                Frame::Integer(match self.session.resp_version() {
                    RespVersion::Resp2 => 2,
                    RespVersion::Resp3 => 3,
                }),
            ),
            ("id", Frame::Integer(self.session.get_id() as i64)),
            ("mode", Frame::bulk_string("standalone")),
            ("role", Frame::bulk_string("standalone")),
            ("modules", Frame::Array(Vec::new())),
        ];
        Ok(match self.session.resp_version() {
            RespVersion::Resp3 => Frame::Map(
                fields
                    .into_iter()
                    .map(|(key, value)| (Frame::bulk_string(key), value))
                    .collect(),
            ),
            RespVersion::Resp2 => Frame::Array(
                fields
                    .into_iter()
                    .flat_map(|(key, value)| [Frame::bulk_string(key), value])
                    .collect(),
            ),
        })
    }

    /// 执行命令（直接调用 Db，无 channel 开销）
    async fn apply_command(&mut self, command: Command) -> Result<Frame, Error> {
        match command {
            Command::Auth(auth) => auth.apply(self),
            Command::Client(client) => client.apply_with_handler(self),
            Command::Config(config) => config.apply(self.args.as_ref()),
            Command::Save(save) => save.apply_sync(&self.db_manager),
            Command::Bgsave(bgsave) => bgsave.apply_sync(&self.db_manager),
            Command::Flushall(_) => {
                for db in self.db_manager.get_all_dbs() {
                    db.clear_async().await?;
                }
                Ok(Frame::Ok)
            }
            Command::Move(r#move) => {
                if self.args.databases <= r#move.get_db_index() {
                    return Ok(Frame::Error("ERR DB index is out of range".to_string()));
                }
                let db = self.session.get_db().clone();
                crate::command_dispatch::handle_command_autocommit_async(
                    &db,
                    Command::Move(r#move),
                )
                .await
            }
            Command::Copy(copy) => {
                let db = self.session.get_db().clone();
                crate::command_dispatch::handle_command_autocommit_async(
                    &db,
                    Command::Copy(copy),
                )
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
            Command::Lua(lua) => {
                let db = self.session.get_db().clone();
                let session_manager = self.session_manager.clone();
                let user = self.session.user().to_string();
                let authorizer: crate::lua::LuaCommandAuthorizer =
                    Arc::new(move |command| session_manager.acl_allows(&user, command));
                lua.apply_authorized_async(&db, authorizer).await
            }
            _ => {
                let db = self.session.get_db().clone();
                if Self::can_apply_direct(&command) {
                    crate::command_dispatch::handle_command_async(&db, command).await
                } else {
                    crate::command_dispatch::handle_command_autocommit_async(&db, command).await
                }
            }
        }
    }
}
