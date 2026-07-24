impl Handler {
    async fn apply_command_response_bytes(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        self.session_manager
            .broadcast_monitor(
                self.session.get_id(),
                format_command_name_for_monitor_context(
                    command.effective_name(),
                    self.session.get_current_db(),
                    self.session.peer_addr(),
                ),
            );
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
                    async move {
                        crate::command_dispatch::handle_command_async(&db, command)
                            .await
                            .map(|f| f.as_bytes())
                    },
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
            crate::command_dispatch::handle_command_async(&db, command).await
        } else {
            crate::command_dispatch::handle_command_autocommit_async(&db, command).await
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
                    db.clear_async().await;
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
                lua.apply_authorized(&db, authorizer)
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
