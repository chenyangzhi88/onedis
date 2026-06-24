impl Handler {
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
        let frame = Self::execute_transaction_commands(txn_db, parsed_commands, self.args.databases);
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
}
