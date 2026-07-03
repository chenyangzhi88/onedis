impl Handler {
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

            let frame = match crate::command_dispatch::handle_command(txn_db, command) {
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

            let frame = match crate::command_dispatch::handle_command_async(txn_db, command).await {
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
}
