impl Handler {
    async fn apply_blocking_list_command(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        let mut blocked = None;
        let timeout_secs = Self::blocking_list_timeout_secs(&command);
        let deadline = if timeout_secs > 0.0 {
            Some(
                Instant::now()
                    .checked_add(Duration::from_micros(
                        (timeout_secs * 1_000_000.0).ceil() as u64
                    ))
                    .ok_or_else(|| Error::msg("ERR timeout is out of range"))?,
            )
        } else {
            None
        };

        loop {
            let db = self.session.get_db().clone();
            let waiter = db.wait_for_key_mutations(&Self::blocking_list_keys(&command));
            let notified = waiter.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(frame) = self.try_blocking_list_command_once(&command).await? {
                return Ok(self.encode_frame(&frame));
            }
            blocked.get_or_insert_with(|| self.client_control.begin_blocking());

            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(self.encode_frame(&Frame::Null));
                    }
                    tokio::select! {
                        result = tokio::time::timeout_at(deadline, notified.as_mut()) => {
                            if result.is_err() {
                                return Ok(self.encode_frame(&Frame::Null));
                            }
                        }
                        result = self.connection.wait_read_closed() => {
                            result?;
                            return Err(Error::msg("Connection closed by peer"));
                        }
                        _ = self.client_control.wait_killed() => return Ok(Vec::new()),
                        mode = self.client_control.wait_unblocked() => {
                            return Ok(Self::client_unblock_response(mode));
                        }
                    }
                }
                None => {
                    tokio::select! {
                        _ = notified.as_mut() => {}
                        result = self.connection.wait_read_closed() => {
                            result?;
                            return Err(Error::msg("Connection closed by peer"));
                        }
                        _ = self.client_control.wait_killed() => return Ok(Vec::new()),
                        mode = self.client_control.wait_unblocked() => {
                            return Ok(Self::client_unblock_response(mode));
                        }
                    }
                }
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
            Command::Blpop(blpop) => txn_db
                .list_multi_pop_async(&blpop.keys, true, 1)
                .await?
                .and_then(|(key, mut values)| values.pop().map(|value| (key, value)))
                .map(|(key, value)| {
                    Frame::Array(vec![Frame::bulk_string(key), Frame::bulk_string(value)])
                }),
            Command::Brpop(brpop) => txn_db
                .list_multi_pop_async(&brpop.inner.keys, false, 1)
                .await?
                .and_then(|(key, mut values)| values.pop().map(|value| (key, value)))
                .map(|(key, value)| {
                    Frame::Array(vec![Frame::bulk_string(key), Frame::bulk_string(value)])
                }),
            Command::Brpoplpush(command) => txn_db
                .list_move_async(&command.source, &command.destination, false, true)
                .await?
                .map(Frame::bulk_string),
            Command::Blmove(command) => txn_db
                .list_move_async(
                    &command.source,
                    &command.destination,
                    command.source_side.is_left(),
                    command.destination_side.is_left(),
                )
                .await?
                .map(Frame::bulk_string),
            Command::Blmpop(command) => txn_db
                .list_multi_pop_async(&command.keys, command.left, command.count)
                .await?
                .map(|(key, values)| {
                    Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::Array(values.into_iter().map(Frame::bulk_string).collect()),
                    ])
                }),
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

    fn blocking_list_keys(command: &Command) -> Vec<&str> {
        match command {
            Command::Blpop(command) => command.keys.iter().map(String::as_str).collect(),
            Command::Brpop(command) => command.inner.keys.iter().map(String::as_str).collect(),
            Command::Brpoplpush(command) => {
                vec![command.source.as_str(), command.destination.as_str()]
            }
            Command::Blmove(command) => {
                vec![command.source.as_str(), command.destination.as_str()]
            }
            Command::Blmpop(command) => command.keys.iter().map(String::as_str).collect(),
            _ => Vec::new(),
        }
    }

    fn is_blocking_list_command(command: &Command) -> bool {
        command.spec().blocking_kind == crate::command::BlockingKind::List
    }
}
