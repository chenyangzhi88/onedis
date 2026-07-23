impl Handler {
    async fn apply_blocking_list_command(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        let timeout_secs = Self::blocking_list_timeout_secs(&command);
        let deadline = if timeout_secs > 0.0 {
            Some(
                Instant::now()
                    .checked_add(Duration::from_micros(
                        (timeout_secs * 1_000_000.0).ceil() as u64,
                    ))
                    .ok_or_else(|| Error::msg("ERR timeout is out of range"))?,
            )
        } else {
            None
        };

        loop {
            let notified = self.db_manager.list_notify().notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
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
                    tokio::select! {
                        result = tokio::time::timeout_at(deadline, notified.as_mut()) => {
                            if result.is_err() {
                                return Ok(Frame::Null.as_bytes());
                            }
                        }
                        result = self.connection.wait_read_closed() => {
                            result?;
                            return Err(Error::msg("Connection closed by peer"));
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
}
