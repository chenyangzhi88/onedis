impl Handler {
    async fn apply_blocking_stream_command(&mut self, command: Command) -> Result<Vec<u8>, Error> {
        let mut blocked = None;
        let block_ms = Self::blocking_stream_timeout_ms(&command).unwrap_or(0);
        let deadline = if block_ms > 0 {
            Some(
                Instant::now()
                    .checked_add(Duration::from_millis(block_ms))
                    .ok_or_else(|| Error::msg("ERR timeout is out of range"))?,
            )
        } else {
            None
        };
        self.metrics.stream_blocked_started();
        let _blocked = StreamBlockedGuard {
            metrics: self.metrics.clone(),
        };
        loop {
            let db = self.session.get_db().clone();
            let waiter = db.wait_for_key_mutations(&Self::blocking_stream_keys(&command));
            let notified = waiter.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let frame = self.try_stream_read_once(&command).await?;
            if !matches!(frame, Frame::Null) {
                return Ok(self.encode_frame(&frame));
            }
            blocked.get_or_insert_with(|| self.client_control.begin_blocking());
            match deadline {
                Some(deadline) => {
                    if Instant::now() >= deadline {
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

    async fn try_stream_read_once(&self, command: &Command) -> Result<Frame, Error> {
        let db = self.session.get_db().clone();
        match command {
            Command::Xread(command) => {
                let streams = db.stream_read_async(&command.streams, command.count).await?;
                if streams.is_empty() {
                    Ok(Frame::Null)
                } else {
                    crate::cmds::stream::stream_reads_frame(streams)
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
                    crate::cmds::stream::stream_reads_frame(streams)
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

    fn blocking_stream_keys(command: &Command) -> Vec<&str> {
        match command {
            Command::Xread(command) => command
                .streams
                .iter()
                .map(|(key, _)| key.as_str())
                .collect(),
            Command::Xreadgroup(command) => command
                .streams
                .iter()
                .map(|(key, _)| key.as_str())
                .collect(),
            _ => Vec::new(),
        }
    }

    fn is_blocking_stream_command(command: &Command) -> bool {
        command.spec().blocking_kind == crate::command::BlockingKind::Stream
    }
}

struct StreamBlockedGuard {
    metrics: Arc<OnedisMetrics>,
}

impl Drop for StreamBlockedGuard {
    fn drop(&mut self) {
        self.metrics.stream_blocked_finished();
    }
}
