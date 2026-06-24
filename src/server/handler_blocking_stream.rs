impl Handler {
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
}
