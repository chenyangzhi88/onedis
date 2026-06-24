impl Handler {
    async fn apply_blocking_zset_command(&self, command: Command) -> Result<Vec<u8>, Error> {
        let timeout_secs = Self::blocking_zset_timeout_secs(&command);
        let deadline = (timeout_secs > 0.0).then(|| {
            Instant::now() + Duration::from_micros((timeout_secs * 1_000_000.0).ceil() as u64)
        });
        loop {
            let notified = self.db_manager.zset_notify().notified();
            if let Some(frame) = self.try_blocking_zset_command_once(&command).await? {
                if !matches!(frame, Frame::Null | Frame::Error(_)) {
                    self.db_manager.notify_zset_waiters();
                }
                return Ok(frame.as_bytes());
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
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

    async fn try_blocking_zset_command_once(
        &self,
        command: &Command,
    ) -> Result<Option<Frame>, Error> {
        let db = self.session.get_db().clone();
        let txn_db = db.transactional_view()?;
        let frame = match command {
            Command::Bzpopmin(command) => {
                match txn_db
                    .zset_multi_pop_async(&command.keys, command.min, 1)
                    .await?
                {
                    Some((key, mut entries)) => {
                        let Some((member, score)) = entries.pop() else {
                            return Ok(None);
                        };
                        Some(Frame::Array(vec![
                            Frame::bulk_string(key),
                            Frame::bulk_string(member),
                            Frame::bulk_string(score.to_string()),
                        ]))
                    }
                    None => None,
                }
            }
            Command::Bzpopmax(command) => {
                let inner = &command.inner;
                match txn_db
                    .zset_multi_pop_async(&inner.keys, inner.min, 1)
                    .await?
                {
                    Some((key, mut entries)) => {
                        let Some((member, score)) = entries.pop() else {
                            return Ok(None);
                        };
                        Some(Frame::Array(vec![
                            Frame::bulk_string(key),
                            Frame::bulk_string(member),
                            Frame::bulk_string(score.to_string()),
                        ]))
                    }
                    None => None,
                }
            }
            Command::Bzmpop(command) => {
                match txn_db
                    .zset_multi_pop_async(&command.keys, command.min, command.count)
                    .await?
                {
                    Some((key, entries)) => Some(Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::Array(
                            entries
                                .into_iter()
                                .map(|(member, score)| {
                                    Frame::Array(vec![
                                        Frame::bulk_string(member),
                                        Frame::bulk_string(score.to_string()),
                                    ])
                                })
                                .collect(),
                        ),
                    ])),
                    None => None,
                }
            }
            _ => unreachable!("non blocking-zset command routed to blocking zset handler"),
        };
        txn_db.commit_transaction_async().await?;
        Ok(frame)
    }

    fn blocking_zset_timeout_secs(command: &Command) -> f64 {
        match command {
            Command::Bzpopmin(command) => command.timeout_secs,
            Command::Bzpopmax(command) => command.inner.timeout_secs,
            Command::Bzmpop(command) => command.timeout_secs,
            _ => 0.0,
        }
    }

    fn is_blocking_zset_command(command: &Command) -> bool {
        matches!(
            command,
            Command::Bzpopmin(_) | Command::Bzpopmax(_) | Command::Bzmpop(_)
        )
    }
}
