impl Handler {
    const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 10_000;

    async fn try_apply_pubsub_or_monitor(
        &mut self,
        command: &Command,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Command::Unknown(unknown) = command else {
            return Ok(None);
        };
        let name = unknown.command_name().to_ascii_uppercase();
        let args = unknown.args();
        match name.as_str() {
            "MONITOR" => {
                let writer = self.connection.shared_writer();
                self.session_manager
                    .add_monitor(self.session.get_id(), writer);
                Ok(Some(Frame::Ok.as_bytes()))
            }
            "ACL" => Ok(Some(self.apply_acl(args).as_bytes())),
            "PUBLISH" | "SPUBLISH" => {
                if args.len() != 2 {
                    return Ok(Some(
                        Frame::Error(format!(
                            "ERR wrong number of arguments for '{}' command",
                            name.to_ascii_lowercase()
                        ))
                        .as_bytes(),
                    ));
                }
                let delivered = self
                    .session_manager
                    .publish(&args[0], &args[1], name == "SPUBLISH");
                Ok(Some(Frame::Integer(delivered as i64).as_bytes()))
            }
            "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                if args.is_empty() {
                    return Ok(Some(
                        Frame::Error(format!(
                            "ERR wrong number of arguments for '{}' command",
                            name.to_ascii_lowercase()
                        ))
                        .as_bytes(),
                    ));
                }
                let current_count = self
                    .session_manager
                    .subscription_count(self.session.get_id());
                let kind = match name.as_str() {
                    "SUBSCRIBE" => SubscriptionKind::Channel,
                    "PSUBSCRIBE" => SubscriptionKind::Pattern,
                    "SSUBSCRIBE" => SubscriptionKind::ShardChannel,
                    _ => unreachable!(),
                };
                let additional = self.session_manager.additional_subscription_count(
                    self.session.get_id(),
                    args,
                    kind,
                );
                if current_count.saturating_add(additional) > Self::MAX_SUBSCRIPTIONS_PER_CLIENT {
                    return Ok(Some(
                        Frame::Error("ERR maximum number of subscriptions reached".to_string())
                            .as_bytes(),
                    ));
                }
                let writer = self.connection.shared_writer();
                let mut frames = Vec::new();
                for channel in args {
                    match name.as_str() {
                        "SUBSCRIBE" => self.session_manager.register_channel(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        "PSUBSCRIBE" => self.session_manager.register_pattern(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        "SSUBSCRIBE" => self.session_manager.register_shard_channel(
                            channel,
                            self.session.get_id(),
                            writer.clone(),
                        ),
                        _ => {}
                    }
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::bulk_string(channel.clone()),
                            Frame::Integer(
                                self.session_manager
                                    .subscription_ack_count(self.session.get_id(), kind)
                                    as i64,
                            ),
                        ])
                        .as_bytes(),
                    );
                }
                Ok(Some(frames))
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" => {
                let channels = if args.is_empty() {
                    match name.as_str() {
                        "UNSUBSCRIBE" => self
                            .session_manager
                            .channel_subscriptions(self.session.get_id()),
                        "PUNSUBSCRIBE" => self
                            .session_manager
                            .pattern_subscriptions(self.session.get_id()),
                        "SUNSUBSCRIBE" => self
                            .session_manager
                            .shard_subscriptions(self.session.get_id()),
                        _ => Vec::new(),
                    }
                } else {
                    args.to_vec()
                };
                let mut frames = Vec::new();
                let kind = match name.as_str() {
                    "UNSUBSCRIBE" => SubscriptionKind::Channel,
                    "PUNSUBSCRIBE" => SubscriptionKind::Pattern,
                    "SUNSUBSCRIBE" => SubscriptionKind::ShardChannel,
                    _ => unreachable!(),
                };
                for channel in channels {
                    match name.as_str() {
                        "UNSUBSCRIBE" => self
                            .session_manager
                            .unregister_channel(&channel, self.session.get_id()),
                        "PUNSUBSCRIBE" => self
                            .session_manager
                            .unregister_pattern(&channel, self.session.get_id()),
                        "SUNSUBSCRIBE" => self
                            .session_manager
                            .unregister_shard_channel(&channel, self.session.get_id()),
                        _ => {}
                    }
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::bulk_string(channel),
                            Frame::Integer(
                                self.session_manager
                                    .subscription_ack_count(self.session.get_id(), kind)
                                    as i64,
                            ),
                        ])
                        .as_bytes(),
                    );
                }
                if frames.is_empty() {
                    frames.extend(
                        Frame::Array(vec![
                            Frame::bulk_string(name.to_ascii_lowercase()),
                            Frame::Null,
                            Frame::Integer(
                                self.session_manager
                                    .subscription_ack_count(self.session.get_id(), kind)
                                    as i64,
                            ),
                        ])
                        .as_bytes(),
                    );
                }
                Ok(Some(frames))
            }
            "PUBSUB" => Ok(Some(self.apply_pubsub_introspection(args).as_bytes())),
            _ => Ok(None),
        }
    }

    fn apply_acl(&mut self, args: &[String]) -> Frame {
        match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
            Some("WHOAMI") => Frame::bulk_string(self.session.user().to_string()),
            Some("USERS") => Frame::Array(
                self.session_manager
                    .acl_users()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("LIST") => Frame::Array(
                self.session_manager
                    .acl_list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("SETUSER") if args.len() >= 2 => {
                match self.session_manager.acl_setuser(&args[1], &args[2..]) {
                    Ok(()) => Frame::Ok,
                    Err(err) => Frame::Error(err),
                }
            }
            Some("DELUSER") if args.len() >= 2 => {
                Frame::Integer(self.session_manager.acl_deluser(&args[1..]) as i64)
            }
            Some("CAT") => Frame::Array(Vec::new()),
            Some("HELP") => Frame::Array(vec![Frame::bulk_string("ACL SETUSER <user> [rule ...]")]),
            _ => Frame::Error("ERR syntax error".to_string()),
        }
    }

    fn apply_pubsub_introspection(&self, args: &[String]) -> Frame {
        match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
            Some("NUMSUB") => {
                let mut frames = Vec::new();
                for channel in args.iter().skip(1) {
                    frames.push(Frame::bulk_string(channel.clone()));
                    frames.push(Frame::Integer(
                        self.session_manager.channel_count(channel, false) as i64,
                    ));
                }
                Frame::Array(frames)
            }
            Some("SHARDNUMSUB") => {
                let mut frames = Vec::new();
                for channel in args.iter().skip(1) {
                    frames.push(Frame::bulk_string(channel.clone()));
                    frames.push(Frame::Integer(
                        self.session_manager.channel_count(channel, true) as i64,
                    ));
                }
                Frame::Array(frames)
            }
            Some("NUMPAT") if args.len() == 1 => {
                Frame::Integer(self.session_manager.pattern_count() as i64)
            }
            Some("CHANNELS") if args.len() <= 2 => Frame::Array(
                self.session_manager
                    .channel_names_matching(false, args.get(1).map(String::as_str))
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Some("SHARDCHANNELS") if args.len() <= 2 => Frame::Array(
                self.session_manager
                    .channel_names_matching(true, args.get(1).map(String::as_str))
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            _ => Frame::Error("ERR syntax error".to_string()),
        }
    }
}
