impl SessionManager {
    pub fn register_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.subscriptions
            .entry(session_id)
            .or_default()
            .channels
            .insert(channel.to_string());
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_pattern(&self, pattern: &str, session_id: usize, writer: SharedWriter) {
        self.subscriptions
            .entry(session_id)
            .or_default()
            .patterns
            .insert(pattern.to_string());
        self.patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_shard_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.subscriptions
            .entry(session_id)
            .or_default()
            .shard_channels
            .insert(channel.to_string());
        self.shard_channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn unregister_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.channels.get(channel) {
            map.remove(&session_id);
        }
        self.channels
            .remove_if(channel, |_, subscribers| subscribers.is_empty());
        let remove_subscription_record =
            if let Some(mut subscriptions) = self.subscriptions.get_mut(&session_id) {
                subscriptions.channels.remove(channel);
                subscriptions.is_empty()
            } else {
                false
            };
        if remove_subscription_record {
            self.subscriptions
                .remove_if(&session_id, |_, subscriptions| subscriptions.is_empty());
        }
    }

    pub fn unregister_pattern(&self, pattern: &str, session_id: usize) {
        if let Some(map) = self.patterns.get(pattern) {
            map.remove(&session_id);
        }
        self.patterns
            .remove_if(pattern, |_, subscribers| subscribers.is_empty());
        let remove_subscription_record =
            if let Some(mut subscriptions) = self.subscriptions.get_mut(&session_id) {
                subscriptions.patterns.remove(pattern);
                subscriptions.is_empty()
            } else {
                false
            };
        if remove_subscription_record {
            self.subscriptions
                .remove_if(&session_id, |_, subscriptions| subscriptions.is_empty());
        }
    }

    pub fn unregister_shard_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.shard_channels.get(channel) {
            map.remove(&session_id);
        }
        self.shard_channels
            .remove_if(channel, |_, subscribers| subscribers.is_empty());
        let remove_subscription_record =
            if let Some(mut subscriptions) = self.subscriptions.get_mut(&session_id) {
                subscriptions.shard_channels.remove(channel);
                subscriptions.is_empty()
            } else {
                false
            };
        if remove_subscription_record {
            self.subscriptions
                .remove_if(&session_id, |_, subscriptions| subscriptions.is_empty());
        }
    }

    pub fn unsubscribe_all(&self, session_id: usize) {
        let Some((_, subscriptions)) = self.subscriptions.remove(&session_id) else {
            return;
        };
        for channel in subscriptions.channels {
            if let Some(subscribers) = self.channels.get(&channel) {
                subscribers.remove(&session_id);
            }
            self.channels
                .remove_if(&channel, |_, subscribers| subscribers.is_empty());
        }
        for pattern in subscriptions.patterns {
            if let Some(subscribers) = self.patterns.get(&pattern) {
                subscribers.remove(&session_id);
            }
            self.patterns
                .remove_if(&pattern, |_, subscribers| subscribers.is_empty());
        }
        for channel in subscriptions.shard_channels {
            if let Some(subscribers) = self.shard_channels.get(&channel) {
                subscribers.remove(&session_id);
            }
            self.shard_channels
                .remove_if(&channel, |_, subscribers| subscribers.is_empty());
        }
    }

    pub fn subscription_count(&self, session_id: usize) -> usize {
        self.subscriptions
            .get(&session_id)
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0)
    }

    pub fn additional_subscription_count(
        &self,
        session_id: usize,
        names: &[String],
        kind: SubscriptionKind,
    ) -> usize {
        let requested = names.iter().map(String::as_str).collect::<HashSet<_>>();
        let subscriptions = self.subscriptions.get(&session_id);
        requested
            .into_iter()
            .filter(|name| {
                let Some(subscriptions) = subscriptions.as_deref() else {
                    return true;
                };
                match kind {
                    SubscriptionKind::Channel => !subscriptions.channels.contains(*name),
                    SubscriptionKind::Pattern => !subscriptions.patterns.contains(*name),
                    SubscriptionKind::ShardChannel => {
                        !subscriptions.shard_channels.contains(*name)
                    }
                }
            })
            .count()
    }

    fn subscription_counts(&self, session_id: usize) -> (usize, usize, usize) {
        self.subscriptions
            .get(&session_id)
            .map(|subscriptions| {
                (
                    subscriptions.channels.len(),
                    subscriptions.patterns.len(),
                    subscriptions.shard_channels.len(),
                )
            })
            .unwrap_or((0, 0, 0))
    }

    pub fn subscription_ack_count(&self, session_id: usize, kind: SubscriptionKind) -> usize {
        let (channels, patterns, shard_channels) = self.subscription_counts(session_id);
        match kind {
            SubscriptionKind::Channel | SubscriptionKind::Pattern => channels + patterns,
            SubscriptionKind::ShardChannel => shard_channels,
        }
    }

    pub fn channel_subscriptions(&self, session_id: usize) -> Vec<String> {
        let mut channels: Vec<String> = self
            .subscriptions
            .get(&session_id)
            .map(|subscriptions| subscriptions.channels.iter().cloned().collect())
            .unwrap_or_default();
        channels.sort_unstable();
        channels
    }

    pub fn pattern_subscriptions(&self, session_id: usize) -> Vec<String> {
        let mut patterns: Vec<String> = self
            .subscriptions
            .get(&session_id)
            .map(|subscriptions| subscriptions.patterns.iter().cloned().collect())
            .unwrap_or_default();
        patterns.sort_unstable();
        patterns
    }

    pub fn shard_subscriptions(&self, session_id: usize) -> Vec<String> {
        let mut channels: Vec<String> = self
            .subscriptions
            .get(&session_id)
            .map(|subscriptions| subscriptions.shard_channels.iter().cloned().collect())
            .unwrap_or_default();
        channels.sort_unstable();
        channels
    }

    pub fn publish(&self, channel: &str, message: &str, shard: bool) -> usize {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        let mut writers = Vec::new();
        if let Some(map) = source.get(channel) {
            writers.extend(
                map.iter()
                    .map(|entry| (*entry.key(), entry.value().clone())),
            );
        }
        let mut pattern_writers = Vec::new();
        if !shard {
            for entry in self.patterns.iter() {
                if glob_match(entry.key(), channel) {
                    pattern_writers.push((
                        entry.key().clone(),
                        entry
                            .value()
                            .iter()
                            .map(|writer| (*writer.key(), writer.value().clone()))
                            .collect::<Vec<_>>(),
                    ));
                }
            }
        }
        if writers.is_empty() && pattern_writers.is_empty() {
            return 0;
        }

        let message: std::sync::Arc<[u8]> = std::sync::Arc::from(message.as_bytes());
        let mut direct_count = 0usize;
        let frame_name = if shard { "smessage" } else { "message" };
        let direct_message = pubsub_message_chunks(&[frame_name, channel], message.clone());
        for (session_id, writer) in writers {
            if writer.is_closed() {
                self.remove_shared_writer_state(session_id);
                continue;
            }
            if writer.try_write_chunks(direct_message.clone()) {
                direct_count += 1;
            } else {
                self.remove_shared_writer_state(session_id);
            }
        }

        let mut pattern_deliveries = 0usize;
        for (pattern, writers) in pattern_writers {
            let live_writers = writers
                .into_iter()
                .filter(|(session_id, writer)| {
                    if writer.is_closed() {
                        self.remove_shared_writer_state(*session_id);
                        false
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>();
            if live_writers.is_empty() {
                continue;
            }
            let pattern_message =
                pubsub_message_chunks(&["pmessage", &pattern, channel], message.clone());
            for (session_id, writer) in live_writers {
                if writer.try_write_chunks(pattern_message.clone()) {
                    pattern_deliveries += 1;
                } else {
                    self.remove_shared_writer_state(session_id);
                }
            }
        }
        direct_count + pattern_deliveries
    }

    pub fn channel_count(&self, channel: &str, shard: bool) -> usize {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        source.get(channel).map(|m| m.len()).unwrap_or(0)
    }

    pub fn pattern_count(&self) -> usize {
        self.patterns
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .count()
    }

    pub fn channel_names(&self, shard: bool) -> Vec<String> {
        self.channel_names_matching(shard, None)
    }

    pub fn channel_names_matching(&self, shard: bool, pattern: Option<&str>) -> Vec<String> {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        let parsed_pattern = pattern.map(|pattern| parse_glob(pattern.as_bytes()));
        let mut channels = source
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .filter(|entry| {
                parsed_pattern
                    .as_ref()
                    .is_none_or(|pattern| glob_match_tokens(pattern, entry.key().as_bytes()))
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        channels.sort_unstable();
        channels
    }
}

fn pubsub_message_chunks(
    leading_fields: &[&str],
    message: std::sync::Arc<[u8]>,
) -> Vec<std::sync::Arc<[u8]>> {
    let mut prefix = Vec::new();
    prefix.extend_from_slice(format!("*{}\r\n", leading_fields.len() + 1).as_bytes());
    for field in leading_fields {
        append_pubsub_bulk(&mut prefix, field.as_bytes());
    }
    prefix.extend_from_slice(format!("${}\r\n", message.len()).as_bytes());
    vec![
        prefix.into(),
        message,
        std::sync::Arc::from(&b"\r\n"[..]),
    ]
}

fn append_pubsub_bulk(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}
