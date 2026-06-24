impl SessionManager {
    pub fn register_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_pattern(&self, pattern: &str, session_id: usize, writer: SharedWriter) {
        self.patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_shard_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.shard_channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn unregister_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.channels.get(channel) {
            map.remove(&session_id);
        }
    }

    pub fn unregister_pattern(&self, pattern: &str, session_id: usize) {
        if let Some(map) = self.patterns.get(pattern) {
            map.remove(&session_id);
        }
    }

    pub fn unregister_shard_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.shard_channels.get(channel) {
            map.remove(&session_id);
        }
    }

    pub fn unsubscribe_all(&self, session_id: usize) {
        for entry in self.channels.iter() {
            entry.value().remove(&session_id);
        }
        for entry in self.patterns.iter() {
            entry.value().remove(&session_id);
        }
        for entry in self.shard_channels.iter() {
            entry.value().remove(&session_id);
        }
    }

    pub async fn publish(&self, channel: &str, message: &str, shard: bool) -> usize {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        let mut writers = Vec::new();
        if let Some(map) = source.get(channel) {
            writers.extend(map.iter().map(|entry| entry.value().clone()));
        }
        let direct_count = writers.len();
        let frame_name = if shard { "smessage" } else { "message" };
        for writer in writers {
            writer
                .write_bytes(
                    Frame::Array(vec![
                        Frame::bulk_string(frame_name),
                        Frame::bulk_string(channel.to_string()),
                        Frame::bulk_string(message.to_string()),
                    ])
                    .as_bytes(),
                )
                .await;
        }

        let mut pattern_deliveries = 0usize;
        if !shard {
            let mut pattern_writers = Vec::new();
            for entry in self.patterns.iter() {
                if glob_match(entry.key(), channel) {
                    for writer in entry.value().iter() {
                        pattern_writers.push((entry.key().clone(), writer.value().clone()));
                    }
                }
            }
            pattern_deliveries = pattern_writers.len();
            for (pattern, writer) in pattern_writers {
                writer
                    .write_bytes(
                        Frame::Array(vec![
                            Frame::bulk_string("pmessage"),
                            Frame::bulk_string(pattern),
                            Frame::bulk_string(channel.to_string()),
                            Frame::bulk_string(message.to_string()),
                        ])
                        .as_bytes(),
                    )
                    .await;
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
        self.patterns.iter().map(|entry| entry.value().len()).sum()
    }

    pub fn channel_names(&self, shard: bool) -> Vec<String> {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        source
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .map(|entry| entry.key().clone())
            .collect()
    }
}
