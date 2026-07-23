impl Handler {
    fn try_handle_ping_fast_batch(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let started = std::time::Instant::now();
        if self.session.is_in_transaction()
            || (self.args.requirepass.is_some() && !self.session.get_certification())
        {
            return None;
        }

        if bytes.chunks_exact(6).remainder().is_empty()
            && bytes
                .chunks_exact(6)
                .all(|chunk| chunk.eq_ignore_ascii_case(b"PING\r\n"))
        {
            let count = bytes.len() / 6;
            let mut out = Vec::with_capacity(count * 7);
            for _ in 0..count {
                out.extend_from_slice(b"+PONG\r\n");
            }
            self.metrics.record_command_batch(
                "PING",
                count,
                crate::observability::metrics::elapsed_us(started),
                None,
                self.slow_command_threshold_us(),
            );
            return Some(out);
        }

        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands
            .iter()
            .all(|args| args.len() == 1 && args[0].eq_ignore_ascii_case(b"PING"))
        {
            let count = commands.len();
            let mut out = Vec::with_capacity(commands.len() * 7);
            for _ in commands {
                out.extend_from_slice(b"+PONG\r\n");
            }
            self.metrics.record_command_batch(
                "PING",
                count,
                crate::observability::metrics::elapsed_us(started),
                None,
                self.slow_command_threshold_us(),
            );
            return Some(out);
        }

        None
    }

    async fn try_handle_borrowed_fast_batch(&mut self, bytes: &[u8]) -> Option<Vec<u8>> {
        let started = std::time::Instant::now();
        if self.session.is_in_transaction()
            || (self.args.requirepass.is_some() && !self.session.get_certification())
        {
            return None;
        }
        if let Some(commands) = parse_borrowed_plain_set_commands(bytes) {
            let count = commands.len();
            let response = self.handle_borrowed_set_byte_commands(commands).await;
            self.record_fast_command_batch("SET", count, started, &response);
            return Some(response);
        }
        if let Some(commands) = parse_borrowed_plain_hset_commands(bytes) {
            let count = commands.len();
            let response = self.handle_borrowed_hset_commands(commands).await;
            self.record_fast_command_batch("HSET", count, started, &response);
            return Some(response);
        }
        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands.iter().all(|args| borrowed_read_supported(args)) {
            let names = borrowed_fast_names(&commands);
            let response = self.handle_borrowed_read_commands(commands).await;
            self.record_fast_command_names(&names, started, &response);
            return Some(response);
        }
        if commands
            .iter()
            .all(|args| borrowed_plain_set_supported(args))
        {
            let count = commands.len();
            let response = self.handle_borrowed_set_commands(commands).await;
            self.record_fast_command_batch("SET", count, started, &response);
            return Some(response);
        }
        if commands
            .iter()
            .all(|args| borrowed_list_push_supported(args))
        {
            let names = borrowed_fast_names(&commands);
            let response = self.handle_borrowed_list_push_commands(commands).await;
            self.db_manager.notify_list_waiters();
            self.record_fast_command_names(&names, started, &response);
            return Some(response);
        }
        if commands.iter().all(|args| borrowed_lrange_supported(args)) {
            let count = commands.len();
            let response = self.handle_borrowed_lrange_commands(commands).await;
            self.record_fast_command_batch("LRANGE", count, started, &response);
            return Some(response);
        }
        None
    }

    fn record_fast_command_batch(
        &self,
        command: &'static str,
        count: usize,
        started: std::time::Instant,
        response: &[u8],
    ) {
        self.metrics.record_command_batch(
            command,
            count,
            crate::observability::metrics::elapsed_us(started),
            crate::observability::metrics::classify_error_response(response),
            self.slow_command_threshold_us(),
        );
        self.metrics.record_protocol_frames(count);
    }

    fn record_fast_command_names(
        &self,
        commands: &[&'static str],
        started: std::time::Instant,
        response: &[u8],
    ) {
        let elapsed = crate::observability::metrics::elapsed_us(started);
        let error_class = crate::observability::metrics::classify_error_response(response);
        for command in commands {
            self.metrics.record_command(
                command,
                elapsed / commands.len().max(1) as u64,
                error_class,
                self.slow_command_threshold_us(),
            );
        }
        self.metrics.record_protocol_frames(commands.len());
    }
}

fn borrowed_fast_names(commands: &[Vec<&[u8]>]) -> Vec<&'static str> {
    commands
        .iter()
        .filter_map(|args| {
            args.first()
                .and_then(|command| crate::observability::metrics::borrowed_fast_command_name(command))
        })
        .collect()
}
