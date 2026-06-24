impl Handler {
    fn try_handle_ping_fast_batch(&self, bytes: &[u8]) -> Option<Vec<u8>> {
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
            return Some(out);
        }

        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands
            .iter()
            .all(|args| args.len() == 1 && args[0].eq_ignore_ascii_case(b"PING"))
        {
            let mut out = Vec::with_capacity(commands.len() * 7);
            for _ in commands {
                out.extend_from_slice(b"+PONG\r\n");
            }
            return Some(out);
        }

        None
    }

    async fn try_handle_borrowed_fast_batch(&mut self, bytes: &[u8]) -> Option<Vec<u8>> {
        if self.session.is_in_transaction()
            || (self.args.requirepass.is_some() && !self.session.get_certification())
        {
            return None;
        }
        if let Some(commands) = parse_borrowed_plain_set_commands(bytes) {
            return Some(self.handle_borrowed_set_byte_commands(commands).await);
        }
        if let Some(commands) = parse_borrowed_plain_hset_commands(bytes) {
            return Some(self.handle_borrowed_hset_commands(commands).await);
        }
        let commands = parse_borrowed_resp_commands(bytes)?;
        if commands.iter().all(|args| borrowed_read_supported(args)) {
            return Some(self.handle_borrowed_read_commands(commands));
        }
        if commands
            .iter()
            .all(|args| borrowed_plain_set_supported(args))
        {
            return Some(self.handle_borrowed_set_commands(commands).await);
        }
        if commands
            .iter()
            .all(|args| borrowed_list_push_supported(args))
        {
            let response = self.handle_borrowed_list_push_commands(commands).await;
            self.db_manager.notify_list_waiters();
            return Some(response);
        }
        if commands.iter().all(|args| borrowed_lrange_supported(args)) {
            return Some(self.handle_borrowed_lrange_commands(commands).await);
        }
        None
    }
}
