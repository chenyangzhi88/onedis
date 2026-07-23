impl SessionManager {
    pub fn create_session(&self, session: &Session) {
        self.sessions
            .insert(session.get_id(), SessionSnapshot::from(session));
    }

    pub fn update_session(&self, session: &Session) {
        if let Some(mut existing) = self.sessions.get_mut(&session.get_id()) {
            *existing = SessionSnapshot::from(session);
        }
    }

    pub fn remove_session(&self, session_id: usize) -> bool {
        self.remove_shared_writer_state(session_id);
        self.sessions.remove(&session_id).is_some()
    }

    fn remove_shared_writer_state(&self, session_id: usize) {
        self.unsubscribe_all(session_id);
        self.monitors.remove(&session_id);
    }

    pub fn get_connection_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_over_max_clients(&self, maxclients: usize) -> bool {
        if maxclients == 0 {
            return false;
        }
        self.get_connection_count() >= maxclients
    }

    pub fn client_list(&self) -> String {
        let mut sessions = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        sessions.sort_unstable_by_key(|session| session.id);

        let mut out = String::new();
        for session in sessions {
            out.push_str(&self.format_client(&session));
        }
        out
    }

    pub fn client_info(&self, session_id: usize) -> Option<String> {
        self.sessions
            .get(&session_id)
            .map(|session| self.format_client(session.value()))
    }

    fn format_client(&self, session: &SessionSnapshot) -> String {
        let name = client_list_escape(session.name.as_deref().unwrap_or(""));
        let cmd = client_list_escape(session.last_cmd.as_deref().unwrap_or("unknown"));
        let user = client_list_escape(&session.user);
        let (sub, psub, ssub) = self.subscription_counts(session.id);
        let flags = if self.is_monitoring(session.id) {
            "O"
        } else if sub + psub + ssub > 0 {
            "P"
        } else if session.in_transaction {
            "x"
        } else {
            "N"
        };
        let multi = if session.in_transaction {
            session.transaction_commands.to_string()
        } else {
            "-1".to_string()
        };
        format!(
            "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem={} rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} resp=2\r\n",
            session.id,
            session.peer_addr,
            session.local_addr,
            name,
            session.connected_at.elapsed().as_secs(),
            session.last_interaction_at.elapsed().as_secs(),
            flags,
            session.current_db,
            sub,
            psub,
            ssub,
            multi,
            session.transaction_bytes,
            cmd,
            user,
        )
    }
}

fn client_list_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        if (b'!'..=b'~').contains(&byte) && byte != b'%' && byte != b'=' {
            escaped.push(char::from(byte));
        } else {
            use std::fmt::Write;
            let _ = write!(escaped, "%{byte:02X}");
        }
    }
    escaped
}
