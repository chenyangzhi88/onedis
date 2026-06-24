impl SessionManager {
    pub fn create_session(&self, session: Session) {
        self.sessions.insert(session.get_id(), session);
    }

    pub fn update_session(&self, session: Session) {
        self.sessions.insert(session.get_id(), session);
    }

    pub fn remove_session(&self, session_id: usize) -> bool {
        self.unsubscribe_all(session_id);
        self.monitors.remove(&session_id);
        self.sessions.remove(&session_id).is_some()
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
        let mut out = String::new();
        for session in self.sessions.iter() {
            let session = session.value();
            let name = session.name().unwrap_or("");
            let cmd = session.last_cmd().unwrap_or("unknown");
            out.push_str(&format!(
                "id={} addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=-1 name={} age={} idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user=default resp=2\r\n",
                session.get_id(),
                name,
                session.age_secs(),
                session.get_current_db(),
                cmd,
            ));
        }
        out
    }
}
