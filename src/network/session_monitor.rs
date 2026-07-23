impl SessionManager {
    pub fn add_monitor(&self, session_id: usize, writer: SharedWriter) {
        self.monitors.insert(session_id, writer);
    }

    pub fn is_monitoring(&self, session_id: usize) -> bool {
        self.monitors.contains_key(&session_id)
    }

    pub fn broadcast_monitor(&self, source_session_id: usize, line: String) {
        let message: std::sync::Arc<[u8]> = Frame::SimpleString(line).as_bytes().into();
        let writers = self
            .monitors
            .iter()
            .filter(|entry| *entry.key() != source_session_id)
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect::<Vec<_>>();
        for (session_id, writer) in writers {
            if !writer.try_write_shared(message.clone()) {
                self.remove_shared_writer_state(session_id);
            }
        }
    }

    pub fn broadcast_monitor_to(&self, session_id: usize, line: String) {
        let Some(writer) = self
            .monitors
            .get(&session_id)
            .map(|writer| writer.clone())
        else {
            return;
        };
        let message: std::sync::Arc<[u8]> = Frame::SimpleString(line).as_bytes().into();
        if !writer.try_write_shared(message) {
            self.remove_shared_writer_state(session_id);
        }
    }
}
