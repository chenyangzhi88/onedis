impl SessionManager {
    pub fn add_monitor(&self, session_id: usize, writer: SharedWriter) {
        self.monitors.insert(session_id, writer);
    }

    pub async fn broadcast_monitor(&self, source_session_id: usize, line: String) {
        let writers = self
            .monitors
            .iter()
            .filter(|entry| *entry.key() != source_session_id)
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for writer in writers {
            writer
                .write_bytes(Frame::SimpleString(line.clone()).as_bytes())
                .await;
        }
    }
}
