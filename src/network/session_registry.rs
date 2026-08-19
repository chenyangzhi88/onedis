impl SessionManager {
    pub fn create_session(&self, session: &Session) -> std::sync::Arc<SessionControl> {
        self.sessions
            .insert(session.get_id(), SessionSnapshot::from(session));
        let control = std::sync::Arc::new(SessionControl::new());
        self.controls.insert(session.get_id(), control.clone());
        control
    }

    pub fn update_session(&self, session: &Session) {
        if let Some(mut existing) = self.sessions.get_mut(&session.get_id()) {
            *existing = SessionSnapshot::from(session);
        }
    }

    pub fn remove_session(&self, session_id: usize) -> bool {
        self.remove_shared_writer_state(session_id);
        self.controls.remove(&session_id);
        self.sessions.remove(&session_id).is_some()
    }

    fn remove_shared_writer_state(&self, session_id: usize) {
        self.unsubscribe_all(session_id);
        self.monitors.remove(&session_id);
    }

    pub fn reset_connection_state(&self, session_id: usize) {
        self.remove_shared_writer_state(session_id);
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
        self.client_list_filtered(&ClientListFilter::default())
    }

    pub fn client_list_filtered(&self, filter: &ClientListFilter) -> String {
        self.try_client_list_filtered(filter, usize::MAX)
            .unwrap_or_default()
    }

    pub fn try_client_list_filtered(
        &self,
        filter: &ClientListFilter,
        max_bytes: usize,
    ) -> Result<String, &'static str> {
        let mut sessions = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|session| self.client_matches_list_filter(session, filter))
            .collect::<Vec<_>>();
        sessions.sort_unstable_by_key(|session| session.id);

        let mut out = String::new();
        for session in sessions {
            let remaining = max_bytes
                .checked_sub(out.len())
                .ok_or("ERR CLIENT LIST response exceeds configured limit")?;
            out.push_str(&self.try_format_client(&session, remaining)?);
        }
        Ok(out)
    }

    fn client_matches_list_filter(
        &self,
        session: &SessionSnapshot,
        filter: &ClientListFilter,
    ) -> bool {
        filter
            .ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&session.id))
            && filter
                .client_type
                .is_none_or(|client_type| self.client_has_type(session, client_type))
    }

    fn client_has_type(&self, session: &SessionSnapshot, client_type: ClientTypeFilter) -> bool {
        match client_type {
            ClientTypeFilter::Normal => {
                let (sub, psub, ssub) = self.subscription_counts(session.id);
                sub + psub + ssub == 0
            }
            ClientTypeFilter::Pubsub => {
                let (sub, psub, ssub) = self.subscription_counts(session.id);
                sub + psub + ssub > 0
            }
            ClientTypeFilter::Master | ClientTypeFilter::Replica => false,
        }
    }

    pub fn kill_clients(&self, current_id: usize, filter: &ClientKillFilter) -> usize {
        let matching_ids = self
            .sessions
            .iter()
            .filter_map(|entry| {
                let session = entry.value();
                if (filter.skip_current && session.id == current_id)
                    || filter.id.is_some_and(|id| id != session.id)
                    || filter
                        .client_type
                        .is_some_and(|kind| !self.client_has_type(session, kind))
                    || filter
                        .user
                        .as_ref()
                        .is_some_and(|user| user != &session.user)
                    || filter
                        .addr
                        .as_ref()
                        .is_some_and(|addr| addr != &session.peer_addr)
                    || filter
                        .local_addr
                        .as_ref()
                        .is_some_and(|addr| addr != &session.local_addr)
                    || filter
                        .min_age_secs
                        .is_some_and(|age| session.connected_at.elapsed().as_secs() < age)
                {
                    None
                } else {
                    Some(session.id)
                }
            })
            .collect::<Vec<_>>();

        matching_ids
            .into_iter()
            .filter(|session_id| {
                self.controls
                    .get(session_id)
                    .is_some_and(|control| control.request_kill())
            })
            .count()
    }

    pub fn unblock_client(&self, session_id: usize, mode: ClientUnblockMode) -> bool {
        self.controls
            .get(&session_id)
            .is_some_and(|control| control.request_unblock(mode))
    }

    /// Wake blocking clients and request every connection to finish its current command and exit.
    /// Returns the number of live sessions that received the shutdown request.
    pub fn request_shutdown_all(&self) -> usize {
        let controls = self
            .controls
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for control in &controls {
            let _ = control.request_unblock(ClientUnblockMode::Error);
            control.request_kill();
        }
        controls.len()
    }

    pub fn client_info(&self, session_id: usize) -> Option<String> {
        self.sessions
            .get(&session_id)
            .map(|session| self.format_client(session.value()))
    }

    pub fn try_client_info(
        &self,
        session_id: usize,
        max_bytes: usize,
    ) -> Result<Option<String>, &'static str> {
        self.sessions
            .get(&session_id)
            .map(|session| self.try_format_client(session.value(), max_bytes))
            .transpose()
    }

    fn format_client(&self, session: &SessionSnapshot) -> String {
        self.try_format_client(session, usize::MAX)
            .unwrap_or_default()
    }

    fn try_format_client(
        &self,
        session: &SessionSnapshot,
        max_bytes: usize,
    ) -> Result<String, &'static str> {
        let variable_bytes = [
            session.name.as_deref().unwrap_or(""),
            session.last_cmd.as_deref().unwrap_or("unknown"),
            &session.user,
            session.library_name.as_deref().unwrap_or(""),
            session.library_version.as_deref().unwrap_or(""),
        ]
        .into_iter()
        .try_fold(0usize, |total, value| {
            total.checked_add(value.len().checked_mul(3)?)
        })
        .ok_or("ERR CLIENT response exceeds configured limit")?;
        if variable_bytes.saturating_add(512) > max_bytes {
            return Err("ERR CLIENT response exceeds configured limit");
        }

        let name = client_list_escape(session.name.as_deref().unwrap_or(""));
        let cmd = client_list_escape(session.last_cmd.as_deref().unwrap_or("unknown"));
        let user = client_list_escape(&session.user);
        let (sub, psub, ssub) = self.subscription_counts(session.id);
        let mut flags = String::new();
        if self.is_monitoring(session.id) {
            flags.push('O');
        }
        if sub + psub + ssub > 0 {
            flags.push('P');
        }
        if session.in_transaction {
            flags.push('x');
        }
        if session.no_evict {
            flags.push('e');
        }
        if session.no_touch {
            flags.push('T');
        }
        if flags.is_empty() {
            flags.push('N');
        }
        let library_name = client_list_escape(session.library_name.as_deref().unwrap_or(""));
        let library_version = client_list_escape(session.library_version.as_deref().unwrap_or(""));
        let multi = if session.in_transaction {
            session.transaction_commands.to_string()
        } else {
            "-1".to_string()
        };
        Ok(format!(
            "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem={} rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} redir=-1 resp={} lib-name={} lib-ver={}\r\n",
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
            match session.resp_version {
                crate::frame::RespVersion::Resp2 => 2,
                crate::frame::RespVersion::Resp3 => 3,
            },
            library_name,
            library_version,
        ))
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
