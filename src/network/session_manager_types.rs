pub struct SessionManager {
    sessions: DashMap<usize, SessionSnapshot>,
    controls: DashMap<usize, std::sync::Arc<SessionControl>>,
    channels: DashMap<String, DashMap<usize, SharedWriter>>,
    patterns: DashMap<String, DashMap<usize, SharedWriter>>,
    shard_channels: DashMap<String, DashMap<usize, SharedWriter>>,
    subscriptions: DashMap<usize, SessionSubscriptions>,
    monitors: DashMap<usize, SharedWriter>,
    acl_users: DashMap<String, AclUser>,
}

#[derive(Clone)]
struct SessionSnapshot {
    id: usize,
    current_db: usize,
    in_transaction: bool,
    transaction_commands: usize,
    transaction_bytes: usize,
    name: Option<String>,
    library_name: Option<String>,
    library_version: Option<String>,
    no_evict: bool,
    no_touch: bool,
    connected_at: std::time::Instant,
    last_interaction_at: std::time::Instant,
    last_cmd: Option<String>,
    user: String,
    peer_addr: String,
    local_addr: String,
}

impl From<&Session> for SessionSnapshot {
    fn from(session: &Session) -> Self {
        Self {
            id: session.get_id(),
            current_db: session.get_current_db(),
            in_transaction: session.is_in_transaction(),
            transaction_commands: session.transaction_command_count(),
            transaction_bytes: session.transaction_bytes(),
            name: session.name().map(ToString::to_string),
            library_name: session.library_name().map(ToString::to_string),
            library_version: session.library_version().map(ToString::to_string),
            no_evict: session.no_evict(),
            no_touch: session.no_touch(),
            connected_at: session.connected_at(),
            last_interaction_at: session.last_interaction_at(),
            last_cmd: session.last_cmd().map(ToString::to_string),
            user: session.user().to_string(),
            peer_addr: session.peer_addr().to_string(),
            local_addr: session.local_addr().to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientTypeFilter {
    Normal,
    Master,
    Replica,
    Pubsub,
}

#[derive(Clone, Debug, Default)]
pub struct ClientListFilter {
    pub client_type: Option<ClientTypeFilter>,
    pub ids: Option<HashSet<usize>>,
}

#[derive(Clone, Debug)]
pub struct ClientKillFilter {
    pub id: Option<usize>,
    pub client_type: Option<ClientTypeFilter>,
    pub user: Option<String>,
    pub addr: Option<String>,
    pub local_addr: Option<String>,
    pub skip_current: bool,
    pub min_age_secs: Option<u64>,
}

impl Default for ClientKillFilter {
    fn default() -> Self {
        Self {
            id: None,
            client_type: None,
            user: None,
            addr: None,
            local_addr: None,
            skip_current: true,
            min_age_secs: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientUnblockMode {
    Timeout,
    Error,
}

#[derive(Default)]
struct SessionControlState {
    killed: bool,
    blocked: bool,
    unblock_mode: Option<ClientUnblockMode>,
}

pub struct SessionControl {
    state: std::sync::Mutex<SessionControlState>,
    killed: tokio::sync::Notify,
    unblocked: tokio::sync::Notify,
}

impl SessionControl {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(SessionControlState::default()),
            killed: tokio::sync::Notify::new(),
            unblocked: tokio::sync::Notify::new(),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SessionControlState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn request_kill(&self) -> bool {
        let mut state = self.lock_state();
        if state.killed {
            return false;
        }
        state.killed = true;
        drop(state);
        self.killed.notify_waiters();
        true
    }

    pub(crate) fn is_killed(&self) -> bool {
        self.lock_state().killed
    }

    pub(crate) async fn wait_killed(&self) {
        loop {
            let notified = self.killed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock_state().killed {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn begin_blocking(self: &std::sync::Arc<Self>) -> SessionBlockingGuard {
        let mut state = self.lock_state();
        state.blocked = true;
        state.unblock_mode = None;
        drop(state);
        SessionBlockingGuard {
            control: self.clone(),
        }
    }

    fn request_unblock(&self, mode: ClientUnblockMode) -> bool {
        let mut state = self.lock_state();
        if !state.blocked || state.unblock_mode.is_some() {
            return false;
        }
        state.unblock_mode = Some(mode);
        drop(state);
        self.unblocked.notify_waiters();
        true
    }

    pub(crate) async fn wait_unblocked(&self) -> ClientUnblockMode {
        loop {
            let notified = self.unblocked.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(mode) = self.lock_state().unblock_mode {
                return mode;
            }
            notified.await;
        }
    }
}

pub(crate) struct SessionBlockingGuard {
    control: std::sync::Arc<SessionControl>,
}

impl Drop for SessionBlockingGuard {
    fn drop(&mut self) {
        let mut state = self.control.lock_state();
        state.blocked = false;
        state.unblock_mode = None;
    }
}

#[derive(Clone, Default)]
struct SessionSubscriptions {
    channels: HashSet<String>,
    patterns: HashSet<String>,
    shard_channels: HashSet<String>,
}

#[derive(Clone, Copy)]
pub enum SubscriptionKind {
    Channel,
    Pattern,
    ShardChannel,
}

impl SessionSubscriptions {
    fn len(&self) -> usize {
        self.channels.len() + self.patterns.len() + self.shard_channels.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone)]
pub struct AclUser {
    pub enabled: bool,
    pub nopass: bool,
    pub password: Option<String>,
    pub all_commands: bool,
    pub allowed: HashSet<String>,
    pub denied: HashSet<String>,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self::with_default_password(None)
    }

    pub fn with_default_password(password: Option<&str>) -> Self {
        let password = password.map(ToString::to_string);
        Self {
            sessions: DashMap::new(),
            controls: DashMap::new(),
            channels: DashMap::new(),
            patterns: DashMap::new(),
            shard_channels: DashMap::new(),
            subscriptions: DashMap::new(),
            monitors: DashMap::new(),
            acl_users: DashMap::from_iter([(
                "default".to_string(),
                AclUser {
                    enabled: true,
                    nopass: password.is_none(),
                    password,
                    all_commands: true,
                    allowed: HashSet::new(),
                    denied: HashSet::new(),
                },
            )]),
        }
    }
}
