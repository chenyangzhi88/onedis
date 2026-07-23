pub struct SessionManager {
    sessions: DashMap<usize, SessionSnapshot>,
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
            connected_at: session.connected_at(),
            last_interaction_at: session.last_interaction_at(),
            last_cmd: session.last_cmd().map(ToString::to_string),
            user: session.user().to_string(),
            peer_addr: session.peer_addr().to_string(),
            local_addr: session.local_addr().to_string(),
        }
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
