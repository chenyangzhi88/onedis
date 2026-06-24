pub struct SessionManager {
    sessions: DashMap<usize, Session>,
    channels: DashMap<String, DashMap<usize, SharedWriter>>,
    patterns: DashMap<String, DashMap<usize, SharedWriter>>,
    shard_channels: DashMap<String, DashMap<usize, SharedWriter>>,
    monitors: DashMap<usize, SharedWriter>,
    acl_users: DashMap<String, AclUser>,
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

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            channels: DashMap::new(),
            patterns: DashMap::new(),
            shard_channels: DashMap::new(),
            monitors: DashMap::new(),
            acl_users: DashMap::from_iter([(
                "default".to_string(),
                AclUser {
                    enabled: true,
                    nopass: true,
                    password: None,
                    all_commands: true,
                    allowed: HashSet::new(),
                    denied: HashSet::new(),
                },
            )]),
        }
    }
}
