impl SessionManager {
    pub fn acl_authenticate(&self, user: &str, password: &str) -> bool {
        let Some(acl_user) = self.acl_users.get(user) else {
            return false;
        };
        acl_user.enabled && (acl_user.nopass || acl_user.password.as_deref() == Some(password))
    }

    pub fn acl_allows(&self, user: &str, command: &str) -> bool {
        let Some(acl_user) = self.acl_users.get(user) else {
            return false;
        };
        if !acl_user.enabled {
            return false;
        }
        let command = command.to_ascii_lowercase();
        if acl_user.denied.contains(&command) {
            return false;
        }
        acl_user.all_commands || acl_user.allowed.contains(&command)
    }

    pub fn acl_whoami(&self, session_id: usize) -> String {
        self.sessions
            .get(&session_id)
            .map(|session| session.user().to_string())
            .unwrap_or_else(|| "default".to_string())
    }

    pub fn acl_users(&self) -> Vec<String> {
        self.acl_users
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn acl_list(&self) -> Vec<String> {
        self.acl_users
            .iter()
            .map(|entry| {
                let user = entry.value();
                let mut flags = Vec::new();
                flags.push(if user.enabled { "on" } else { "off" }.to_string());
                if user.nopass {
                    flags.push("nopass".to_string());
                } else {
                    flags.push("#<redacted>".to_string());
                }
                flags.push("~*".to_string());
                flags.push("&*".to_string());
                if user.all_commands {
                    flags.push("+@all".to_string());
                } else {
                    flags.push("-@all".to_string());
                    for command in &user.allowed {
                        flags.push(format!("+{}", command));
                    }
                    for command in &user.denied {
                        flags.push(format!("-{}", command));
                    }
                }
                format!("user {} {}", entry.key(), flags.join(" "))
            })
            .collect()
    }

    pub fn acl_setuser(&self, name: &str, rules: &[String]) -> Result<(), String> {
        let mut user = self
            .acl_users
            .get(name)
            .map(|entry| entry.value().clone())
            .unwrap_or(AclUser {
                enabled: false,
                nopass: false,
                password: None,
                all_commands: false,
                allowed: HashSet::new(),
                denied: HashSet::new(),
            });
        for rule in rules {
            match rule.as_str() {
                "on" => user.enabled = true,
                "off" => user.enabled = false,
                "nopass" => {
                    user.nopass = true;
                    user.password = None;
                }
                "+@all" | "allcommands" => {
                    user.all_commands = true;
                    user.denied.clear();
                }
                "-@all" | "nocommands" => {
                    user.all_commands = false;
                    user.allowed.clear();
                }
                rule if rule.starts_with('>') => {
                    user.nopass = false;
                    user.password = Some(rule[1..].to_string());
                }
                rule if rule.starts_with('+') => {
                    user.allowed.insert(rule[1..].to_ascii_lowercase());
                    user.denied.remove(&rule[1..].to_ascii_lowercase());
                }
                rule if rule.starts_with('-') => {
                    user.denied.insert(rule[1..].to_ascii_lowercase());
                    user.allowed.remove(&rule[1..].to_ascii_lowercase());
                }
                rule if rule.starts_with('~') || rule.starts_with('&') => {}
                _ => return Err(format!("ERR Error in ACL SETUSER modifier '{}'", rule)),
            }
        }
        self.acl_users.insert(name.to_string(), user);
        Ok(())
    }

    pub fn acl_deluser(&self, users: &[String]) -> usize {
        users
            .iter()
            .filter(|user| user.as_str() != "default")
            .filter(|user| self.acl_users.remove(*user).is_some())
            .count()
    }
}
