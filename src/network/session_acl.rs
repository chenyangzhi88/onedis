impl SessionManager {
    pub fn acl_authenticate(&self, user: &str, password: &str) -> bool {
        let Some(acl_user) = self.acl_users.get(user) else {
            return false;
        };
        acl_user.enabled
            && (acl_user.nopass
                || acl_user
                    .password
                    .as_deref()
                    .is_some_and(|expected| constant_time_eq(expected.as_bytes(), password.as_bytes())))
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
            .map(|session| session.user.clone())
            .unwrap_or_else(|| "default".to_string())
    }

    pub fn acl_users(&self) -> Vec<String> {
        let mut users = self
            .acl_users
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        users.sort_unstable();
        users
    }

    pub fn acl_list(&self) -> Vec<String> {
        let mut entries = self
            .acl_users
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
                }
                let mut allowed = user.allowed.iter().collect::<Vec<_>>();
                allowed.sort_unstable();
                for command in allowed {
                    flags.push(format!("+{}", command));
                }
                let mut denied = user.denied.iter().collect::<Vec<_>>();
                denied.sort_unstable();
                for command in denied {
                    flags.push(format!("-{}", command));
                }
                format!("user {} {}", entry.key(), flags.join(" "))
            })
            .collect::<Vec<_>>();
        entries.sort_unstable();
        entries
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
            if let Some(password) = rule.strip_prefix('>') {
                user.nopass = false;
                user.password = Some(password.to_string());
                continue;
            }

            let normalized = rule.to_ascii_lowercase();
            match normalized.as_str() {
                "on" => user.enabled = true,
                "off" => user.enabled = false,
                "nopass" => {
                    user.nopass = true;
                    user.password = None;
                }
                "resetpass" => {
                    user.nopass = false;
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
                "allkeys" | "allchannels" | "clearselectors" => {}
                "reset" | "resetkeys" | "resetchannels" => {
                    return Err(format!(
                        "ERR ACL SETUSER modifier '{}' is not supported",
                        rule
                    ));
                }
                rule if rule.starts_with("+@") || rule.starts_with("-@") => {
                    return Err(format!(
                        "ERR ACL command category modifier '{}' is not supported",
                        rule
                    ));
                }
                rule if rule.starts_with('+') && rule.len() > 1 => {
                    user.allowed.insert(rule[1..].to_string());
                    user.denied.remove(&rule[1..]);
                }
                rule if rule.starts_with('-') && rule.len() > 1 => {
                    user.denied.insert(rule[1..].to_string());
                    user.allowed.remove(&rule[1..]);
                }
                "~*" | "&*" => {}
                rule if rule.starts_with('~') || rule.starts_with('&') => {
                    return Err(format!(
                        "ERR ACL key/channel pattern modifier '{}' is not supported",
                        rule
                    ));
                }
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

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let max_len = expected.len().max(actual.len());
    let mut difference = expected.len() ^ actual.len();
    for index in 0..max_len {
        let expected_byte = expected.get(index).copied().unwrap_or(0);
        let actual_byte = actual.get(index).copied().unwrap_or(0);
        difference |= usize::from(expected_byte ^ actual_byte);
    }
    difference == 0
}
