use dashmap::DashMap;
use std::collections::HashSet;

use crate::frame::Frame;
use crate::network::connection::SharedWriter;
use crate::network::session::Session;

/// 高性能会话管理器
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
    // 创建实例
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

    /// 添加会话
    pub fn create_session(&self, session: Session) {
        self.sessions.insert(session.get_id(), session);
    }

    pub fn update_session(&self, session: Session) {
        self.sessions.insert(session.get_id(), session);
    }

    /// 移除会话
    pub fn remove_session(&self, session_id: usize) -> bool {
        self.unsubscribe_all(session_id);
        self.monitors.remove(&session_id);
        self.sessions.remove(&session_id).is_some()
    }

    /// 获取当前连接数
    pub fn get_connection_count(&self) -> usize {
        self.sessions.len()
    }

    /// 检查是否超过最大连接数限制
    pub fn is_over_max_clients(&self, maxclients: usize) -> bool {
        if maxclients == 0 {
            return false; // 0 表示无限制
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

    pub fn register_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_pattern(&self, pattern: &str, session_id: usize, writer: SharedWriter) {
        self.patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn register_shard_channel(&self, channel: &str, session_id: usize, writer: SharedWriter) {
        self.shard_channels
            .entry(channel.to_string())
            .or_default()
            .insert(session_id, writer);
    }

    pub fn unregister_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.channels.get(channel) {
            map.remove(&session_id);
        }
    }

    pub fn unregister_pattern(&self, pattern: &str, session_id: usize) {
        if let Some(map) = self.patterns.get(pattern) {
            map.remove(&session_id);
        }
    }

    pub fn unregister_shard_channel(&self, channel: &str, session_id: usize) {
        if let Some(map) = self.shard_channels.get(channel) {
            map.remove(&session_id);
        }
    }

    pub fn unsubscribe_all(&self, session_id: usize) {
        for entry in self.channels.iter() {
            entry.value().remove(&session_id);
        }
        for entry in self.patterns.iter() {
            entry.value().remove(&session_id);
        }
        for entry in self.shard_channels.iter() {
            entry.value().remove(&session_id);
        }
    }

    pub async fn publish(&self, channel: &str, message: &str, shard: bool) -> usize {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        let mut writers = Vec::new();
        if let Some(map) = source.get(channel) {
            writers.extend(map.iter().map(|entry| entry.value().clone()));
        }
        let direct_count = writers.len();
        let frame_name = if shard { "smessage" } else { "message" };
        for writer in writers {
            writer
                .write_bytes(
                    Frame::Array(vec![
                        Frame::bulk_string(frame_name),
                        Frame::bulk_string(channel.to_string()),
                        Frame::bulk_string(message.to_string()),
                    ])
                    .as_bytes(),
                )
                .await;
        }

        let mut pattern_deliveries = 0usize;
        if !shard {
            let mut pattern_writers = Vec::new();
            for entry in self.patterns.iter() {
                if glob_match(entry.key(), channel) {
                    for writer in entry.value().iter() {
                        pattern_writers.push((entry.key().clone(), writer.value().clone()));
                    }
                }
            }
            pattern_deliveries = pattern_writers.len();
            for (pattern, writer) in pattern_writers {
                writer
                    .write_bytes(
                        Frame::Array(vec![
                            Frame::bulk_string("pmessage"),
                            Frame::bulk_string(pattern),
                            Frame::bulk_string(channel.to_string()),
                            Frame::bulk_string(message.to_string()),
                        ])
                        .as_bytes(),
                    )
                    .await;
            }
        }
        direct_count + pattern_deliveries
    }

    pub fn channel_count(&self, channel: &str, shard: bool) -> usize {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        source.get(channel).map(|m| m.len()).unwrap_or(0)
    }

    pub fn pattern_count(&self) -> usize {
        self.patterns.iter().map(|entry| entry.value().len()).sum()
    }

    pub fn channel_names(&self, shard: bool) -> Vec<String> {
        let source = if shard {
            &self.shard_channels
        } else {
            &self.channels
        };
        source
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .map(|entry| entry.key().clone())
            .collect()
    }

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

fn glob_match(pattern: &str, text: &str) -> bool {
    fn inner(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => inner(&p[1..], t) || (!t.is_empty() && inner(p, &t[1..])),
            b'?' => !t.is_empty() && inner(&p[1..], &t[1..]),
            c => !t.is_empty() && c == t[0] && inner(&p[1..], &t[1..]),
        }
    }
    inner(pattern.as_bytes(), text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::connection::Connection;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    async fn shared_writer_pair() -> (SharedWriter, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = listener.accept();
        let connect = TcpStream::connect(addr);
        let (accepted, connected) = tokio::join!(accept, connect);
        let (server_stream, _) = accepted.unwrap();
        let mut connection = Connection::new(server_stream);
        (connection.shared_writer(), connected.unwrap())
    }

    #[test]
    fn acl_rules_cover_auth_allow_deny_listing_and_deletion_edges() {
        let manager = SessionManager::new();

        assert!(!manager.is_over_max_clients(0));
        assert!(!manager.is_over_max_clients(1));
        assert!(manager.acl_authenticate("default", "anything"));
        assert!(!manager.acl_authenticate("missing", "pw"));
        assert!(manager.acl_allows("default", "GET"));
        assert!(!manager.acl_allows("missing", "GET"));

        manager
            .acl_setuser(
                "alice",
                &[
                    "on".to_string(),
                    ">pw".to_string(),
                    "-@all".to_string(),
                    "+get".to_string(),
                    "-set".to_string(),
                    "~*".to_string(),
                    "&*".to_string(),
                ],
            )
            .unwrap();
        assert!(manager.acl_authenticate("alice", "pw"));
        assert!(!manager.acl_authenticate("alice", "bad"));
        assert!(manager.acl_allows("alice", "GET"));
        assert!(!manager.acl_allows("alice", "SET"));
        assert!(manager.acl_users().contains(&"alice".to_string()));
        assert!(manager.acl_list().iter().any(|line| line.contains("alice")));

        manager
            .acl_setuser("alice", &["off".to_string(), "nopass".to_string()])
            .unwrap();
        assert!(!manager.acl_authenticate("alice", "pw"));
        assert!(!manager.acl_allows("alice", "GET"));
        assert!(
            manager
                .acl_setuser("bad", &["invalid".to_string()])
                .is_err()
        );
        assert_eq!(manager.acl_deluser(&["default".to_string()]), 0);
        assert_eq!(manager.acl_deluser(&["alice".to_string()]), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pubsub_monitor_counts_names_unregister_and_delivery_paths_work() {
        let manager = SessionManager::new();
        let (writer_a, mut client_a) = shared_writer_pair().await;
        let (writer_b, mut client_b) = shared_writer_pair().await;
        let (writer_c, mut client_c) = shared_writer_pair().await;

        manager.register_channel("news", 1, writer_a.clone());
        manager.register_pattern("n*", 2, writer_b.clone());
        manager.register_shard_channel("shard", 3, writer_c.clone());
        assert_eq!(manager.channel_count("news", false), 1);
        assert_eq!(manager.channel_count("shard", true), 1);
        assert_eq!(manager.pattern_count(), 1);
        assert!(manager.channel_names(false).contains(&"news".to_string()));
        assert!(manager.channel_names(true).contains(&"shard".to_string()));

        assert_eq!(manager.publish("news", "payload", false).await, 2);
        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client_a.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("message"));
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client_b.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("pmessage"));

        assert_eq!(manager.publish("shard", "payload", true).await, 1);
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client_c.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("smessage"));

        manager.add_monitor(9, writer_a.clone());
        manager.broadcast_monitor(1, "PING".to_string()).await;
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client_a.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("PING"));

        manager.unregister_channel("news", 1);
        manager.unregister_pattern("n*", 2);
        manager.unregister_shard_channel("shard", 3);
        assert_eq!(manager.channel_count("news", false), 0);
        assert_eq!(manager.pattern_count(), 0);
        manager.unsubscribe_all(1);
        assert_eq!(manager.acl_whoami(404), "default");
    }
}
