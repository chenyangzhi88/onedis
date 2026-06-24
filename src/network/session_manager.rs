use dashmap::DashMap;
use std::collections::HashSet;

use crate::frame::Frame;
use crate::network::connection::SharedWriter;
use crate::network::session::Session;

include!("session_manager_types.rs");
include!("session_registry.rs");
include!("session_pubsub.rs");
include!("session_monitor.rs");
include!("session_acl.rs");
include!("session_glob.rs");

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

    #[test]
    fn glob_match_handles_wildcards_and_long_inputs_iteratively() {
        assert!(glob_match("n*", "news"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));

        let text = "a".repeat(4096);
        let mut pattern = "*".repeat(512);
        pattern.push('z');
        assert!(!glob_match(&pattern, &text));
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
