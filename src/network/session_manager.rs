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
    use std::sync::Arc;
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
        let protected = SessionManager::with_default_password(Some("secret"));
        assert!(!protected.acl_authenticate("default", "anything"));
        assert!(protected.acl_authenticate("default", "secret"));

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
        assert!(
            manager
                .acl_setuser("bad-pattern", &["~tenant:*".to_string()])
                .is_err()
        );
        assert!(
            manager
                .acl_setuser("bad-category", &["+@read".to_string()])
                .is_err()
        );
        manager
            .acl_setuser(
                "uppercase",
                &[
                    "ON".to_string(),
                    ">CaseSensitive".to_string(),
                    "-@ALL".to_string(),
                    "+GET".to_string(),
                ],
            )
            .unwrap();
        assert!(manager.acl_authenticate("uppercase", "CaseSensitive"));
        assert!(!manager.acl_authenticate("uppercase", "casesensitive"));
        assert!(manager.acl_allows("uppercase", "get"));
        assert_eq!(manager.acl_deluser(&["default".to_string()]), 0);
        assert_eq!(manager.acl_deluser(&["alice".to_string()]), 1);
    }

    #[test]
    fn glob_match_handles_wildcards_and_long_inputs_iteratively() {
        assert!(glob_match("n*", "news"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[^e]llo", "hello"));
        assert!(glob_match("h[^e]llo", "hallo"));
        assert!(glob_match("item[0-9]", "item7"));
        assert!(glob_match("item[9-0]", "item7"));
        assert!(glob_match(r"literal\\*", r"literal\anything"));
        assert!(glob_match(r"literal\*", "literal*"));
        assert!(!glob_match("[unfinished", "[unfinished"));

        let text = "a".repeat(4096);
        let mut pattern = "*".repeat(512);
        pattern.push('z');
        assert!(!glob_match(&pattern, &text));

        let chunks = pubsub_message_chunks(&["pmessage", "n*", "news"], Arc::from(&b"payload"[..]));
        let encoded = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(
            encoded,
            b"*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$7\r\npayload\r\n"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pubsub_monitor_counts_names_unregister_and_delivery_paths_work() {
        let manager = SessionManager::new();
        let (writer_a, mut client_a) = shared_writer_pair().await;
        let (writer_b, mut client_b) = shared_writer_pair().await;
        let (writer_c, mut client_c) = shared_writer_pair().await;

        manager.register_channel("news", 1, writer_a.clone());
        manager.register_channel("news", 1, writer_a.clone());
        manager.register_pattern("n*", 2, writer_b.clone());
        manager.register_shard_channel("shard", 3, writer_c.clone());
        assert_eq!(manager.subscription_count(1), 1);
        assert_eq!(
            manager.additional_subscription_count(
                1,
                &["news".to_string(), "news".to_string(), "other".to_string()],
                SubscriptionKind::Channel,
            ),
            1
        );
        assert_eq!(
            manager.subscription_ack_count(1, SubscriptionKind::Channel),
            1
        );
        assert_eq!(
            manager.subscription_ack_count(3, SubscriptionKind::ShardChannel),
            1
        );
        assert_eq!(manager.channel_subscriptions(1), vec!["news"]);
        assert_eq!(manager.pattern_subscriptions(2), vec!["n*"]);
        assert_eq!(manager.shard_subscriptions(3), vec!["shard"]);
        assert_eq!(manager.channel_count("news", false), 1);
        assert_eq!(manager.channel_count("shard", true), 1);
        assert_eq!(manager.pattern_count(), 1);
        assert!(manager.channel_names(false).contains(&"news".to_string()));
        assert!(manager.channel_names(true).contains(&"shard".to_string()));

        assert_eq!(manager.publish("news", "payload", false), 2);
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
        manager.register_pattern("n*", 1, writer_a.clone());
        assert_eq!(manager.pattern_count(), 1);
        manager.unregister_pattern("n*", 1);

        assert_eq!(manager.publish("shard", "payload", true), 1);
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client_c.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("smessage"));

        let (slow_writer, _slow_client) = shared_writer_pair().await;
        manager.register_channel("slow", 77, slow_writer.clone());
        manager.register_pattern("slow*", 77, slow_writer.clone());
        slow_writer.close_for_test();
        assert_eq!(manager.publish("slow", "payload", false), 0);
        assert_eq!(manager.subscription_count(77), 0);
        assert_eq!(manager.channel_count("slow", false), 0);

        manager.add_monitor(9, writer_a.clone());
        manager.broadcast_monitor(1, "PING".to_string());
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
        assert!(manager.channels.is_empty());
        assert!(manager.patterns.is_empty());
        assert!(manager.shard_channels.is_empty());
        assert!(manager.subscriptions.is_empty());
        manager.unsubscribe_all(1);
        assert_eq!(manager.acl_whoami(404), "default");
    }
}
