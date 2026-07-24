#![cfg(feature = "tcp-integration-tests")]

mod support;

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use redis::{Commands, cmd};

    #[test]
    fn ping_echo_and_client_handshake_commands_work() {
        let (_server, mut con) = crate::support::setup_connection();

        let pong: String = cmd("PING").query(&mut con).unwrap();
        assert_eq!(pong, "PONG");

        let message: String = cmd("PING").arg("hello").query(&mut con).unwrap();
        assert_eq!(message, "hello");

        let echo: String = cmd("ECHO").arg("hello world").query(&mut con).unwrap();
        assert_eq!(echo, "hello world");

        let _: () = cmd("CLIENT")
            .arg("SETINFO")
            .arg("LIB-NAME")
            .arg("redis-rs")
            .query(&mut con)
            .unwrap();
        let _: () = cmd("CLIENT")
            .arg("SETINFO")
            .arg("LIB-VER")
            .arg("1.0.0")
            .query(&mut con)
            .unwrap();

        let info: String = cmd("CLIENT").arg("INFO").query(&mut con).unwrap();
        assert!(info.contains("lib-name=redis-rs"));
        assert!(info.contains("lib-ver=1.0.0"));

        let client_id: i64 = cmd("CLIENT").arg("ID").query(&mut con).unwrap();
        assert!(client_id > 0);
    }

    #[test]
    fn client_list_filters_kill_and_unblock_control_real_connections() {
        let (server, mut controller) = crate::support::setup_connection();
        let controller_id: i64 = cmd("CLIENT").arg("ID").query(&mut controller).unwrap();
        let mut target = server.connection();
        let target_id: i64 = cmd("CLIENT").arg("ID").query(&mut target).unwrap();

        let filtered: String = cmd("CLIENT")
            .arg("LIST")
            .arg("ID")
            .arg(target_id)
            .query(&mut controller)
            .unwrap();
        assert!(filtered.contains(&format!("id={target_id}")));
        assert!(!filtered.contains(&format!("id={controller_id} ")));

        let killed: i64 = cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(target_id)
            .query(&mut controller)
            .unwrap();
        assert_eq!(killed, 1);
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if cmd("PING").query::<String>(&mut target).is_err() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "killed client remained connected"
            );
            thread::sleep(Duration::from_millis(10));
        }

        let mut blocked = server.connection();
        let blocked_id: i64 = cmd("CLIENT").arg("ID").query(&mut blocked).unwrap();
        let blocked_query = thread::spawn(move || {
            cmd("BLPOP")
                .arg("client-unblock-missing")
                .arg(0)
                .query::<Option<(String, String)>>(&mut blocked)
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let unblocked: i64 = cmd("CLIENT")
                .arg("UNBLOCK")
                .arg(blocked_id)
                .arg("ERROR")
                .query(&mut controller)
                .unwrap();
            if unblocked == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "blocking client never became visible to CLIENT UNBLOCK"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let error = blocked_query.join().unwrap().unwrap_err();
        assert!(error.to_string().contains("UNBLOCKED"));

        let mut self_killed = server.connection();
        let self_killed_id: i64 = cmd("CLIENT").arg("ID").query(&mut self_killed).unwrap();
        let mut pipeline = redis::pipe();
        pipeline
            .cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(self_killed_id)
            .arg("SKIPME")
            .arg("NO")
            .cmd("PING");
        assert!(
            pipeline.query::<(i64, String)>(&mut self_killed).is_err(),
            "a self-killing CLIENT KILL must not execute later pipelined commands"
        );
    }

    #[test]
    fn select_switches_between_isolated_databases() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = crate::support::select_db(&mut con, 0).unwrap();
        let _: () = con.set("select-test-key", "db0").unwrap();

        let _: () = crate::support::select_db(&mut con, 1).unwrap();
        let missing_in_db1: bool = con.exists("select-test-key").unwrap();
        assert!(!missing_in_db1);

        let _: () = con.set("select-test-key", "db1").unwrap();
        let db1_value: String = con.get("select-test-key").unwrap();
        assert_eq!(db1_value, "db1");

        let _: () = crate::support::select_db(&mut con, 0).unwrap();
        let db0_value: String = con.get("select-test-key").unwrap();
        assert_eq!(db0_value, "db0");

        let invalid = crate::support::select_db(&mut con, 16);
        assert!(invalid.is_err());
    }
}
