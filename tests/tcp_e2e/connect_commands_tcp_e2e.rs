#![cfg(feature = "tcp-integration-tests")]

mod support;

#[cfg(test)]
mod tests {
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

        let client_id: i64 = cmd("CLIENT").arg("ID").query(&mut con).unwrap();
        assert!(client_id > 0);
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
