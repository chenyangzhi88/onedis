#![cfg(feature = "tcp-integration-tests")]

mod support;

#[cfg(test)]
mod tests {
    use redis::{Commands, RedisResult};

    /// 设置测试环境并返回 Redis 连接
    /// 测试事务基本功能
    #[test]
    fn test_basic_transaction() {
        // 连接到服务器（假设服务器已经在运行）
        let (_server, mut con) = crate::support::setup_connection();

        // 清理测试数据
        let _: () = con.del("key").unwrap();

        // 发送 MULTI 命令
        let result: RedisResult<()> = redis::cmd("MULTI").query(&mut con);
        assert!(result.is_ok());

        // 发送 SET 命令
        let result: RedisResult<()> = redis::cmd("SET").arg("key").arg("value").query(&mut con);
        assert!(result.is_ok());

        // 发送 GET 命令
        let result: RedisResult<()> = redis::cmd("GET").arg("key").query(&mut con);
        assert!(result.is_ok());

        // 发送 EXEC 命令
        let result: RedisResult<Vec<String>> = redis::cmd("EXEC").query(&mut con);
        assert!(result.is_ok());

        let results = result.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "OK"); // SET 命令的结果
        assert_eq!(results[1], "value"); // GET 命令的结果
    }

    /// 测试 DISCARD 命令
    #[test]
    fn test_discard_transaction() {
        // 连接到服务器（假设服务器已经在运行）
        let (_server, mut con) = crate::support::setup_connection();

        // 清理测试数据
        let _: () = con.del("discard_key").unwrap();

        // 发送 MULTI 命令
        let result: RedisResult<()> = redis::cmd("MULTI").query(&mut con);
        assert!(result.is_ok());

        // 发送 SET 命令
        let result: RedisResult<()> = redis::cmd("SET")
            .arg("discard_key")
            .arg("value")
            .query(&mut con);
        assert!(result.is_ok());

        // 发送 DISCARD 命令
        let result: RedisResult<String> = redis::cmd("DISCARD").query(&mut con);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "OK");

        // 验证键未被设置
        let result: RedisResult<Option<String>> = con.get("discard_key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    /// 测试在非事务模式下使用 EXEC 和 DISCARD
    #[test]
    fn test_exec_discard_without_multi() {
        // 连接到服务器（假设服务器已经在运行）
        let (_server, mut con) = crate::support::setup_connection();

        // 发送 EXEC 命令（没有先发送 MULTI）
        let result: RedisResult<()> = redis::cmd("EXEC").query(&mut con);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("EXEC without MULTI") || err_msg.contains("ERR"));

        // 发送 DISCARD 命令（没有先发送 MULTI）
        let result: RedisResult<()> = redis::cmd("DISCARD").query(&mut con);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("DISCARD without MULTI") || err_msg.contains("ERR"));
    }

    #[test]
    fn test_watch_exec_returns_nil_when_watched_key_changes() {
        let (server, mut con1) = crate::support::setup_connection();
        let mut con2 = server.connection();

        let _: () = con1.del("watch-key").unwrap();
        let _: () = redis::cmd("SET")
            .arg("watch-key")
            .arg("old")
            .query(&mut con1)
            .unwrap();

        let _: () = redis::cmd("WATCH")
            .arg("watch-key")
            .query(&mut con1)
            .unwrap();
        let _: () = redis::cmd("MULTI").query(&mut con1).unwrap();
        let _: () = redis::cmd("SET")
            .arg("watch-key")
            .arg("queued")
            .query(&mut con1)
            .unwrap();

        let _: () = redis::cmd("SET")
            .arg("watch-key")
            .arg("changed")
            .query(&mut con2)
            .unwrap();

        let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut con1).unwrap();
        assert!(result.is_none());
        let value: String = con1.get("watch-key").unwrap();
        assert_eq!(value, "changed");
    }

    #[test]
    fn test_unwatch_allows_transaction_after_key_changes() {
        let (server, mut con1) = crate::support::setup_connection();
        let mut con2 = server.connection();

        let _: () = con1.del("unwatch-key").unwrap();
        let _: () = redis::cmd("WATCH")
            .arg("unwatch-key")
            .query(&mut con1)
            .unwrap();
        let _: () = redis::cmd("UNWATCH").query(&mut con1).unwrap();
        let _: () = redis::cmd("SET")
            .arg("unwatch-key")
            .arg("changed-before-multi")
            .query(&mut con2)
            .unwrap();
        let _: () = redis::cmd("MULTI").query(&mut con1).unwrap();
        let _: () = redis::cmd("SET")
            .arg("unwatch-key")
            .arg("committed")
            .query(&mut con1)
            .unwrap();

        let result: Vec<String> = redis::cmd("EXEC").query(&mut con1).unwrap();
        assert_eq!(result, vec!["OK".to_string()]);
        let value: String = con1.get("unwatch-key").unwrap();
        assert_eq!(value, "committed");
    }

    #[test]
    fn test_watch_detects_same_value_rewrite() {
        let (server, mut con1) = crate::support::setup_connection();
        let mut con2 = server.connection();

        let _: () = con1.del("watch-same-value").unwrap();
        let _: () = redis::cmd("SET")
            .arg("watch-same-value")
            .arg("stable")
            .query(&mut con1)
            .unwrap();

        let _: () = redis::cmd("WATCH")
            .arg("watch-same-value")
            .query(&mut con1)
            .unwrap();
        let _: () = redis::cmd("MULTI").query(&mut con1).unwrap();
        let _: () = redis::cmd("SET")
            .arg("watch-same-value")
            .arg("queued")
            .query(&mut con1)
            .unwrap();

        let _: () = redis::cmd("SET")
            .arg("watch-same-value")
            .arg("stable")
            .query(&mut con2)
            .unwrap();

        let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut con1).unwrap();
        assert!(result.is_none());
        let value: String = con1.get("watch-same-value").unwrap();
        assert_eq!(value, "stable");
    }
}
