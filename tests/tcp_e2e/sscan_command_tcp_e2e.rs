#![cfg(feature = "tcp-integration-tests")]

mod support;

#[cfg(test)]
mod tests {
    use redis::Commands;

    #[test]
    fn test_sscan_basic() {
        let (_server, mut con) = crate::support::setup_connection();

        // 清理数据库
        let _: () = redis::cmd("FLUSHDB").exec(&mut con).unwrap();

        // 添加一些测试数据到集合
        let _: () = con.sadd("myset", "member1").unwrap();
        let _: () = con.sadd("myset", "member2").unwrap();
        let _: () = con.sadd("myset", "member3").unwrap();

        // 测试基本的SSCAN命令
        let result = crate::support::sscan(&mut con, "myset", 0, None, None).unwrap();
        let (cursor, members) = result;

        // 验证返回的结果
        assert_eq!(cursor, 0); // 由于只有3个成员且默认COUNT为10，应该一次就返回所有成员，游标为0
        assert!(!members.is_empty());
        // 检查返回的成员
        assert!(members.contains(&"member1".to_string()));
        assert!(members.contains(&"member2".to_string()));
        assert!(members.contains(&"member3".to_string()));
    }

    #[test]
    fn test_sscan_with_match() {
        let (_server, mut con) = crate::support::setup_connection();

        // 清理数据库
        let _: () = redis::cmd("FLUSHDB").exec(&mut con).unwrap();

        // 添加一些测试数据到集合
        let _: () = con.sadd("myset", "user:1").unwrap();
        let _: () = con.sadd("myset", "user:2").unwrap();
        let _: () = con.sadd("myset", "admin:1").unwrap();

        // 测试带MATCH参数的SSCAN命令
        let result = crate::support::sscan(&mut con, "myset", 0, Some("user:*"), None).unwrap();
        let (cursor, members) = result;

        // 验证返回的结果
        assert_eq!(cursor, 0);
        assert_eq!(members.len(), 2);
        // 所有返回的成员都应该匹配模式"user:*"
        for member in &members {
            assert!(member.starts_with("user:"));
        }
    }

    #[test]
    fn test_sscan_with_count() {
        let (_server, mut con) = crate::support::setup_connection();

        // 清理数据库
        let _: () = redis::cmd("FLUSHDB").exec(&mut con).unwrap();

        // 添加一些测试数据到集合
        for i in 0..20 {
            let _: () = con.sadd("myset", format!("member_{}", i)).unwrap();
        }

        // 测试带COUNT参数的SSCAN命令
        let result = crate::support::sscan(&mut con, "myset", 0, None, Some(5)).unwrap();
        let (cursor, members) = result;

        // 验证返回的结果
        assert!(cursor >= 0);
        // 应该返回大约5个成员（具体数量可能因实现而异）
        assert!(!members.is_empty());
        assert!(members.len() <= 10); // 给一些余地
    }

    #[test]
    fn test_sscan_nonexistent_key() {
        let (_server, mut con) = crate::support::setup_connection();

        // 清理数据库
        let _: () = redis::cmd("FLUSHDB").exec(&mut con).unwrap();

        // 测试对不存在键的SSCAN命令
        let result = crate::support::sscan(&mut con, "nonexistent", 0, None, None).unwrap();
        let (cursor, members) = result;

        // 验证返回的结果
        assert_eq!(cursor, 0);
        assert!(members.is_empty());
    }

    #[test]
    fn test_sscan_wrong_type() {
        let (_server, mut con) = crate::support::setup_connection();

        // 清理数据库
        let _: () = redis::cmd("FLUSHDB").exec(&mut con).unwrap();

        // 添加一个字符串类型的键
        let _: () = con.set("string_key", "value").unwrap();

        // 测试对错误类型键的SSCAN命令
        let result = crate::support::sscan(&mut con, "string_key", 0, None, None);

        // 验证返回错误
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("wrong kind of value"));
    }
}
