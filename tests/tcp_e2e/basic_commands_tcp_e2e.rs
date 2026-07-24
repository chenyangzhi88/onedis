#[cfg(feature = "tcp-integration-tests")]
mod support;

#[cfg(not(feature = "tcp-integration-tests"))]
#[test]
#[ignore = "requires local TCP socket creation; run with --features tcp-integration-tests"]
fn basic_commands_tcp_e2e_requires_tcp_socket() {}

#[cfg(feature = "tcp-integration-tests")]
mod tests {

    use std::{thread::sleep, time::Duration};

    use redis::Commands;

    #[test]
    fn test_set() {
        let (_server, mut con) = crate::support::setup_connection();
        let _: () = con.set("test", "Helloword").unwrap();
        let get_set_result: String = con.get("test").unwrap();
        assert_eq!(get_set_result, "Helloword");
    }

    #[test]
    fn test_set_batch() {
        let (_server, mut con) = crate::support::setup_connection();
        for i in 0..1000 {
            let _: () = con.set(i.to_string(), i.to_string()).unwrap();
        }
    }

    #[test]
    fn test_get_batch() {
        let (_server, mut con) = crate::support::setup_connection();
        for _i in 0..1000 {
            let _: Option<String> = con.get("user").unwrap();
        }
    }

    #[test]
    fn test_del() {
        let (_server, mut con) = crate::support::setup_connection();
        let _: () = con.set("del-test", "Helloword").unwrap();

        let get_set_result: String = con.get("del-test").unwrap();
        assert_eq!(get_set_result, "Helloword");

        let _: () = con.del("del-test").unwrap();
        let get_del_result: Option<String> = con.get("del-test").unwrap();
        assert_eq!(get_del_result, None);
    }

    #[test]
    fn test_append() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.set("append-test", "Hello").unwrap();
        let _: () = con.append("append-test", "word").unwrap();
        let get_result: String = con.get("append-test").unwrap();
        assert_eq!(get_result, "Helloword");
    }

    #[test]
    fn test_setrange() {
        let (_server, mut con) = crate::support::setup_connection();

        // 测试基本的 setrange 功能
        let _: () = con.set("setrange-test", "Hello World").unwrap();
        let result: usize = con.setrange("setrange-test", 6, "Redis").unwrap();
        assert_eq!(result, 11);

        let get_result: String = con.get("setrange-test").unwrap();
        assert_eq!(get_result, "Hello Redis");

        // 测试扩展字符串长度的情况
        let _: () = con.set("setrange-extend", "Hello").unwrap();
        let result: usize = con.setrange("setrange-extend", 10, "World").unwrap();
        assert_eq!(result, 15);

        let get_result: String = con.get("setrange-extend").unwrap();
        // 注意：中间会有空字节，所以结果可能不是直观的 "Hello     World"
        assert_eq!(get_result.len(), 15);
    }

    #[test]
    fn test_exists() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.set("exists-test", "Helloworld").unwrap();
        let key_exists: bool = con.exists("exists-test").unwrap();
        assert!(key_exists);
    }

    #[test]
    fn test_rename() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.set("rename-test", "Helloworld").unwrap();
        let _: () = con.rename("rename-test", "rename-new-test").unwrap();

        let key_exists: bool = con.exists("rename-new-test").unwrap();

        log::info!("是否存在：{}", key_exists);

        assert!(key_exists);
    }

    #[test]
    fn test_keys() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.set("keys-1-test", "Helloworld").unwrap();
        let _: () = con.set("keys-2-test", "Helloworld").unwrap();
        let _: () = con.set("keys-3-test", "Helloworld").unwrap();

        let result: Vec<String> = con.keys("keys*").unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_llen() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.del("llen-test").unwrap();
        let _: () = con.rpush("llen-test", "Helloworld").unwrap();
        let _: () = con.rpush("llen-test", "Helloworld").unwrap();
        let _: () = con.rpush("llen-test", "Helloworld").unwrap();

        let count: usize = con.llen("llen-test").unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_rpush() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.del("rpush-test").unwrap();
        let _: () = con.rpush("rpush-test", "Helloworld1").unwrap();
        let _: () = con.rpush("rpush-test", "Helloworld2").unwrap();
        let _: () = con.rpush("rpush-test", "Helloworld3").unwrap();

        let value: String = con.lindex("rpush-test", 0).unwrap();

        assert_eq!(value, "Helloworld1");
    }

    #[test]
    fn test_lpush() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.del("lpush-test").unwrap();
        let _: () = con.lpush("lpush-test", "Helloworld1").unwrap();
        let _: () = con.lpush("lpush-test", "Helloworld2").unwrap();
        let _: () = con.lpush("lpush-test", "Helloworld3").unwrap();

        let value: String = con.lindex("lpush-test", 0).unwrap();

        assert_eq!(value, "Helloworld3");
    }

    #[test]
    fn test_sadd() {
        let (_server, mut con) = crate::support::setup_connection();

        let _: () = con.del("sadd-test").unwrap();
        let _: () = con.sadd("sadd-test", "admin1").unwrap();
        let _: () = con.sadd("sadd-test", "admin2").unwrap();
        let _: () = con.sadd("sadd-test", "admin3").unwrap();

        let count: usize = con.scard("sadd-test").unwrap();
        assert_eq!(count, 3);

        let members: Vec<String> = con.smembers("sadd-test").unwrap();
        assert_eq!(members.len(), 3);
    }

    #[test]
    fn test_expire() {
        let (_server, mut con) = crate::support::setup_connection();
        let _: () = con.set("test-expire", "Helloword").unwrap();
        let _: () = con.expire("test-expire", 3).unwrap();

        sleep(Duration::from_secs(2));

        let value1: Option<String> = con.get("test-expire").unwrap();
        assert_eq!(value1, Some("Helloword".to_string()));

        sleep(Duration::from_secs(2));

        let value2: Option<String> = con.get("test-expire").unwrap();
        assert_eq!(value2, None);
    }

    #[test]
    fn test_hmset() {
        let (_server, mut con) = crate::support::setup_connection();

        let data: [(String, String); 3] = [
            ("name".to_string(), "Alice".to_string()),
            ("age".to_string(), "30".to_string()),
            ("email".to_string(), "alice@example.com".to_string()),
        ];

        let _: () = con.del("test-hmset").unwrap();
        let _: () = con.hset_multiple("test-hmset", &data).unwrap();

        let name: String = con.hget("test-hmset", "name").unwrap();
        assert_eq!(name, "Alice");

        let _: () = con.hdel("test-hmset", "email").unwrap();

        let email: Option<String> = con.hget("test-hmset", "email").unwrap();
        assert_eq!(email, None);

        let _: () = con.hset("test-hmset", "sex", "boy").unwrap();

        let sex: String = con.hget("test-hmset", "sex").unwrap();
        assert_eq!(sex, "boy");

        let exists: usize = con.hexists("test-hmset", "city").unwrap();
        assert_eq!(exists, 0);
    }

    #[test]
    fn test_ltrim() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-test").unwrap();
        let _: () = con.rpush("ltrim-test", "hello").unwrap();
        let _: () = con.rpush("ltrim-test", "hello").unwrap();
        let _: () = con.rpush("ltrim-test", "foo").unwrap();
        let _: () = con.rpush("ltrim-test", "bar").unwrap();

        // 执行 LTRIM 命令
        let result: String = con.ltrim("ltrim-test", 1, -1).unwrap();
        assert_eq!(result, "OK");

        // 验证结果
        let range: Vec<String> = con.lrange("ltrim-test", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "hello");
        assert_eq!(range[1], "foo");
        assert_eq!(range[2], "bar");
    }

    #[test]
    fn test_ltrim_out_of_range() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-out-of-range-test").unwrap();
        let _: () = con.rpush("ltrim-out-of-range-test", "hello").unwrap();
        let _: () = con.rpush("ltrim-out-of-range-test", "world").unwrap();

        // 执行 LTRIM 命令，使用超出范围的索引
        let result: String = con.ltrim("ltrim-out-of-range-test", 5, 10).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 列表应该为空
        let range: Vec<String> = con.lrange("ltrim-out-of-range-test", 0, -1).unwrap();
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_ltrim_with_negative_indices() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-negative-indices-test").unwrap();
        let _: () = con.rpush("ltrim-negative-indices-test", "one").unwrap();
        let _: () = con.rpush("ltrim-negative-indices-test", "two").unwrap();
        let _: () = con.rpush("ltrim-negative-indices-test", "three").unwrap();
        let _: () = con.rpush("ltrim-negative-indices-test", "four").unwrap();
        let _: () = con.rpush("ltrim-negative-indices-test", "five").unwrap();

        // 执行 LTRIM 命令，使用负数索引
        let result: String = con.ltrim("ltrim-negative-indices-test", -3, -1).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 应该保留最后3个元素
        let range: Vec<String> = con.lrange("ltrim-negative-indices-test", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "three");
        assert_eq!(range[1], "four");
        assert_eq!(range[2], "five");
    }

    #[test]
    fn test_ltrim_entire_list() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-entire-list-test").unwrap();
        let _: () = con.rpush("ltrim-entire-list-test", "a").unwrap();
        let _: () = con.rpush("ltrim-entire-list-test", "b").unwrap();
        let _: () = con.rpush("ltrim-entire-list-test", "c").unwrap();

        // 执行 LTRIM 命令，保留整个列表
        let result: String = con.ltrim("ltrim-entire-list-test", 0, -1).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 列表应该保持不变
        let range: Vec<String> = con.lrange("ltrim-entire-list-test", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "a");
        assert_eq!(range[1], "b");
        assert_eq!(range[2], "c");
    }

    #[test]
    fn test_ltrim_empty_list() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备空列表
        let _: () = con.del("ltrim-empty-list-test").unwrap();

        // 执行 LTRIM 命令在空列表上
        let result: String = con.ltrim("ltrim-empty-list-test", 0, -1).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 列表应该仍然为空
        let range: Vec<String> = con.lrange("ltrim-empty-list-test", 0, -1).unwrap();
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_ltrim_start_greater_than_end() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-start-greater-test").unwrap();
        let _: () = con.rpush("ltrim-start-greater-test", "x").unwrap();
        let _: () = con.rpush("ltrim-start-greater-test", "y").unwrap();
        let _: () = con.rpush("ltrim-start-greater-test", "z").unwrap();

        // 执行 LTRIM 命令，start 大于 end
        let result: String = con.ltrim("ltrim-start-greater-test", 2, 1).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 列表应该为空
        let range: Vec<String> = con.lrange("ltrim-start-greater-test", 0, -1).unwrap();
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_ltrim_large_negative_start() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-large-negative-start-test").unwrap();
        let _: () = con.rpush("ltrim-large-negative-start-test", "a").unwrap();
        let _: () = con.rpush("ltrim-large-negative-start-test", "b").unwrap();
        let _: () = con.rpush("ltrim-large-negative-start-test", "c").unwrap();

        // 执行 LTRIM 命令，使用很大的负数作为起始索引
        let result: String = con
            .ltrim("ltrim-large-negative-start-test", -10, -1)
            .unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 应该保留整个列表（因为负数索引超出了范围，会被调整为0）
        let range: Vec<String> = con
            .lrange("ltrim-large-negative-start-test", 0, -1)
            .unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "a");
        assert_eq!(range[1], "b");
        assert_eq!(range[2], "c");
    }

    #[test]
    fn test_ltrim_single_element() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-single-element-test").unwrap();
        let _: () = con.rpush("ltrim-single-element-test", "only").unwrap();

        // 执行 LTRIM 命令，只保留一个元素
        let result: String = con.ltrim("ltrim-single-element-test", 0, 0).unwrap();
        assert_eq!(result, "OK");

        // 验证结果
        let range: Vec<String> = con.lrange("ltrim-single-element-test", 0, -1).unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0], "only");
    }

    #[test]
    fn test_ltrim_stop_exceeds_length() {
        let (_server, mut con) = crate::support::setup_connection();

        // 准备测试数据
        let _: () = con.del("ltrim-stop-exceeds-test").unwrap();
        let _: () = con.rpush("ltrim-stop-exceeds-test", "first").unwrap();
        let _: () = con.rpush("ltrim-stop-exceeds-test", "second").unwrap();
        let _: () = con.rpush("ltrim-stop-exceeds-test", "third").unwrap();

        // 执行 LTRIM 命令，停止索引超过列表长度
        let result: String = con.ltrim("ltrim-stop-exceeds-test", 1, 100).unwrap();
        assert_eq!(result, "OK");

        // 验证结果 - 应该保留从索引1开始到列表末尾的所有元素
        let range: Vec<String> = con.lrange("ltrim-stop-exceeds-test", 0, -1).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], "second");
        assert_eq!(range[1], "third");
    }
}
