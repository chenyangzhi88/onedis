#![cfg(feature = "tcp-integration-tests")]

mod support;

#[cfg(test)]
mod tests {
    use redis::Commands;

    #[test]
    fn test_hash_basic_commands() {
        let (_server, mut con) = crate::support::setup_connection();
        let key = "hash-basic-test";

        let _: () = con.del(key).unwrap();

        let created: i32 = con.hset(key, "name", "alice").unwrap();
        assert_eq!(created, 1);

        let overwritten: i32 = con.hset(key, "name", "bob").unwrap();
        assert_eq!(overwritten, 0);

        let value: String = con.hget(key, "name").unwrap();
        assert_eq!(value, "bob");

        let missing: Option<String> = con.hget(key, "missing").unwrap();
        assert_eq!(missing, None);

        let exists: i32 = con.hexists(key, "name").unwrap();
        assert_eq!(exists, 1);

        let missing_exists: i32 = con.hexists(key, "missing").unwrap();
        assert_eq!(missing_exists, 0);

        let len: i32 = redis::cmd("HLEN").arg(key).query(&mut con).unwrap();
        assert_eq!(len, 1);

        let strlen: i32 = redis::cmd("HSTRLEN")
            .arg(key)
            .arg("name")
            .query(&mut con)
            .unwrap();
        assert_eq!(strlen, 3);

        let _: () = con.del(key).unwrap();
    }

    #[test]
    fn test_hash_multi_field_commands() {
        let (_server, mut con) = crate::support::setup_connection();
        let key = "hash-multi-test";

        let _: () = con.del(key).unwrap();
        let _: () = redis::cmd("HMSET")
            .arg(key)
            .arg("name")
            .arg("alice")
            .arg("age")
            .arg("30")
            .arg("city")
            .arg("paris")
            .query(&mut con)
            .unwrap();

        let values = crate::support::hmget(&mut con, key, &["name", "age", "missing"]).unwrap();
        assert_eq!(
            values,
            vec![Some("alice".to_string()), Some("30".to_string()), None,]
        );

        let mut all = crate::support::hgetall(&mut con, key).unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![
                "30".to_string(),
                "age".to_string(),
                "alice".to_string(),
                "city".to_string(),
                "name".to_string(),
                "paris".to_string(),
            ]
        );

        let mut keys = crate::support::hkeys(&mut con, key).unwrap();
        keys.sort();
        assert_eq!(keys, vec!["age", "city", "name"]);

        let mut vals = crate::support::hvals(&mut con, key).unwrap();
        vals.sort();
        assert_eq!(vals, vec!["30", "alice", "paris"]);

        let _: () = con.del(key).unwrap();
    }

    #[test]
    fn test_hsetnx_and_hdel_remove_empty_key() {
        let (_server, mut con) = crate::support::setup_connection();
        let key = "hash-delete-test";

        let _: () = con.del(key).unwrap();

        let created = crate::support::hsetnx(&mut con, key, "field1", "v1").unwrap();
        assert_eq!(created, 1);

        let not_created = crate::support::hsetnx(&mut con, key, "field1", "v2").unwrap();
        assert_eq!(not_created, 0);

        let deleted: i32 = redis::cmd("HDEL")
            .arg(key)
            .arg("field1")
            .query(&mut con)
            .unwrap();
        assert_eq!(deleted, 1);

        let exists: bool = con.exists(key).unwrap();
        assert!(!exists);

        let all = crate::support::hgetall(&mut con, key).unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn test_hscan_returns_field_value_pairs_with_match_and_cursor() {
        let (_server, mut con) = crate::support::setup_connection();
        let key = "hash-scan-test";

        let _: () = con.del(key).unwrap();
        let _: () = redis::cmd("HMSET")
            .arg(key)
            .arg("name")
            .arg("alice")
            .arg("nickname")
            .arg("ally")
            .arg("city")
            .arg("paris")
            .query(&mut con)
            .unwrap();

        let (cursor, first_page) = crate::support::hscan(&mut con, key, 0, None, Some(2)).unwrap();
        assert!(cursor >= 0);
        assert!(!first_page.is_empty());
        assert_eq!(first_page.len() % 2, 0);

        let (done_cursor, matched) =
            crate::support::hscan(&mut con, key, 0, Some("*name*"), Some(10)).unwrap();
        assert_eq!(done_cursor, 0);
        assert_eq!(
            matched,
            vec![
                "name".to_string(),
                "alice".to_string(),
                "nickname".to_string(),
                "ally".to_string(),
            ]
        );

        let _: () = con.del(key).unwrap();
    }
}
