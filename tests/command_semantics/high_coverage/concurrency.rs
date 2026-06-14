#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_command_paths_preserve_counts_and_values() {
    let db = Arc::new(test_db("command-semantics-concurrent"));

    let mut tasks = Vec::new();
    for shard in 0..8 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..64 {
                let key = format!("counter:{}", shard);
                let field = format!("field:{}", i % 8);
                let list = format!("list:{}", shard);
                let set_result =
                    apply_async(&db, &["SET", &format!("k:{}:{}", shard, i), "v"]).await;
                assert!(matches!(set_result, Frame::Ok));
                assert!(matches!(
                    apply_async(&db, &["INCR", &key]).await,
                    Frame::Integer(_)
                ));
                assert!(matches!(
                    apply_async(&db, &["HINCRBY", "hash", &field, "1"]).await,
                    Frame::Integer(_)
                ));
                assert!(matches!(
                    apply_async(&db, &["LPUSH", &list, &i.to_string()]).await,
                    Frame::Integer(_)
                ));
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    for shard in 0..8 {
        assert!(matches!(
            apply(&db, &["GET", &format!("counter:{}", shard)]),
            Frame::BulkString(bytes) if bytes == b"64"
        ));
        assert!(matches!(
            apply(&db, &["LLEN", &format!("list:{}", shard)]),
            Frame::Integer(64)
        ));
    }
    assert!(matches!(apply(&db, &["HLEN", "hash"]), Frame::Integer(8)));
    assert!(matches!(apply(&db, &["DBSIZE"]), Frame::Integer(size) if size >= 25));
}


