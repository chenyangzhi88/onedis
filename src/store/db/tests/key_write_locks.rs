use super::*;

#[test]
fn db_and_ttl_manager_share_the_authoritative_key_lock_pool() {
    let db = test_db();
    let ttl_locks = db.ttl_manager.key_write_locks();

    assert_eq!(
        db.key_write_locks.len(),
        crate::store::key_write_locks::KEY_WRITE_LOCK_SHARDS
    );
    assert_eq!(db.key_write_locks.len(), 1 << 16);
    assert!(Arc::ptr_eq(&db.key_write_locks, &ttl_locks));
}

#[test]
fn lock_shards_include_the_database_index_and_are_stable() {
    let db0 = crate::store::key_write_locks::key_write_lock_shard(0, "shared-key");
    let db0_again = crate::store::key_write_locks::key_write_lock_shard(0, "shared-key");
    let db1 = crate::store::key_write_locks::key_write_lock_shard(1, "shared-key");

    assert_eq!(db0, db0_again);
    assert_ne!(db0, db1);
}

#[tokio::test]
async fn transaction_commit_waits_for_a_shared_hash_structural_guard() {
    let db = test_db();
    let txn_db = db.transactional_view().unwrap();
    assert!(
        txn_db
            .hash_set_async("commit-barrier-hash", "field", "value")
            .await
            .unwrap()
    );

    let read_guard = db.set_write_lock("commit-barrier-hash").read().await;
    let mut commit = tokio::spawn(async move { txn_db.commit_transaction_async().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut commit)
            .await
            .is_err()
    );

    drop(read_guard);
    commit.await.unwrap().unwrap();
    assert_eq!(
        db.hash_get_async("commit-barrier-hash", "field")
            .await
            .unwrap(),
        Some("value".to_string())
    );
}
