use super::*;

#[test]
fn transaction_view_uses_repeatable_read_snapshot() {
    let db = test_db();

    crate::command_dispatch::handle_command_autocommit(
        &db,
        Command::Set(Set::new("rr-key".to_string(), "v1".to_string(), None)),
    )
    .unwrap();

    let txn_db = db.transactional_view().unwrap();
    assert!(matches!(
        txn_db.get("rr-key"),
        Some(Structure::String(value)) if value == "v1"
    ));

    crate::command_dispatch::handle_command_autocommit(
        &db,
        Command::Set(Set::new("rr-key".to_string(), "v2".to_string(), None)),
    )
    .unwrap();

    assert!(matches!(
        txn_db.get("rr-key"),
        Some(Structure::String(value)) if value == "v1"
    ));
    txn_db.commit_transaction().unwrap();

    assert!(matches!(
        db.get("rr-key"),
        Some(Structure::String(value)) if value == "v2"
    ));
}

#[test]
fn long_transaction_commit_conflicts_with_autocommit_write() {
    let db = test_db();

    crate::command_dispatch::handle_command_autocommit(
        &db,
        Command::Set(Set::new("conflict-key".to_string(), "v1".to_string(), None)),
    )
    .unwrap();

    let txn_db = db.transactional_view().unwrap();
    assert!(matches!(
        txn_db.get("conflict-key"),
        Some(Structure::String(value)) if value == "v1"
    ));

    crate::command_dispatch::handle_command_autocommit(
        &db,
        Command::Set(Set::new("conflict-key".to_string(), "v2".to_string(), None)),
    )
    .unwrap();

    txn_db.update(
        "conflict-key".to_string(),
        Structure::String("v3".to_string()),
    );

    assert!(txn_db.commit_transaction().is_err());
    assert!(matches!(
        db.get("conflict-key"),
        Some(Structure::String(value)) if value == "v2"
    ));
}

#[test]
fn long_transaction_commit_conflicts_with_direct_write() {
    let db = test_db();

    crate::command_dispatch::handle_command(
        &db,
        Command::Set(Set::new(
            "direct-conflict-key".to_string(),
            "v1".to_string(),
            None,
        )),
    )
    .unwrap();

    let txn_db = db.transactional_view().unwrap();
    assert!(matches!(
        txn_db.get("direct-conflict-key"),
        Some(Structure::String(value)) if value == "v1"
    ));

    crate::command_dispatch::handle_command(
        &db,
        Command::Set(Set::new(
            "direct-conflict-key".to_string(),
            "v2".to_string(),
            None,
        )),
    )
    .unwrap();

    txn_db.update(
        "direct-conflict-key".to_string(),
        Structure::String("v3".to_string()),
    );

    assert!(txn_db.commit_transaction().is_err());
    assert!(matches!(
        db.get("direct-conflict-key"),
        Some(Structure::String(value)) if value == "v2"
    ));
}

#[test]
fn set_over_complex_type_hides_old_subkeys_and_gc_cleans_after_rebuild() {
    let db = test_db();

    assert!(db.hash_set("reuse-key", "old-field", "old-value").unwrap());
    let old_version = db.version_counter.current();
    let old_field_key = hash_field_key(db.db_index, "reuse-key", old_version, "old-field");
    assert!(db.store.contains_key(&old_field_key));

    db.insert_string("reuse-key".to_string(), "plain-string".to_string(), None);
    assert!(db.store.contains_key(&old_field_key));
    assert!(matches!(
        db.get("reuse-key"),
        Some(Structure::String(value)) if value == "plain-string"
    ));

    let rebuilt_ttl = TtlManager::new(db.store.clone(), TtlConfig::default());
    let rebuilt_counter = Arc::new(VersionCounter::new());
    rebuilt_ttl.rebuild_from_store(1, &rebuilt_counter);
    assert!(rebuilt_counter.current() >= old_version);

    let rebuilt_db = Db::new(db.db_index, db.store.clone(), rebuilt_counter, rebuilt_ttl);
    assert!(matches!(
        rebuilt_db.remove("reuse-key"),
        Some(Structure::String(value)) if value == "plain-string"
    ));
    assert!(
        rebuilt_db
            .hash_set("reuse-key", "new-field", "new-value")
            .unwrap()
    );
    assert_eq!(rebuilt_db.hash_get("reuse-key", "old-field").unwrap(), None);
    assert_eq!(rebuilt_db.retired_version_gc_once(usize::MAX), 1);
    assert!(!rebuilt_db.store.contains_key(&old_field_key));

    let fields: HashMap<_, _> = rebuilt_db
        .hash_get_all("reuse-key")
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(fields.get("old-field"), None);
    assert_eq!(fields.get("new-field"), Some(&"new-value".to_string()));
}
