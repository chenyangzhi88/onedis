use super::super::*;
use super::support::*;

#[test]
fn runtime_removal_is_fenced_by_index_incarnation() {
    let store = test_store("runtime-incarnation-fence");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let index_options = FullTextIndexOptions {
        skip_initial_scan: true,
        ..FullTextIndexOptions::default()
    };
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options,
        },
    )
    .unwrap();

    let meta = db.read_fulltext_meta_direct("idx").unwrap();
    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(!db.fulltext_runtimes.remove_if_incarnation(
        0,
        "idx",
        meta.incarnation.saturating_add(1)
    ));
    let retained = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(Arc::ptr_eq(&runtime, &retained));

    assert!(
        db.fulltext_runtimes
            .remove_if_incarnation(0, "idx", meta.incarnation)
    );
    assert!(db.fulltext_runtimes.get(0, "idx").is_none());
}

#[test]
fn immutable_search_generation_does_not_hold_the_writer_lock_and_is_retired_on_drop() {
    let store = test_store("immutable-generation");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let index_options = FullTextIndexOptions {
        skip_initial_scan: true,
        ..FullTextIndexOptions::default()
    };
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options,
        },
    )
    .unwrap();
    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    let generation = db
        .fulltext_runtimes
        .get_search_generation(0, "idx")
        .unwrap();

    assert!(runtime.try_write().is_ok());
    assert!(generation.ensure_active().is_ok());
    db.fulltext_runtimes.remove(0, "idx");
    assert!(generation.ensure_active().is_err());
}

#[test]
fn progress_signal_wakes_waiters_and_observes_deadlines() {
    let signal = Arc::new(FullTextProgressSignal::default());
    let observed = signal.generation().unwrap();
    let waiter = Arc::clone(&signal);
    let thread = std::thread::spawn(move || {
        waiter
            .wait_for_change(observed, Instant::now() + Duration::from_secs(1))
            .unwrap()
    });
    signal.notify();
    assert!(thread.join().unwrap());

    let observed = signal.generation().unwrap();
    assert!(!signal.wait_for_change(observed, Instant::now()).unwrap());
}

#[test]
fn query_cache_evicts_incrementally_instead_of_clearing_every_entry() {
    let registry = FullTextRuntimeRegistry::default();
    for ordinal in 0..=4_096 {
        registry
            .query_ast(0, "idx", 1, 2, &format!("term{ordinal}"))
            .unwrap();
    }
    assert_eq!(registry.query_asts.len(), 4_096);
    assert!(registry.query_ast(0, "idx", 1, 2, "term4096").is_ok());
    assert_eq!(registry.query_asts.len(), 4_096);
}
