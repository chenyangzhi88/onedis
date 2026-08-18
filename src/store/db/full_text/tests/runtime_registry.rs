use super::super::*;
use super::support::*;

#[test]
fn runtime_removal_is_fenced_by_index_incarnation() {
    let store = test_store("runtime-incarnation-fence");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let mut index_options = FullTextIndexOptions::default();
    index_options.skip_initial_scan = true;
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
