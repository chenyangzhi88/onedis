use super::super::*;
use super::support::*;
use tantivy::directory::{Directory, INDEX_WRITER_LOCK};

#[test]
fn alter_runtime_failure_rolls_back_schema_generation_and_runtime() {
    let store = test_store("alter-rollback");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options: FullTextIndexOptions::default(),
        },
    )
    .unwrap();
    db.hash_set("doc:1", "title", "alpha").unwrap();
    let before = db.read_fulltext_meta_direct("idx").unwrap();

    FULLTEXT_ALTER_FAIL_AFTER_SWAP.store(true, AtomicOrdering::SeqCst);
    let error = match db.fulltext_alter("idx", vec![text_field("body")]) {
        Ok(_) => panic!("injected FT.ALTER failure should roll back"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("injected FT.ALTER runtime failure")
    );

    let after = db.read_fulltext_meta_direct("idx").unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.schema.len(), 1);
    assert_eq!(after.schema[0].name, "title");
    assert!(matches!(after.state, FullTextIndexState::Ready));
    assert_eq!(
        db.fulltext_active_storage_name("idx", &after),
        before.active_storage
    );
    assert!(db.fulltext_runtimes.get(0, "idx").is_some());
}

#[test]
fn metadata_cas_rejects_stale_writers_and_sequences_are_strictly_monotonic() {
    let store = test_store("metadata-cas-sequence");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let options = FullTextCreateOptions {
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema: vec![text_field("title")],
        index_options: FullTextIndexOptions::default(),
    };
    db.fulltext_create("idx-1", options.clone()).unwrap();
    db.fulltext_create("idx-2", options).unwrap();
    let first_generation = db.read_fulltext_meta_direct("idx-1").unwrap().generation;
    let second_generation = db.read_fulltext_meta_direct("idx-2").unwrap().generation;
    assert!(second_generation > first_generation);

    let (mut first_writer, expected_raw) = db.read_fulltext_meta_versioned("idx-1").unwrap();
    let mut stale_writer = first_writer.clone();
    first_writer.indexed_docs = 7;
    let mut first_batch = WriteBatch::new();
    db.fulltext_write_meta_cas("idx-1", &expected_raw, &mut first_writer, &mut first_batch)
        .unwrap();
    stale_writer.indexed_docs = 9;
    let mut stale_batch = WriteBatch::new();
    assert!(
        db.fulltext_write_meta_cas("idx-1", &expected_raw, &mut stale_writer, &mut stale_batch,)
            .is_err()
    );
    assert_eq!(
        db.read_fulltext_meta_direct("idx-1").unwrap().indexed_docs,
        7
    );

    db.hash_set("doc:1", "title", "one").unwrap();
    db.hash_set("doc:1", "title", "two").unwrap();
    let sequences = db
        .store
        .scan_prefix_raw(&fulltext_outbox_prefix(0, "idx-1"))
        .into_iter()
        .filter_map(|(key, _)| fulltext_outbox_seq_from_key(0, "idx-1", &key))
        .collect::<Vec<_>>();
    assert_eq!(sequences.len(), 2);
    assert!(sequences[0] < sequences[1]);
    assert!(sequences[0] > second_generation);
}

#[test]
fn ensure_runtime_recovers_writer_lock_left_by_unclean_exit() {
    let store = test_store("stale-writer-lock");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store.clone(), version_counter, ttl_manager);
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options: FullTextIndexOptions::default(),
        },
    )
    .unwrap();
    let meta = db.read_fulltext_meta_direct("idx").unwrap();
    db.fulltext_runtimes.remove(0, "idx");

    let directory = KvTantivyDirectory::new(store, 0, &meta.active_storage);
    let mut stale_lock = directory
        .open_write(INDEX_WRITER_LOCK.filepath.as_path())
        .unwrap();
    std::io::Write::flush(&mut stale_lock).unwrap();
    drop(stale_lock);
    assert!(
        directory
            .exists(INDEX_WRITER_LOCK.filepath.as_path())
            .unwrap()
    );

    db.ensure_fulltext_runtime("idx").unwrap();
    let recovered = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(
        directory
            .exists(INDEX_WRITER_LOCK.filepath.as_path())
            .unwrap(),
        "the recovered runtime must own a live writer lock"
    );
    db.ensure_fulltext_runtime("idx").unwrap();
    let still_loaded = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(
        Arc::ptr_eq(&recovered, &still_loaded),
        "a live runtime must not be replaced while it owns the writer lock"
    );
}

#[test]
fn recreated_index_ignores_late_outbox_records_from_the_dropped_incarnation() {
    let store = test_store("outbox-incarnation");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let mut options = FullTextCreateOptions {
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema: vec![text_field("title")],
        index_options: FullTextIndexOptions::default(),
    };
    db.fulltext_create("idx", options.clone()).unwrap();
    let old_incarnation = db.read_fulltext_meta_direct("idx").unwrap().incarnation;
    db.fulltext_drop_index("idx", false).unwrap();

    db.hash_set("doc:late", "title", "stale-event").unwrap();
    options.index_options.skip_initial_scan = true;
    db.fulltext_create("idx", options).unwrap();
    let new_meta = db.read_fulltext_meta_direct("idx").unwrap();
    assert_ne!(new_meta.incarnation, old_incarnation);

    let sequence = db.next_fulltext_sequence();
    db.store.put_raw(
        &fulltext_outbox_key(0, "idx", sequence),
        &encode_record(&FullTextMutationRecord {
            incarnation: old_incarnation,
            kind: FullTextMutationKind::UpsertKey,
            key: "doc:late".to_string(),
        })
        .unwrap(),
    );
    db.fulltext_refresh_index("idx", true).unwrap();

    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(
        runtime
            .read()
            .unwrap()
            .search(
                "stale-event",
                &search_options(),
                Some(10),
                search_deadline(),
            )
            .unwrap()
            .hits
            .is_empty()
    );
}
