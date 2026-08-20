use super::super::*;
use super::support::*;
use tantivy::directory::{Directory, INDEX_WRITER_LOCK};

#[test]
fn fulltext_vector_indexes_are_internal_and_not_redis_keys() {
    let store = test_store("internal-vector-namespace");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let mut vector = field("vec", FullTextFieldKind::Vector);
    vector.options.vector = Some(vector_options());
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![vector],
            index_options: FullTextIndexOptions::default(),
        },
    )
    .unwrap();
    let meta = db.read_fulltext_meta_direct("idx").unwrap();
    let internal = fulltext_vector_index_name("idx", meta.generation, "vec");
    assert_eq!(db.vector_dim(&internal).unwrap(), Some(3));
    assert!(!db.keys("*").unwrap().contains(&internal));
    assert!(!db.delete_key(&internal).unwrap());
    assert_eq!(db.vector_dim(&internal).unwrap(), Some(3));

    db.hash_set("doc:1", "vec", "[1,0,0]").unwrap();
    db.fulltext_maintenance_tick().unwrap();
    assert_eq!(db.vector_card(&internal).unwrap(), 1);

    db.fulltext_drop_index("idx", false).unwrap();
    assert_eq!(db.vector_dim(&internal).unwrap(), None);
}

#[test]
fn drop_index_dd_deletes_multiple_source_pages() {
    let store = test_store("drop-index-dd-pages");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    let options = FullTextIndexOptions {
        skip_initial_scan: true,
        ..FullTextIndexOptions::default()
    };
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options: options,
        },
    )
    .unwrap();
    for ordinal in 0..300 {
        db.hash_set(&format!("doc:{ordinal}"), "title", "batch delete")
            .unwrap();
    }
    db.hash_set("keep:1", "title", "unrelated").unwrap();

    db.fulltext_drop_index("idx", true).unwrap();

    assert!(db.fulltext_list().is_ok());
    assert!(db.hash_get("doc:0", "title").unwrap().is_none());
    assert!(db.hash_get("doc:299", "title").unwrap().is_none());
    assert_eq!(
        db.hash_get("keep:1", "title").unwrap().as_deref(),
        Some("unrelated")
    );
}

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
        .unwrap()
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
fn concurrent_cold_runtime_initialization_publishes_one_runtime() {
    let store = test_store("concurrent-cold-runtime");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Arc::new(Db::new(0, store, version_counter, ttl_manager));
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
    db.fulltext_runtimes.remove(0, "idx");

    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                db.ensure_fulltext_runtime("idx").unwrap();
                db.fulltext_runtimes.get(0, "idx").unwrap()
            })
        })
        .collect::<Vec<_>>();
    let runtimes = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert!(
        runtimes
            .iter()
            .skip(1)
            .all(|runtime| Arc::ptr_eq(&runtimes[0], runtime))
    );
}

#[test]
fn runtime_registry_prunes_dead_per_index_locks() {
    let registry = FullTextRuntimeRegistry::default();
    for id in 0..128 {
        drop(registry.lifecycle_lock(0, &format!("lifecycle-{id}")));
        drop(registry.refresh_lock(0, &format!("refresh-{id}")));
    }

    let lifecycle = registry.lifecycle_lock(0, "live");
    let refresh = registry.refresh_lock(0, "live");
    assert!(registry.lifecycle_locks.len() <= 64);
    assert!(registry.refresh_locks.len() <= 64);
    drop(lifecycle);
    drop(refresh);

    drop(registry.lifecycle_lock(0, "next"));
    drop(registry.refresh_lock(0, "next"));
    assert!(registry.lifecycle_locks.len() <= 64);
    assert!(registry.refresh_locks.len() <= 64);
}

#[test]
fn maintenance_rebuilds_an_incompatible_persisted_runtime_before_search() {
    let store = test_store("legacy-runtime-schema");
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
    db.hash_set("doc:1", "title", "schema migration").unwrap();
    let meta = db.read_fulltext_meta_direct("idx").unwrap();
    db.fulltext_runtimes.remove(0, "idx");
    let mut cleanup = WriteBatch::new();
    db.delete_fulltext_storage_to_batch(&mut cleanup, &meta.active_storage)
        .unwrap();
    db.write_batch_if_not_empty(&cleanup).unwrap();

    let mut legacy_schema = Schema::builder();
    legacy_schema.add_text_field(FULLTEXT_KEY_FIELD, STRING | STORED);
    legacy_schema.add_text_field("title", TextOptions::default());
    let directory = KvTantivyDirectory::new(store, 0, &meta.active_storage);
    let legacy = Index::open_or_create(directory, legacy_schema.build()).unwrap();
    let mut writer = legacy
        .writer::<TantivyDocument>(FULLTEXT_WRITER_HEAP_BYTES)
        .unwrap();
    writer.commit().unwrap();
    drop(writer);
    drop(legacy);

    db.fulltext_maintenance_tick().unwrap();

    let hits = db
        .fulltext_collect_live_hits(
            "idx",
            "migration",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap();
    assert_eq!(hits.total, 1);
    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert!(
        runtime
            .read()
            .unwrap()
            .index
            .schema()
            .get_field(FULLTEXT_EXPIRES_AT_FIELD)
            .is_ok()
    );
    let schema = runtime.read().unwrap().index.schema();
    let key_field = schema.get_field(FULLTEXT_KEY_FIELD).unwrap();
    assert!(schema.get_field_entry(key_field).is_fast());
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
    db.store
        .put_raw(
            &fulltext_outbox_key(0, "idx", sequence),
            &encode_record(&FullTextMutationRecord {
                incarnation: old_incarnation,
                kind: FullTextMutationKind::UpsertKey,
                key: "doc:late".to_string(),
                projection: None,
            })
            .unwrap(),
        )
        .unwrap();
    db.fulltext_maintenance_tick().unwrap();

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

#[test]
fn maintenance_publishes_and_checkpoints_before_search_reads_the_generation() {
    let store = test_store("hot-checkpoint");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store.clone(), version_counter, ttl_manager);
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
    db.hash_set("doc:1", "title", "near realtime").unwrap();
    assert!(
        !db.store
            .scan_prefix_raw(&fulltext_outbox_prefix(0, "idx"))
            .unwrap()
            .is_empty(),
        "the source mutation must be durable before query-side publication"
    );

    db.fulltext_maintenance_tick().unwrap();
    let checkpointed = db.read_fulltext_meta_direct("idx").unwrap();
    assert!(checkpointed.last_indexed_outbox_seq > 0);
    assert!(
        db.store
            .scan_prefix_raw(&fulltext_outbox_prefix(0, "idx"))
            .unwrap()
            .is_empty()
    );

    let hits = db
        .fulltext_collect_live_hits(
            "idx",
            "realtime",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap();
    assert_eq!(hits.total, 1);
    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    let published = runtime.read().unwrap().published_outbox_seq();
    assert_eq!(checkpointed.last_indexed_outbox_seq, published);

    drop(runtime);
    db.fulltext_runtimes.remove(0, "idx");
    db.ensure_fulltext_runtime("idx").unwrap();
    let recovered = db.fulltext_runtimes.get(0, "idx").unwrap();
    let recovered_hits = recovered
        .read()
        .unwrap()
        .search("realtime", &search_options(), Some(10), search_deadline())
        .unwrap();
    assert_eq!(recovered_hits.hits.len(), 1);
}

#[test]
fn search_materializes_only_the_requested_page_and_nocontent_skips_source_fields() {
    let store = test_store("page-materialization");
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
            schema: vec![
                text_field("title"),
                field("price", FullTextFieldKind::Numeric),
            ],
            index_options,
        },
    )
    .unwrap();
    for ordinal in 0..30 {
        let key = format!("doc:{ordinal:02}");
        db.hash_set(&key, "title", "common page materialization")
            .unwrap();
        db.hash_set(&key, "price", &ordinal.to_string()).unwrap();
    }
    db.fulltext_maintenance_tick().unwrap();

    let mut options = search_options();
    options.offset = 20;
    options.limit = 5;
    let content = db
        .fulltext_collect_live_hits("idx", "common", &options, FullTextCollectMode::Page)
        .unwrap();
    assert_eq!(content.total, 30);
    assert_eq!(content.hits.len(), 5);
    assert!(content.page_offset_applied);
    assert!(content.hits.iter().all(|hit| !hit.fields.is_empty()));

    options.return_fields = Some(vec![FullTextReturnField {
        identifier: "title".to_string(),
        alias: Some("headline".to_string()),
    }]);
    let projected = db
        .fulltext_collect_live_hits("idx", "common", &options, FullTextCollectMode::Page)
        .unwrap();
    assert!(projected.hits.iter().all(|hit| hit.fields
        == vec![(
            "title".to_string(),
            "common page materialization".to_string()
        )]));

    options.no_content = true;
    options.return_fields = None;
    let no_content = db
        .fulltext_collect_live_hits("idx", "common", &options, FullTextCollectMode::Page)
        .unwrap();
    assert_eq!(no_content.total, 30);
    assert_eq!(no_content.hits.len(), 5);
    assert!(no_content.page_offset_applied);
    assert!(no_content.hits.iter().all(|hit| hit.fields.is_empty()));

    options.filters.push(FullTextSearchNumericFilter {
        field: "price".to_string(),
        min: FullTextSearchBound::Inclusive(20.0),
        max: FullTextSearchBound::Inclusive(29.0),
    });
    options.offset = 0;
    let filtered = db
        .fulltext_collect_live_hits("idx", "common", &options, FullTextCollectMode::Page)
        .unwrap();
    assert_eq!(filtered.total, 10);
    assert_eq!(filtered.hits.len(), 5);
    assert!(filtered.page_offset_applied);
}
