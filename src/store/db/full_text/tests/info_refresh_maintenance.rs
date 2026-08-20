use super::super::*;
use super::support::*;
use crate::store::db::HashSetBatchMutation;

fn lifecycle_test_db(label: &str) -> Db {
    let store = test_store(label);
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn lifecycle_test_options() -> FullTextCreateOptions {
    let index_options = FullTextIndexOptions {
        skip_initial_scan: true,
        ..FullTextIndexOptions::default()
    };
    FullTextCreateOptions {
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema: vec![text_field("title")],
        index_options,
    }
}

#[test]
fn background_publish_defers_durable_checkpoint_until_due_or_forced() {
    let db = lifecycle_test_db("deferred-checkpoint");
    db.fulltext_config_set("REFRESH_INTERVAL_MS", "0").unwrap();
    db.fulltext_config_set("CHECKPOINT_INTERVAL_MS", "60000")
        .unwrap();
    db.fulltext_create("idx", lifecycle_test_options()).unwrap();
    db.hash_set("doc:1", "title", "visible before checkpoint")
        .unwrap();

    db.fulltext_maintenance_tick_mode(false).unwrap();
    let runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    let (published, durable) = {
        let runtime = runtime.read().unwrap();
        (runtime.published_outbox_seq, runtime.durable_outbox_seq)
    };
    assert!(published > durable);
    assert_eq!(
        db.fulltext_collect_live_hits(
            "idx",
            "visible",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap()
        .total,
        1
    );

    // Losing the process-local hot generation before its checkpoint must not
    // lose the document: the durable source/outbox pair is replayed into a new
    // runtime and the forced maintenance checkpoint retires it afterwards.
    db.fulltext_runtimes.remove(0, "idx");
    drop(runtime);
    db.fulltext_maintenance_tick().unwrap();
    let recovered = db.fulltext_runtimes.get(0, "idx").unwrap();
    let runtime = recovered.read().unwrap();
    assert_eq!(runtime.published_outbox_seq, runtime.durable_outbox_seq);
    drop(runtime);
    assert_eq!(
        db.fulltext_collect_live_hits(
            "idx",
            "visible",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap()
        .total,
        1
    );
}

#[tokio::test]
async fn packed_hset_outbox_is_one_record_and_publishes_the_whole_batch() {
    let db = lifecycle_test_db("packed-hset-outbox");
    db.fulltext_config_set("REFRESH_MAX_DOCS", "32").unwrap();
    db.fulltext_create("idx", lifecycle_test_options()).unwrap();
    let keys = (0..200)
        .map(|ordinal| format!("doc:{ordinal}"))
        .collect::<Vec<_>>();
    let mutations = keys
        .iter()
        .map(|key| HashSetBatchMutation {
            key,
            fields: vec![("title", b"packed mutation".as_slice())],
        })
        .collect::<Vec<_>>();

    let replies = db.apply_hash_set_batch_mutations_async(&mutations).await;
    assert!(replies.iter().all(|reply| matches!(reply, Ok(1))));
    let prefix = fulltext_outbox_prefix(0, "idx");
    let outbox = db.store.scan_prefix_raw(&prefix).unwrap();
    assert_eq!(outbox.len(), 1);
    let packed = decode_fulltext_mutation_records(&outbox[0].1).unwrap();
    assert_eq!(packed.len(), 200);
    assert!(packed.iter().all(|record| record.projection.is_none()));

    db.fulltext_maintenance_tick().unwrap();
    assert_eq!(
        db.fulltext_collect_live_hits(
            "idx",
            "packed",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap()
        .total,
        200
    );
}

#[tokio::test]
async fn separated_hash_outbox_carries_only_the_indexed_projection() {
    let db = lifecycle_test_db("separated-hash-projection");
    db.fulltext_create("idx", lifecycle_test_options()).unwrap();
    let padding = vec![b'x'; 9 * 1024];
    let mutation = [HashSetBatchMutation {
        key: "doc:large",
        fields: vec![
            ("title", b"projected mutation".as_slice()),
            ("padding", padding.as_slice()),
        ],
    }];

    let replies = db.apply_hash_set_batch_mutations_async(&mutation).await;
    assert!(matches!(replies.as_slice(), [Ok(2)]));
    let outbox = db
        .store
        .scan_prefix_raw(&fulltext_outbox_prefix(0, "idx"))
        .unwrap();
    assert_eq!(outbox.len(), 1);
    let projected = decode_fulltext_mutation_records(&outbox[0].1).unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(
        projected[0].projection.as_ref().unwrap().fields,
        vec![("title".to_string(), "projected mutation".to_string())]
    );

    db.fulltext_maintenance_tick().unwrap();
    assert_eq!(
        db.fulltext_collect_live_hits(
            "idx",
            "projected",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap()
        .total,
        1
    );
}

#[test]
fn dropped_index_cancels_a_stale_maintenance_snapshot() {
    let db = lifecycle_test_db("dropped-maintenance-snapshot");
    db.fulltext_create("idx", lifecycle_test_options()).unwrap();
    let stale_snapshot = db.read_fulltext_meta_direct("idx").unwrap();

    db.fulltext_drop_index("idx", false).unwrap();

    db.fulltext_maintain_index_snapshot("idx", &stale_snapshot)
        .unwrap();
    db.fulltext_maintenance_tick().unwrap();
    assert!(db.read_fulltext_meta_direct("idx").is_err());
    assert!(db.fulltext_runtimes.get(0, "idx").is_none());
}

#[test]
fn stale_maintenance_cannot_modify_a_recreated_index() {
    let db = lifecycle_test_db("recreated-maintenance-snapshot");
    let options = lifecycle_test_options();
    db.fulltext_create("idx", options.clone()).unwrap();
    let stale_snapshot = db.read_fulltext_meta_direct("idx").unwrap();
    db.fulltext_drop_index("idx", false).unwrap();

    db.fulltext_create("idx", options).unwrap();
    db.hash_set("doc:1", "title", "new incarnation").unwrap();
    let recreated_meta = db.read_fulltext_meta_direct("idx").unwrap();
    let recreated_runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert_ne!(recreated_meta.incarnation, stale_snapshot.incarnation);

    db.fulltext_maintain_index_snapshot("idx", &stale_snapshot)
        .unwrap();

    let current_meta = db.read_fulltext_meta_direct("idx").unwrap();
    let current_runtime = db.fulltext_runtimes.get(0, "idx").unwrap();
    assert_eq!(current_meta.incarnation, recreated_meta.incarnation);
    assert!(matches!(current_meta.state, FullTextIndexState::Ready));
    assert!(Arc::ptr_eq(&current_runtime, &recreated_runtime));
    assert_eq!(
        current_runtime.read().unwrap().incarnation,
        recreated_meta.incarnation
    );

    db.fulltext_maintenance_tick().unwrap();
    let hits = db
        .fulltext_collect_live_hits(
            "idx",
            "incarnation",
            &search_options(),
            FullTextCollectMode::Page,
        )
        .unwrap();
    assert_eq!(hits.total, 1);
    db.fulltext_info("idx").unwrap();
}

#[test]
fn same_process_drop_recreate_search_and_info_remain_available() {
    let db = lifecycle_test_db("drop-recreate-loop");
    let options = lifecycle_test_options();

    for iteration in 0..8 {
        db.fulltext_create("idx", options.clone()).unwrap();
        db.hash_set(
            "doc:current",
            "title",
            &format!("lifecycle generation {iteration}"),
        )
        .unwrap();
        db.fulltext_maintenance_tick().unwrap();

        let hits = db
            .fulltext_collect_live_hits(
                "idx",
                &iteration.to_string(),
                &search_options(),
                FullTextCollectMode::Page,
            )
            .unwrap();
        assert_eq!(hits.total, 1);
        db.fulltext_info("idx").unwrap();

        let stale_snapshot = db.read_fulltext_meta_direct("idx").unwrap();
        db.fulltext_drop_index("idx", false).unwrap();
        db.fulltext_maintain_index_snapshot("idx", &stale_snapshot)
            .unwrap();
    }
}
