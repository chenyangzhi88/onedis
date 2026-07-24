use super::super::*;
use super::support::{meta, test_store};

#[test]
fn source_scan_pages_are_bounded_sorted_and_deduplicate_overlapping_prefixes() {
    let store = test_store("source-pages");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    for index in 0..600 {
        db.hash_set(
            &format!("doc:{index:04}"),
            "title",
            &format!("value-{index}"),
        )
        .unwrap();
    }

    let mut source_meta = meta(vec![]);
    source_meta.prefixes = vec!["doc:".to_string(), "doc:0".to_string()];
    let mut cursor = None;
    let mut keys = Vec::new();
    loop {
        let (page, has_more) = db
            .fulltext_source_keys_page(&source_meta, cursor.as_deref(), 73)
            .unwrap();
        assert!(page.len() <= 73);
        assert!(page.windows(2).all(|pair| pair[0] < pair[1]));
        if let Some(previous) = cursor.as_ref() {
            assert!(page.first().is_none_or(|key| key > previous));
        }
        cursor = page.last().cloned();
        keys.extend(page);
        if !has_more {
            break;
        }
        assert!(cursor.is_some());
    }
    assert_eq!(keys.len(), 600);
    assert_eq!(keys.first().map(String::as_str), Some("doc:0000"));
    assert_eq!(keys.last().map(String::as_str), Some("doc:0599"));
}

#[test]
fn source_scan_reports_more_when_the_raw_tail_exceeds_the_logical_page() {
    let store = test_store("source-small-pages");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    for index in 0..40 {
        db.hash_set(&format!("doc:{index:02}"), "title", "value")
            .unwrap();
    }

    let mut source_meta = meta(vec![]);
    source_meta.prefixes = vec!["doc:".to_string()];
    let mut cursor = None;
    let mut keys = Vec::new();
    loop {
        let (page, has_more) = db
            .fulltext_source_keys_page(&source_meta, cursor.as_deref(), 7)
            .unwrap();
        assert!(page.len() <= 7);
        cursor = page.last().cloned();
        keys.extend(page);
        if !has_more {
            break;
        }
    }
    assert_eq!(keys.len(), 40);
}
