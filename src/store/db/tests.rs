use super::{
    Db, DbKeyRef, ExpireCondition, HASH_FIELD_NAMESPACE, JSON_INDEXED_MARKER, JSON_NODE_NAMESPACE,
    KEY_ENCODING_LAYOUT_META_KEY, KeyEncodingLayout, KeyExpirationBatchMutation,
    KeyMutationTracker, LIST_ITEM_NAMESPACE, ListMeta, SET_MEMBER_NAMESPACE,
    STREAM_CONSUMER_NAMESPACE, STREAM_ENTRY_NAMESPACE, STREAM_GROUP_NAMESPACE,
    STREAM_PEL_NAMESPACE, SetBatchMutation, SetCondition, SetExpiration, SetOutcome, StreamEntry,
    StreamId, StreamReadGroupStart, StreamReadStart, StringBatchMutation, StringBatchReply,
    StringExpireUpdate, Structure, TYPE_HASH, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_SORTED_SET,
    TYPE_STREAM, TYPE_VECTOR, VECTOR_DOC_NAMESPACE, VECTOR_GRAPH_NAMESPACE, VECTOR_META_NAMESPACE,
    VECTOR_NUMERIC_NAMESPACE, VECTOR_SEGMENT_NAMESPACE, VECTOR_TAG_NAMESPACE, WRONG_TYPE_ERROR,
    ZSET_MEMBER_NAMESPACE, ZSET_RANK_NAMESPACE, ZsetAggregate, ZsetScoreWindow, db_prefix,
    db_prefix_exclusive_upper_bound, decode_db_prefix, decode_entry,
    delete_sub_keys_to_batch_bytes, encode_set_meta, hash_field_expire_key, hash_field_key,
    hash_field_prefix, internal_prefix, is_known_subkey_namespace, json_node_key, json_node_prefix,
    main_key, main_key_bytes, now_ms, parse_json_path, prefix_exclusive_upper_bound,
    set_member_prefix, sub_key_range_end_bytes, sub_key_range_start_bytes, version_owner_prefix,
    write_json_subtree_to_batch,
};
use crate::cmds::json::JsonSet;
use crate::cmds::string::set::Set;
use crate::command::Command;
use crate::store::kv_store::{CompareCondition, KvStore};
use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter, decode_meta_header};
use common::types::write_batch::WriteBatch;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::{thread::sleep, time::Duration};

fn test_root(prefix: &str) -> std::path::PathBuf {
    let unique = format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique)
}

fn test_db() -> Db {
    let root = test_root("onedis-db-test");
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

#[test]
fn recovered_nonempty_db_table_initializes_missing_table_local_layout() {
    let root = test_root("onedis-layout-migration-test");
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1).for_db_index(7);
    store.put_raw(b"legacy-key", b"legacy-value").unwrap();

    assert_eq!(store.get_raw(KEY_ENCODING_LAYOUT_META_KEY).unwrap(), None);
    assert_eq!(
        KeyEncodingLayout::open_or_initialize_for_table(&store),
        KeyEncodingLayout::TableLocalV2
    );
    assert_eq!(
        store
            .get_raw(KEY_ENCODING_LAYOUT_META_KEY)
            .unwrap()
            .as_deref(),
        Some(b"table-local-v2".as_slice())
    );
    assert_eq!(
        store.get_raw(b"legacy-key").unwrap().as_deref(),
        Some(b"legacy-value".as_slice())
    );
}

#[test]
fn nonempty_non_db_table_without_layout_still_fails_closed() {
    let root = test_root("onedis-layout-nondb-test");
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1).for_table("legacy_custom_table");
    store.put_raw(b"legacy-key", b"legacy-value").unwrap();

    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            KeyEncodingLayout::open_or_initialize_for_table(&store)
        }))
        .is_err()
    );
    assert_eq!(store.get_raw(KEY_ENCODING_LAYOUT_META_KEY).unwrap(), None);
}

mod full_text_directory;
mod hash;
mod json;
mod key_string_bitmap;
mod key_write_locks;
mod keyspace_copy;
mod native_hash;
mod native_list;
mod native_set_zset;
mod set_list_async;
mod stream_group;
mod stream_string_batch;
mod string_integer;
mod transactions;
mod zset;
