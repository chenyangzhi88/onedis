use super::{
    Db, ExpireCondition, HASH_FIELD_NAMESPACE, JSON_INDEXED_MARKER, JSON_NODE_NAMESPACE,
    LIST_ITEM_NAMESPACE, SET_MEMBER_NAMESPACE, SET_MEMBER_SLOT_NAMESPACE, SET_SLOT_NAMESPACE,
    STREAM_CONSUMER_NAMESPACE, STREAM_ENTRY_NAMESPACE, STREAM_GROUP_NAMESPACE,
    STREAM_PEL_NAMESPACE, SetCondition, SetExpiration, SetOutcome, StreamEntry, StreamId,
    StreamReadGroupStart, StreamReadStart, StringExpireUpdate, Structure, TYPE_HASH, TYPE_JSON,
    TYPE_LIST, TYPE_SET, TYPE_SORTED_SET, TYPE_STREAM, TYPE_VECTOR, VECTOR_DOC_NAMESPACE,
    VECTOR_GRAPH_NAMESPACE, VECTOR_META_NAMESPACE, VECTOR_NUMERIC_NAMESPACE,
    VECTOR_SEGMENT_NAMESPACE, VECTOR_TAG_NAMESPACE, WRONG_TYPE_ERROR, ZSET_MEMBER_NAMESPACE,
    ZSET_RANK_NAMESPACE, ZsetAggregate, db_prefix, db_prefix_exclusive_upper_bound,
    decode_db_prefix, decode_entry, delete_sub_keys_to_batch_bytes, hash_field_key,
    internal_prefix, is_known_subkey_namespace, json_node_key, json_node_prefix, main_key,
    main_key_bytes, now_ms, parse_json_path, prefix_exclusive_upper_bound, set_slot_key,
    sub_key_range_end_bytes, sub_key_range_start_bytes,
};
use crate::cmds::json::JsonSet;
use crate::cmds::string::set::Set;
use crate::command::Command;
use crate::store::kv_store::KvStore;
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

mod hash;
mod json;
mod key_string_bitmap;
mod keyspace_copy;
mod native_hash;
mod native_list;
mod native_set_zset;
mod set_list_async;
mod stream_group;
mod stream_string_batch;
mod transactions;
mod zset;
