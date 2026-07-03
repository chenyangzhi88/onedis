    use super::{
        Db, ExpireCondition, HASH_FIELD_NAMESPACE, JSON_NODE_NAMESPACE, LIST_ITEM_NAMESPACE,
        SET_MEMBER_NAMESPACE, SET_MEMBER_SLOT_NAMESPACE, SET_SLOT_NAMESPACE,
        STREAM_CONSUMER_NAMESPACE, STREAM_ENTRY_NAMESPACE, STREAM_GROUP_NAMESPACE,
        STREAM_PEL_NAMESPACE, SetCondition, SetExpiration, SetOutcome, StreamEntry, StreamId,
        StreamReadGroupStart, StreamReadStart, StringExpireUpdate, Structure, TYPE_HASH, TYPE_JSON,
        TYPE_LIST, TYPE_SET, TYPE_SORTED_SET, TYPE_STREAM, TYPE_VECTOR, VECTOR_DOC_NAMESPACE,
        VECTOR_GRAPH_NAMESPACE, VECTOR_META_NAMESPACE, VECTOR_NUMERIC_NAMESPACE,
        VECTOR_SEGMENT_NAMESPACE, VECTOR_TAG_NAMESPACE, WRONG_TYPE_ERROR, ZSET_MEMBER_NAMESPACE,
        ZSET_RANK_NAMESPACE, ZsetAggregate, db_prefix, internal_prefix, db_prefix_exclusive_upper_bound,
        decode_db_prefix, decode_entry, delete_sub_keys_to_batch_bytes, hash_field_key,
        is_known_subkey_namespace, json_node_key, json_node_prefix, main_key, main_key_bytes,
        now_ms, parse_json_path, prefix_exclusive_upper_bound, set_slot_key,
        sub_key_range_end_bytes, sub_key_range_start_bytes, JSON_INDEXED_MARKER,
    };
    use crate::cmds::json::JsonSet;
    use crate::cmds::string::set::Set;
    use crate::command::Command;
    use crate::store::kv_store::KvStore;
    use crate::store::ttl::{decode_meta_header, TtlConfig, TtlManager, VersionCounter};
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


    mod key_string_bitmap {
        use super::*;
        include!("tests/key_string_bitmap.rs");
    }

    mod hash {
        use super::*;
        include!("tests/hash.rs");
    }

    mod json {
        use super::*;
        include!("tests/json.rs");
    }

    mod keyspace_copy {
        use super::*;
        include!("tests/keyspace_copy.rs");
    }

    mod set_list_async {
        use super::*;
        include!("tests/set_list_async.rs");
    }

    mod zset {
        use super::*;
        include!("tests/zset.rs");
    }

    mod stream_group {
        use super::*;
        include!("tests/stream_group.rs");
    }

    mod transactions {
        use super::*;
        include!("tests/transactions.rs");
    }

    mod native_hash {
        use super::*;
        include!("tests/native_hash.rs");
    }

    mod native_set_zset {
        use super::*;
        include!("tests/native_set_zset.rs");
    }

    mod native_list {
        use super::*;
        include!("tests/native_list.rs");
    }

    mod stream_string_batch {
        use super::*;
        include!("tests/stream_string_batch.rs");
    }
