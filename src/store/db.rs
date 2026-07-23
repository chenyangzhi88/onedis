use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::Error;
use bincode::{Decode, Encode};
use bytes::Bytes;
use common::types::status::Status;
use common::types::write_batch::{WriteBatch, WriteType};
use dashmap::{DashMap, mapref::entry::Entry};
use serde_json::Value as JsonValue;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::kv_store::{CompareCondition, KvStore, ObservedRawValue};
use super::ttl::{
    TYPE_HASH, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_SORTED_SET, TYPE_STREAM, TYPE_STRING,
    TYPE_VECTOR, TtlManager, VersionCounter, decode_meta_header, patch_meta_expire_ms,
    reserve_version_high_water_to_batch,
};
use crate::{observability::metrics::global_metrics, tools::pattern};

#[path = "full_text.rs"]
mod full_text;
#[path = "full_text_directory.rs"]
mod full_text_directory;
#[path = "vector.rs"]
mod vector;
pub use full_text::{
    FullTextAggregateLoadField, FullTextAggregateOptions, FullTextAggregateReducer,
    FullTextAggregateReducerKind, FullTextAggregateSortBy, FullTextAggregateStep,
    FullTextCreateOptions, FullTextFieldKind, FullTextFieldOptions, FullTextFieldSchema,
    FullTextGeoShapeCoordinateSystem, FullTextIndexOptions, FullTextReturnField,
    FullTextRuntimeRegistry, FullTextScorer, FullTextSearchBound, FullTextSearchGeoFilter,
    FullTextSearchNumericFilter, FullTextSearchOptions, FullTextSortBy, FullTextSourceType,
    FullTextVectorAlgorithm, FullTextVectorOptions,
};
pub use vector::{
    VectorCreateOptions, VectorFieldKind, VectorFieldSchema, VectorRuntimeRegistry,
    VectorSearchOptions, VectorSearchResult,
};

mod collection_key_codec;
mod core_key_watch;
mod core_lifecycle;
mod core_write_batch;
mod hash_basic_write;
mod hash_field_ttl;
mod hash_get_set_ex;
mod hash_key_codec;
mod hash_numeric_update;
mod hash_read_query;
mod hash_scan;
mod json_read_delete_ops;
mod json_set_ops;
mod json_value_codec;
mod key_encoding;
mod key_expiration_runtime;
mod key_expire_persist_ops;
mod keyspace_copy_ops;
mod keyspace_move;
mod keyspace_rename;
mod keyspace_scan_admin;
mod list_helpers;
mod list_move_insert_ops;
mod list_pop_ops;
mod list_push_ops;
mod list_read_range_ops;
mod list_update_trim_remove;
mod native_expire_helpers;
mod native_hash_helpers;
mod native_list_helpers;
mod native_set_member_scan;
mod native_set_meta;
mod native_set_random_seek;
mod native_set_slot_index;
mod native_set_slot_mutation;
mod native_stream_helpers;
mod native_zset_helpers;
mod set_aggregate_ops;
mod set_member_reads;
mod set_member_writes;
mod set_random_pop_scan;
mod sorted_set_entry_reads;
mod sorted_set_lex_ops;
mod sorted_set_pop_ops;
mod sorted_set_range_score;
mod sorted_set_read_query;
mod sorted_set_remove_range;
mod sorted_set_scan;
mod sorted_set_store_setops;
mod sorted_set_write_ops;
mod storage_delete_helpers;
mod storage_json_helpers;
mod storage_live_raw;
mod storage_read_helpers;
mod storage_structure_copy_async;
mod storage_structure_copy_sync;
mod storage_structure_delete;
mod storage_write_helpers;
mod stream_entry_delete_ops;
mod stream_entry_read_ops;
mod stream_entry_trim_ops;
mod stream_entry_write_ops;
mod stream_group_management;
mod stream_group_read;
mod stream_info;
mod stream_key_codec;
mod stream_pending_ops;
mod string_batch_write_ops;
mod string_bitmap_ops;
mod string_integer_ops;
mod string_key_readonly;
mod string_keyspace_flush;
mod string_read_ops;
mod string_set_write_ops;
mod string_structure_write_ops;
mod types;
mod value_entry_codec;
mod value_meta_codec;
mod value_runtime_helpers;
mod version_owner_gc;

use collection_key_codec::*;
use hash_key_codec::*;
use json_value_codec::*;
use key_encoding::*;
use list_helpers::*;
use stream_key_codec::*;
use types::*;
use value_entry_codec::*;
use value_meta_codec::*;
use value_runtime_helpers::*;

pub use types::*;
pub use value_entry_codec::decode_string_bytes_slice;

pub struct Db {
    db_index: u16,
    store: KvStore,
    key_layout: KeyEncodingLayout,
    pub changes: Arc<AtomicU64>,
    version_counter: Arc<VersionCounter>,
    ttl_manager: Arc<TtlManager>,
    counter_cache: Arc<DashMap<Vec<u8>, CounterCacheEntry>>,
    counter_cache_maybe_non_empty: Arc<AtomicBool>,
    counter_cache_epoch: Arc<AtomicU64>,
    list_meta_cache: Arc<DashMap<Vec<u8>, ListMeta>>,
    list_meta_cache_maybe_non_empty: Arc<AtomicBool>,
    vector_runtimes: Arc<VectorRuntimeRegistry>,
    fulltext_runtimes: Arc<FullTextRuntimeRegistry>,
    set_write_locks: Arc<[tokio::sync::Mutex<()>; SET_WRITE_LOCK_SHARDS]>,
    mutation_tracker: Arc<KeyMutationTracker>,
    pending_mutations: Arc<Mutex<PendingMutations>>,
}

#[cfg(test)]
mod tests;
