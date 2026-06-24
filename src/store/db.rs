use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::Error;
use bincode::{Decode, Encode};
use bytes::Bytes;
use common::types::status::Status;
use common::types::write_batch::{WriteBatch, WriteType};
use dashmap::{DashMap, mapref::entry::Entry};
use kv_engine::db::SchemalessCompareCondition as CompareCondition;
use serde_json::Value as JsonValue;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::kv_store::KvStore;
use super::ttl::{
    TYPE_HASH, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_SORTED_SET, TYPE_STREAM, TYPE_STRING,
    TYPE_VECTOR, TtlManager, VersionCounter, decode_meta_header, delete_sub_keys_to_batch,
    patch_meta_expire_ms, reserve_version_high_water_to_batch,
};
use crate::{command::Command, frame::Frame, tools::pattern};

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
    FullTextRuntimeRegistry, FullTextSearchBound, FullTextSearchGeoFilter,
    FullTextSearchNumericFilter, FullTextSearchOptions, FullTextSortBy, FullTextSourceType,
    FullTextVectorAlgorithm, FullTextVectorOptions,
};
pub use vector::{
    VectorCreateOptions, VectorFieldKind, VectorFieldSchema, VectorRuntimeRegistry,
    VectorSearchOptions, VectorSearchResult,
};

include!("db/key_encoding.rs");
include!("db/types.rs");
include!("db/value_encoding.rs");

pub struct Db {
    db_index: u16,
    store: KvStore,
    pub changes: Arc<AtomicU64>,
    version_counter: Arc<VersionCounter>,
    ttl_manager: Arc<TtlManager>,
    counter_cache: Arc<DashMap<Vec<u8>, CounterCacheEntry>>,
    counter_cache_epoch: Arc<AtomicU64>,
    list_meta_cache: Arc<DashMap<Vec<u8>, ListMeta>>,
    vector_runtimes: Arc<VectorRuntimeRegistry>,
    fulltext_runtimes: Arc<FullTextRuntimeRegistry>,
    set_write_locks: Arc<[tokio::sync::Mutex<()>; SET_WRITE_LOCK_SHARDS]>,
    mutation_tracker: Arc<KeyMutationTracker>,
    pending_mutations: Arc<Mutex<PendingMutations>>,
}

include!("db/core_dispatch.rs");
include!("db/string_json.rs");
include!("db/bitmap_integer_ttl.rs");
include!("db/hash.rs");
include!("db/set.rs");
include!("db/sorted_set.rs");
include!("db/stream.rs");
include!("db/list.rs");
include!("db/keyspace_copy.rs");
include!("db/native_helpers.rs");
include!("db/storage_helpers.rs");
include!("db/list_helpers.rs");

#[cfg(test)]
mod tests {
    include!("db/tests.rs");
}
