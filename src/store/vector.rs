use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    hash::{BuildHasher, Hash, Hasher},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use bincode::{Decode, Encode};
use common::types::write_batch::WriteBatch;
use dashmap::DashMap;
use hnsw_rs::prelude::{DistCosine, DistL2, Distance, Hnsw};
use serde_json::Value as JsonValue;

use super::{
    CompareCondition, Db, KeyEncodingLayout, Structure, TYPE_VECTOR, VECTOR_DOC_NAMESPACE,
    VECTOR_GRAPH_NAMESPACE, VECTOR_META_NAMESPACE, VECTOR_NUMERIC_NAMESPACE,
    VECTOR_SEGMENT_NAMESPACE, VECTOR_TAG_NAMESPACE, Vector, VectorLinkLayers,
    VectorObservabilitySnapshot, WRONG_TYPE_ERROR, decode_meta_header, encode_entry,
};
use crate::observability::metrics::{elapsed_us, global_metrics};

const DEFAULT_VECTOR_SEGMENT_MAX_DOCS: u64 = 1024;
const DEFAULT_VECTOR_LSM_MAX_SEGMENT_DOCS: u64 = 1024 * 4 * 4 * 4 * 4;
const VECTOR_LSM_MERGE_FACTOR: usize = 4;
const DEFAULT_HNSW_M: u32 = 16;
const DEFAULT_HNSW_EF_CONSTRUCTION: u32 = 200;
const DEFAULT_HNSW_EF_RUNTIME: u32 = 64;
const DEFAULT_HNSW_MAX_LAYER: usize = 16;
const MAX_VECTOR_DIMENSIONS: usize = 65_536;
const MAX_VECTOR_INITIAL_CAP: usize = 1_000_000;
const MAX_VECTOR_HNSW_EF: usize = 1_000_000;
const MAX_VECTOR_PROJECTION_CELLS: usize = 16 * 1024 * 1024;
const DEFAULT_VECTOR_SEARCH_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_VECTOR_EXACT_SCAN_LIMIT: usize = 1_000_000;

include!("vector/types_runtime.rs");

include!("vector/db_api.rs");

include!("vector/storage_filter_helpers.rs");

#[cfg(test)]
mod tests {
    include!("vector/tests.rs");
}
