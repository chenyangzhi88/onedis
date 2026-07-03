use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use anyhow::Error;
use bincode::{Decode, Encode};
use common::types::write_batch::WriteBatch;
use dashmap::DashMap;
use hnsw_rs::{
    anndists::dist::distances::Distance,
    prelude::{DistCosine, DistL2, Hnsw},
};
use serde_json::Value as JsonValue;

use super::{
    Db, Structure, TYPE_VECTOR, VECTOR_DOC_NAMESPACE, VECTOR_GRAPH_NAMESPACE,
    VECTOR_META_NAMESPACE, VECTOR_NUMERIC_NAMESPACE, VECTOR_SEGMENT_NAMESPACE,
    VECTOR_TAG_NAMESPACE, Vector, VectorObservabilitySnapshot, WRONG_TYPE_ERROR,
    decode_meta_header, encode_entry, main_key, sub_key_range_start_bytes,
};
use crate::observability::metrics::{elapsed_us, global_metrics};

const DEFAULT_VECTOR_SEGMENT_MAX_DOCS: u64 = 1024;
const DEFAULT_HNSW_M: u32 = 16;
const DEFAULT_HNSW_EF_CONSTRUCTION: u32 = 64;
const DEFAULT_HNSW_EF_RUNTIME: u32 = 64;
const DEFAULT_HNSW_MAX_LAYER: usize = 16;

include!("vector/types_runtime.rs");

include!("vector/db_api.rs");

include!("vector/storage_filter_helpers.rs");

#[cfg(test)]
mod tests {
    include!("vector/tests.rs");
}
