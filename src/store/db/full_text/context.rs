pub(super) use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    collections::VecDeque,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(super) use anyhow::Error;
pub(super) use arc_swap::ArcSwapOption;
pub(super) use bincode::{Decode, Encode};
pub(super) use common::types::status::Status;
pub(super) use common::types::write_batch::WriteBatch;
pub(super) use dashmap::{DashMap, mapref::entry::Entry};
pub(super) use jieba_rs::Jieba;
pub(super) use levenshtein_automata::{
    DFA, Distance as LevenshteinDistance, LevenshteinAutomatonBuilder, SINK_STATE,
};
pub(super) use rust_stemmers::{Algorithm as StemmerAlgorithm, Stemmer};
pub(super) use tantivy::collector::sort_key::{SortByStaticFastValue, SortByString};
pub(super) use tantivy::{
    DocAddress, DocId, Index, IndexReader, IndexWriter, Order, Score, SegmentOrdinal,
    SegmentReader, Term,
    collector::{Collector, Count, SegmentCollector, TopDocs},
    indexer::LogMergePolicy,
    query::{
        AllQuery, BooleanQuery, BoostQuery, DisjunctionMaxQuery, EmptyQuery, Occur, PhraseQuery,
        Query, QueryParser, RangeQuery, RegexQuery, TermQuery,
    },
    schema::{
        FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TantivyDocument,
        TextFieldIndexing, TextOptions,
    },
};
pub(super) use tantivy_fst::{Automaton, Regex as FstRegex};
pub(super) use unicode_segmentation::UnicodeSegmentation;

pub(super) use super::super::full_text_directory::KvTantivyDirectory;
pub(super) use super::super::{
    Db, FULLTEXT_FILE_NAMESPACE, FULLTEXT_META_NAMESPACE, FULLTEXT_OUTBOX_NAMESPACE,
    PackedHashFields, VectorCreateOptions, VectorExactDistanceRequest, VectorSearchOptions,
    VectorSearchResult, hash_uses_packed_layout, internal_prefix, logical_main_key_from_raw_key,
    prefix_exclusive_upper_bound,
};
pub(super) use crate::frame::Frame;
pub(super) use crate::observability::metrics::{FullTextSearchStage, elapsed_us, global_metrics};
pub(super) use crate::store::kv_store::CompareCondition;
pub(super) use crate::store::ttl::{TYPE_HASH, TYPE_JSON, decode_meta_header};

pub(super) const FULLTEXT_KEY_FIELD: &str = "__key";
pub(super) const FULLTEXT_EXPIRES_AT_FIELD: &str = "__expires_at";
pub(super) const FULLTEXT_PRESENCE_FIELD_PREFIX: &str = "__presence_";
pub(super) const FULLTEXT_EMPTY_FIELD_PREFIX: &str = "__empty_";
pub(super) const FULLTEXT_GEO_FIELD_PREFIX: &str = "__geo_";
pub(super) const FULLTEXT_GEOSHAPE_FIELD_PREFIX: &str = "__geoshape_";
pub(super) const FULLTEXT_WRITER_HEAP_BYTES: usize = 50_000_000;
pub(super) const DEFAULT_REFRESH_INTERVAL_MS: u64 = 100;
pub(super) const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 1_000;
pub(super) const DEFAULT_REFRESH_MAX_DOCS: usize = 8192;
pub(super) const DEFAULT_REFRESH_MAX_BYTES: usize = 4 * 1024 * 1024;
pub(super) const DEFAULT_REFRESH_TIMEOUT_MS: u64 = 500;
pub(super) const DEFAULT_OUTBOX_COMPACT_THRESHOLD: usize = 1024;
pub(super) const DEFAULT_REPAIR_THROTTLE_MS: u64 = 1_000;
pub(super) const DEFAULT_DIRECTORY_CACHE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const DEFAULT_MERGE_MIN_SEGMENTS: usize = 4;
pub(super) const DEFAULT_MERGE_MAX_DOCS: usize = 10_000_000;
pub(super) const DEFAULT_MERGE_MIN_LAYER_DOCS: usize = 10_000;
pub(super) const DEFAULT_MERGE_DELETE_RATIO: f32 = 0.25;

#[cfg(test)]
pub(super) static FULLTEXT_ALTER_FAIL_AFTER_SWAP: AtomicBool = AtomicBool::new(false);
