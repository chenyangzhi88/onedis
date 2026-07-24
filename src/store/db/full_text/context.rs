pub(super) use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    collections::VecDeque,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
pub(super) use std::sync::atomic::AtomicBool;

pub(super) use anyhow::Error;
pub(super) use bincode::{Decode, Encode};
pub(super) use common::types::status::Status;
pub(super) use common::types::write_batch::WriteBatch;
pub(super) use dashmap::DashMap;
pub(super) use jieba_rs::Jieba;
pub(super) use rust_stemmers::{Algorithm as StemmerAlgorithm, Stemmer};
pub(super) use tantivy::{
    DocAddress, DocId, DocSet, Index, IndexReader, IndexWriter, Score, SegmentOrdinal,
    SegmentReader, TERMINATED, Term,
    collector::{Collector, Count, SegmentCollector},
    query::{
        AllQuery, BooleanQuery, BoostQuery, DisjunctionMaxQuery, FuzzyTermQuery, Occur,
        PhrasePrefixQuery, PhraseQuery, Query, QueryParser, RangeQuery, RegexQuery, Scorer,
        TermQuery, Weight,
    },
    schema::{
        Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TantivyDocument,
        TextFieldIndexing, TextOptions, Value,
    },
};
pub(super) use unicode_segmentation::UnicodeSegmentation;

pub(super) use super::super::full_text_directory::KvTantivyDirectory;
pub(super) use super::super::{
    Db, FULLTEXT_FILE_NAMESPACE, FULLTEXT_META_NAMESPACE, FULLTEXT_OUTBOX_NAMESPACE,
    VectorCreateOptions, VectorSearchOptions, VectorSearchResult, internal_prefix,
    logical_main_key_from_raw_key, prefix_exclusive_upper_bound,
};
pub(super) use crate::frame::Frame;
pub(super) use crate::observability::metrics::{elapsed_us, global_metrics};
pub(super) use crate::store::kv_store::CompareCondition;
pub(super) use crate::store::ttl::{TYPE_HASH, TYPE_JSON, decode_meta_header};

pub(super) const FULLTEXT_KEY_FIELD: &str = "__key";
pub(super) const FULLTEXT_WRITER_HEAP_BYTES: usize = 50_000_000;
pub(super) const DEFAULT_REFRESH_INTERVAL_MS: u64 = 100;
pub(super) const DEFAULT_REFRESH_MAX_DOCS: usize = 1024;
pub(super) const DEFAULT_REFRESH_MAX_BYTES: usize = 4 * 1024 * 1024;
pub(super) const DEFAULT_REFRESH_TIMEOUT_MS: u64 = 500;
pub(super) const DEFAULT_OUTBOX_COMPACT_THRESHOLD: usize = 1024;
pub(super) const DEFAULT_REPAIR_THROTTLE_MS: u64 = 1_000;

#[cfg(test)]
pub(super) static FULLTEXT_ALTER_FAIL_AFTER_SWAP: AtomicBool = AtomicBool::new(false);
