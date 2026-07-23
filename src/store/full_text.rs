use std::{
    collections::VecDeque,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use bincode::{Decode, Encode};
use common::types::write_batch::WriteBatch;
use dashmap::DashMap;
use jieba_rs::Jieba;
use rust_stemmers::{Algorithm as StemmerAlgorithm, Stemmer};
use tantivy::{
    Index, IndexReader, IndexWriter, Term,
    collector::{Count, TopDocs},
    query::{
        AllQuery, BooleanQuery, BoostQuery, DisjunctionMaxQuery, FuzzyTermQuery, Occur,
        PhrasePrefixQuery, PhraseQuery, Query, QueryParser, RangeQuery, RegexQuery, TermQuery,
    },
    schema::{
        Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TantivyDocument,
        TextFieldIndexing, TextOptions, Value,
    },
};
use unicode_segmentation::UnicodeSegmentation;

use super::full_text_directory::KvTantivyDirectory;
use super::{
    Db, FULLTEXT_FILE_NAMESPACE, FULLTEXT_META_NAMESPACE, FULLTEXT_OUTBOX_NAMESPACE,
    VectorCreateOptions, VectorSearchOptions, VectorSearchResult, internal_prefix,
    logical_main_key_from_raw_key, prefix_exclusive_upper_bound,
};
use crate::frame::Frame;
use crate::observability::metrics::{elapsed_us, global_metrics};
use crate::store::ttl::{TYPE_HASH, TYPE_JSON, decode_meta_header};

const FULLTEXT_KEY_FIELD: &str = "__key";
const FULLTEXT_WRITER_HEAP_BYTES: usize = 50_000_000;
const DEFAULT_REFRESH_INTERVAL_MS: u64 = 100;
const DEFAULT_REFRESH_MAX_DOCS: usize = 1024;
const DEFAULT_REFRESH_MAX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_REFRESH_TIMEOUT_MS: u64 = 500;
const DEFAULT_OUTBOX_COMPACT_THRESHOLD: usize = 1024;
const DEFAULT_REPAIR_THROTTLE_MS: u64 = 1_000;

include!("full_text/types.rs");
include!("full_text/runtime.rs");

include!("full_text/index_management_config.rs");
include!("full_text/search.rs");
include!("full_text/aggregate_cursor.rs");
include!("full_text/dictionary_suggestion_synonym.rs");
include!("full_text/info.rs");
include!("full_text/source_vector_indexing.rs");
include!("full_text/refresh_backfill_progress.rs");
include!("full_text/refresh_outbox.rs");
include!("full_text/metadata_config_helpers.rs");

include!("full_text/query_parser.rs");
include!("full_text/vector_query.rs");
include!("full_text/query_helpers_validation.rs");
include!("full_text/aggregate_helpers.rs");
include!("full_text/aggregate_cursor_store.rs");
include!("full_text/text_analysis.rs");
include!("full_text/search_eval_geo.rs");
include!("full_text/frames_json_schema.rs");
include!("full_text/storage_keys_config.rs");

#[cfg(test)]
mod tests {
    include!("full_text/tests.rs");
}
