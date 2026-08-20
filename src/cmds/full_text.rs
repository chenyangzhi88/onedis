use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{
        Db, FullTextAggregateLoadField, FullTextAggregateOptions, FullTextAggregateReducer,
        FullTextAggregateReducerKind, FullTextAggregateSortBy, FullTextAggregateStep,
        FullTextCreateOptions, FullTextFieldKind, FullTextFieldOptions, FullTextFieldSchema,
        FullTextGeoShapeCoordinateSystem, FullTextHighlightOptions, FullTextHybridCombine,
        FullTextHybridOptions, FullTextIndexOptions, FullTextReturnField, FullTextScorer,
        FullTextSearchBound, FullTextSearchGeoFilter, FullTextSearchNumericFilter,
        FullTextSearchOptions, FullTextSortBy, FullTextSourceType, FullTextSpellcheckDictionary,
        FullTextSummarizeOptions, FullTextVectorAlgorithm, FullTextVectorOptions,
    },
};

pub struct FtCreate {
    pub index: String,
    pub options: FullTextCreateOptions,
}

pub struct FtSearch {
    pub index: String,
    pub query: String,
    pub options: FullTextSearchOptions,
}

pub struct FtHybrid {
    pub index: String,
    pub search_query: String,
    pub vector_query: String,
    pub options: FullTextHybridOptions,
}

pub struct FtAggregate {
    pub index: String,
    pub query: String,
    pub options: FullTextAggregateOptions,
}

pub enum FtCursor {
    Read {
        index: String,
        cursor_id: u64,
        count: usize,
    },
    Del {
        index: String,
        cursor_id: u64,
    },
}

pub struct FtProfile {
    target: FtProfileTarget,
    limited: bool,
}

enum FtProfileTarget {
    Search(FtSearch),
    Hybrid(FtHybrid),
    Aggregate(FtAggregate),
}

pub struct FtExplain {
    pub index: String,
    pub query: String,
    pub options: FullTextSearchOptions,
    pub cli: bool,
}

pub struct FtTagVals {
    pub index: String,
    pub field: String,
}

pub struct FtInfo {
    pub index: String,
}

pub struct FtList;

pub struct FtDropIndex {
    pub index: String,
    pub delete_documents: bool,
}

pub struct FtAlter {
    pub index: String,
    pub skip_initial_scan: bool,
    pub fields: Vec<FullTextFieldSchema>,
}

pub struct FtAliasAdd {
    pub alias: String,
    pub index: String,
}

pub struct FtAliasUpdate {
    pub alias: String,
    pub index: String,
}

pub struct FtAliasDel {
    pub alias: String,
}

pub enum FtConfig {
    Get { name: String },
    Set { name: String, value: String },
}

pub enum FtDict {
    Add { dict: String, terms: Vec<String> },
    Del { dict: String, terms: Vec<String> },
    Dump { dict: String },
}

pub struct FtSpellCheck {
    pub index: String,
    pub query: String,
    pub distance: usize,
    pub include: Vec<FullTextSpellcheckDictionary>,
    pub exclude: Vec<FullTextSpellcheckDictionary>,
    pub dialect: Option<u8>,
}

pub enum FtSug {
    Add {
        key: String,
        string: String,
        score: f64,
        incr: bool,
        payload: Option<Vec<u8>>,
    },
    Get {
        key: String,
        prefix: String,
        fuzzy: bool,
        with_scores: bool,
        with_payloads: bool,
        max: usize,
    },
    Del {
        key: String,
        string: String,
    },
    Len {
        key: String,
    },
}

pub enum FtSyn {
    Update {
        index: String,
        group: String,
        terms: Vec<String>,
        skip_initial_scan: bool,
    },
    Dump {
        index: String,
    },
}

pub struct FtUnsupported {
    command_name: String,
}

impl FtConfig {
    pub(crate) fn may_write_data(&self) -> bool {
        matches!(self, Self::Set { .. })
    }
}

impl FtDict {
    pub(crate) fn command_name(&self) -> &'static str {
        match self {
            Self::Add { .. } => "FT.DICTADD",
            Self::Del { .. } => "FT.DICTDEL",
            Self::Dump { .. } => "FT.DICTDUMP",
        }
    }

    pub(crate) fn may_write_data(&self) -> bool {
        matches!(self, Self::Add { .. } | Self::Del { .. })
    }
}

impl FtSug {
    pub(crate) fn command_name(&self) -> &'static str {
        match self {
            Self::Add { .. } => "FT.SUGADD",
            Self::Get { .. } => "FT.SUGGET",
            Self::Del { .. } => "FT.SUGDEL",
            Self::Len { .. } => "FT.SUGLEN",
        }
    }

    pub(crate) fn may_write_data(&self) -> bool {
        matches!(self, Self::Add { .. } | Self::Del { .. })
    }
}

impl FtSyn {
    pub(crate) fn command_name(&self) -> &'static str {
        match self {
            Self::Update { .. } => "FT.SYNUPDATE",
            Self::Dump { .. } => "FT.SYNDUMP",
        }
    }

    pub(crate) fn may_write_data(&self) -> bool {
        matches!(self, Self::Update { .. })
    }
}

impl FtUnsupported {
    pub(crate) fn command_name(&self) -> &str {
        &self.command_name
    }
}

include!("full_text/create_index_management.rs");
include!("full_text/search.rs");
include!("full_text/aggregate_profile.rs");
include!("full_text/explain_info_auxiliary.rs");
include!("full_text/parsing_helpers.rs");

#[cfg(test)]
#[path = "full_text/tests.rs"]
mod tests;
