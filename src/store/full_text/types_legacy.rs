#[derive(Clone, Debug, Encode, Decode)]
struct LegacyFullTextIndexMeta {
    prefixes: Vec<String>,
    schema: Vec<LegacyFullTextFieldSchema>,
    state: FullTextIndexState,
    generation: u64,
    backfill_cursor: Option<String>,
    last_indexed_outbox_seq: u64,
    refresh_policy: FullTextRefreshPolicy,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyPhase2FullTextIndexMeta {
    source_type: FullTextSourceType,
    prefixes: Vec<String>,
    schema: Vec<LegacyPhase2FullTextFieldSchema>,
    aliases: Vec<String>,
    index_options: LegacyPhase2FullTextIndexOptions,
    state: FullTextIndexState,
    generation: u64,
    backfill_cursor: Option<String>,
    last_indexed_outbox_seq: u64,
    refresh_policy: FullTextRefreshPolicy,
}

#[derive(Clone, Debug, Default, Encode, Decode)]
struct LegacyPhase2FullTextIndexOptions {
    skip_initial_scan: bool,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyPhase2FullTextFieldSchema {
    name: String,
    kind: FullTextFieldKind,
    options: LegacyPhase2FullTextFieldOptions,
}

#[derive(Clone, Debug, Default, Encode, Decode)]
struct LegacyPhase2FullTextFieldOptions {
    alias: Option<String>,
    sortable: bool,
    noindex: bool,
    weight: Option<f32>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyFullTextFieldSchema {
    name: String,
    kind: FullTextFieldKind,
}

impl From<LegacyPhase2FullTextIndexMeta> for FullTextIndexMeta {
    fn from(value: LegacyPhase2FullTextIndexMeta) -> Self {
        Self {
            source_type: value.source_type,
            prefixes: value.prefixes,
            schema: value
                .schema
                .into_iter()
                .map(FullTextFieldSchema::from)
                .collect(),
            aliases: value.aliases,
            index_options: FullTextIndexOptions {
                skip_initial_scan: value.index_options.skip_initial_scan,
                ..FullTextIndexOptions::default()
            },
            state: value.state,
            generation: value.generation,
            backfill_cursor: value.backfill_cursor,
            last_indexed_outbox_seq: value.last_indexed_outbox_seq,
            refresh_policy: value.refresh_policy,
        }
    }
}

impl From<LegacyPhase2FullTextFieldSchema> for FullTextFieldSchema {
    fn from(value: LegacyPhase2FullTextFieldSchema) -> Self {
        Self {
            name: value.name,
            kind: value.kind,
            options: FullTextFieldOptions {
                alias: value.options.alias,
                sortable: value.options.sortable,
                noindex: value.options.noindex,
                weight: value.options.weight,
                ..FullTextFieldOptions::default()
            },
        }
    }
}

impl From<LegacyFullTextIndexMeta> for FullTextIndexMeta {
    fn from(value: LegacyFullTextIndexMeta) -> Self {
        Self {
            source_type: FullTextSourceType::Hash,
            prefixes: value.prefixes,
            schema: value
                .schema
                .into_iter()
                .map(|field| FullTextFieldSchema {
                    name: field.name,
                    kind: field.kind,
                    options: FullTextFieldOptions::default(),
                })
                .collect(),
            aliases: Vec::new(),
            index_options: FullTextIndexOptions::default(),
            state: value.state,
            generation: value.generation,
            backfill_cursor: value.backfill_cursor,
            last_indexed_outbox_seq: value.last_indexed_outbox_seq,
            refresh_policy: value.refresh_policy,
        }
    }
}
