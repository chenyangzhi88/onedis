#[derive(Clone, Debug)]
pub struct FullTextAggregateOptions {
    pub search_options: FullTextSearchOptions,
    pub load: Option<Vec<FullTextAggregateLoadField>>,
    pub steps: Vec<FullTextAggregateStep>,
    pub sort_by: Vec<FullTextAggregateSortBy>,
    pub offset: usize,
    pub limit: usize,
    pub cursor_count: Option<usize>,
    pub cursor_max_idle_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateLoadField {
    pub identifier: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FullTextAggregateStep {
    Apply {
        expression: String,
        alias: String,
    },
    Filter {
        expression: String,
    },
    GroupBy {
        fields: Vec<String>,
        reducers: Vec<FullTextAggregateReducer>,
    },
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateReducer {
    pub kind: FullTextAggregateReducerKind,
    pub args: Vec<String>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FullTextAggregateReducerKind {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    FirstValue,
    ToList,
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateSortBy {
    pub field: String,
    pub asc: bool,
}

#[derive(Clone, Debug)]
struct FullTextAggregateRow {
    values: HashMap<String, FullTextAggregateValue>,
    output: Vec<(String, FullTextAggregateValue)>,
}

#[derive(Clone, Debug)]
enum FullTextAggregateValue {
    Null,
    String(String),
    Number(f64),
    List(Vec<FullTextAggregateValue>),
}
