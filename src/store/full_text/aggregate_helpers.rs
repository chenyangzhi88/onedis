include!("aggregate_row_projection.rs");
include!("aggregate_reducers.rs");
include!("aggregate_expressions.rs");
include!("aggregate_value_ops.rs");
include!("aggregate_sort_frame.rs");
fn fulltext_index_filter_matches(
    meta: &FullTextIndexMeta,
    fields: &[(String, String)],
) -> Result<bool, Error> {
    let Some(filter) = meta.index_options.filter.as_deref() else {
        return Ok(true);
    };
    let mut values = HashMap::new();
    for (name, value) in fields {
        let aggregate_value = value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .map(FullTextAggregateValue::Number)
            .unwrap_or_else(|| FullTextAggregateValue::String(value.clone()));
        values.insert(name.clone(), aggregate_value.clone());
        values.insert(name.trim_start_matches('@').to_string(), aggregate_value);
    }
    eval_fulltext_aggregate_filter(
        filter,
        &FullTextAggregateRow {
            values,
            output: Vec::new(),
        },
    )
}
