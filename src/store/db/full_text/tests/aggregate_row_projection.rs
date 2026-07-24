use super::super::*;
use super::support::*;

#[test]
fn aggregate_rows_project_hits_and_replace_existing_output() {
    let mut row = row("doc:1", 2.0, &[("title", "Rust")]);
    assert_eq!(number_value(row.values.get("__score").unwrap()), 2.0);
    assert_eq!(string_value(row.values.get("__key").unwrap()), "doc:1");
    fulltext_aggregate_set_output(
        &mut row,
        "computed".to_string(),
        FullTextAggregateValue::Number(12.0),
    );
    fulltext_aggregate_set_output(
        &mut row,
        "computed".to_string(),
        FullTextAggregateValue::Number(14.0),
    );
    assert_eq!(row.output.len(), 1);
    assert_eq!(number_value(&row.output[0].1), 14.0);
}
