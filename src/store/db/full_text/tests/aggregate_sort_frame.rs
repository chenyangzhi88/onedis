use super::super::*;
use super::support::*;

#[test]
fn aggregate_sort_and_frames_preserve_numeric_order_and_values() {
    let mut rows = [
        row("doc:2", 1.0, &[("price", "2")]),
        row("doc:1", 1.0, &[("price", "10")]),
    ];
    rows.sort_by(|left, right| {
        compare_fulltext_aggregate_rows(
            left,
            right,
            &[FullTextAggregateSortBy {
                field: "@price".to_string(),
                asc: true,
            }],
        )
    });
    assert_eq!(
        fulltext_aggregate_value_to_string(rows[0].values.get("__key").unwrap()),
        "doc:2"
    );
    rows[0].output.push((
        "price".to_string(),
        FullTextAggregateValue::String("2".to_string()),
    ));
    let frame = fulltext_aggregate_frame(1, vec![rows[0].clone()]);
    assert!(frame.to_string().contains("price"));
    assert!(matches!(
        fulltext_aggregate_value_frame(FullTextAggregateValue::Null),
        Frame::Null
    ));
}
