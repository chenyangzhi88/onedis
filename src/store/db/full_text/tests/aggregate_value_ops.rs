use super::super::*;

#[test]
fn aggregate_value_conversions_truthiness_and_field_normalization_are_consistent() {
    assert!(!fulltext_aggregate_value_truthy(
        &FullTextAggregateValue::String("0".to_string())
    ));
    assert!(fulltext_aggregate_value_to_number(&FullTextAggregateValue::List(Vec::new())).is_err());
    assert_eq!(
        fulltext_aggregate_value_to_string(&FullTextAggregateValue::List(vec![
            FullTextAggregateValue::String("a".to_string()),
            FullTextAggregateValue::Number(2.0),
        ])),
        "a,2"
    );
    assert_eq!(normalize_fulltext_aggregate_field("@price"), "price");
}
