use super::super::*;
use super::support::*;

#[test]
fn aggregate_expressions_and_filters_cover_success_and_errors() {
    let row = row(
        "doc:1",
        2.0,
        &[("category", "books"), ("price", "10"), ("title", "Rust")],
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("(@price + 5) * 2", &row).unwrap()),
        30.0
    );
    assert_eq!(
        string_value(&eval_fulltext_aggregate_expression("upper(@title)", &row).unwrap()),
        "RUST"
    );
    assert_eq!(
        string_value(&eval_fulltext_aggregate_expression("lower('RUST')", &row).unwrap()),
        "rust"
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("sqrt(9)", &row).unwrap()),
        3.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("ceil(1.2)", &row).unwrap()),
        2.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("floor(1.8)", &row).unwrap()),
        1.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("abs(-3)", &row).unwrap()),
        3.0
    );
    assert!(eval_fulltext_aggregate_expression("", &row).is_err());
    assert!(eval_fulltext_aggregate_expression("bad(@price)", &row).is_err());
    assert!(eval_fulltext_aggregate_filter("@price >= 10", &row).unwrap());
    assert!(eval_fulltext_aggregate_filter("@title != 'Go'", &row).unwrap());
    assert!(eval_fulltext_aggregate_filter("@title", &row).unwrap());
}
