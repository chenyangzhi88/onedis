use super::super::*;
use super::support::*;

#[test]
fn aggregate_reducers_use_incremental_group_state_and_enforce_memory() {
    let rows = vec![
        row(
            "doc:1",
            2.0,
            &[("category", "books"), ("price", "10"), ("title", "Rust")],
        ),
        row(
            "doc:2",
            1.0,
            &[("category", "books"), ("price", "20"), ("title", "Go")],
        ),
        row(
            "doc:3",
            3.0,
            &[("category", "games"), ("price", "5"), ("title", "Chess")],
        ),
    ];
    let reducers = vec![
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Count,
            args: Vec::new(),
            alias: Some("n".to_string()),
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Sum,
            args: vec!["@price".to_string()],
            alias: Some("sum_price".to_string()),
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Avg,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Min,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Max,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::CountDistinct,
            args: vec!["@title".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::FirstValue,
            args: vec!["@title".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::ToList,
            args: vec!["@title".to_string()],
            alias: None,
        },
    ];
    let grouped =
        fulltext_aggregate_group(rows.clone(), &["@category".to_string()], &reducers).unwrap();
    let books = grouped
        .iter()
        .find(|row| string_value(row.values.get("category").unwrap()) == "books")
        .unwrap();
    assert_eq!(number_value(books.values.get("n").unwrap()), 2.0);
    assert_eq!(number_value(books.values.get("sum_price").unwrap()), 30.0);
    assert_eq!(number_value(books.values.get("avg").unwrap()), 15.0);
    assert_eq!(number_value(books.values.get("min").unwrap()), 10.0);
    assert_eq!(number_value(books.values.get("max").unwrap()), 20.0);
    assert_eq!(
        number_value(books.values.get("count_distinct").unwrap()),
        2.0
    );
    assert_eq!(
        string_value(books.values.get("first_value").unwrap()),
        "Rust"
    );
    assert_eq!(
        fulltext_aggregate_value_to_string(books.values.get("tolist").unwrap()),
        "Rust,Go"
    );

    let missing_arg = FullTextAggregateReducer {
        kind: FullTextAggregateReducerKind::Sum,
        args: Vec::new(),
        alias: None,
    };
    assert!(fulltext_aggregate_reduce(&missing_arg, &[rows[0].clone()]).is_err());
    let mut bounded =
        FullTextAggregateGroupState::new(&["@category".to_string()], &reducers, 1).unwrap();
    assert!(bounded.push(&rows[0]).is_err());
}
