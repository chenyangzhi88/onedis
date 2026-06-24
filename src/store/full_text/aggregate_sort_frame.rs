fn compare_fulltext_aggregate_rows(
    left: &FullTextAggregateRow,
    right: &FullTextAggregateRow,
    sort_by: &[FullTextAggregateSortBy],
) -> std::cmp::Ordering {
    for sort_by in sort_by {
        let field = normalize_fulltext_aggregate_field(&sort_by.field);
        let left_value = left
            .values
            .get(&field)
            .unwrap_or(&FullTextAggregateValue::Null);
        let right_value = right
            .values
            .get(&field)
            .unwrap_or(&FullTextAggregateValue::Null);
        let ordering = match (
            fulltext_aggregate_value_to_number(left_value),
            fulltext_aggregate_value_to_number(right_value),
        ) {
            (Ok(left), Ok(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => fulltext_aggregate_value_to_string(left_value)
                .cmp(&fulltext_aggregate_value_to_string(right_value)),
        };
        let ordering = if sort_by.asc {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    fulltext_aggregate_value_to_string(
        left.values
            .get("__key")
            .unwrap_or(&FullTextAggregateValue::Null),
    )
    .cmp(&fulltext_aggregate_value_to_string(
        right
            .values
            .get("__key")
            .unwrap_or(&FullTextAggregateValue::Null),
    ))
}

fn fulltext_aggregate_frame(total: usize, rows: Vec<FullTextAggregateRow>) -> Frame {
    let mut out = Vec::with_capacity(rows.len() + 1);
    out.push(Frame::Integer(total as i64));
    for row in rows {
        let mut fields = Vec::with_capacity(row.output.len() * 2);
        for (field, value) in row.output {
            fields.push(Frame::bulk_string(field));
            fields.push(fulltext_aggregate_value_frame(value));
        }
        out.push(Frame::Array(fields));
    }
    Frame::Array(out)
}

fn fulltext_aggregate_value_frame(value: FullTextAggregateValue) -> Frame {
    match value {
        FullTextAggregateValue::Null => Frame::Null,
        FullTextAggregateValue::String(value) => Frame::bulk_string(value),
        FullTextAggregateValue::Number(value) => {
            Frame::bulk_string(format_fulltext_aggregate_number(value))
        }
        FullTextAggregateValue::List(values) => Frame::Array(
            values
                .into_iter()
                .map(fulltext_aggregate_value_frame)
                .collect(),
        ),
    }
}
