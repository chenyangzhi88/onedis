fn compare_fulltext_aggregate_values(
    left: &FullTextAggregateValue,
    right: &FullTextAggregateValue,
    op: &str,
) -> Result<bool, Error> {
    let ordering = match (
        fulltext_aggregate_value_to_number(left),
        fulltext_aggregate_value_to_number(right),
    ) {
        (Ok(left), Ok(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => {
            fulltext_aggregate_value_to_string(left).cmp(&fulltext_aggregate_value_to_string(right))
        }
    };
    Ok(match op {
        "==" | "=" => ordering == std::cmp::Ordering::Equal,
        "!=" => ordering != std::cmp::Ordering::Equal,
        ">" => ordering == std::cmp::Ordering::Greater,
        ">=" => matches!(
            ordering,
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ),
        "<" => ordering == std::cmp::Ordering::Less,
        "<=" => matches!(
            ordering,
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
        ),
        _ => return Err(Error::msg("ERR invalid aggregate filter")),
    })
}

fn fulltext_aggregate_value_truthy(value: &FullTextAggregateValue) -> bool {
    match value {
        FullTextAggregateValue::Null => false,
        FullTextAggregateValue::String(value) => !value.is_empty() && value != "0",
        FullTextAggregateValue::Number(value) => *value != 0.0,
        FullTextAggregateValue::List(values) => !values.is_empty(),
    }
}

fn fulltext_aggregate_value_to_number(value: &FullTextAggregateValue) -> Result<f64, Error> {
    match value {
        FullTextAggregateValue::Number(value) => Ok(*value),
        FullTextAggregateValue::String(value) => value
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR aggregate expression expected numeric value")),
        FullTextAggregateValue::Null | FullTextAggregateValue::List(_) => Err(Error::msg(
            "ERR aggregate expression expected numeric value",
        )),
    }
}

fn fulltext_aggregate_value_to_string(value: &FullTextAggregateValue) -> String {
    match value {
        FullTextAggregateValue::Null => String::new(),
        FullTextAggregateValue::String(value) => value.clone(),
        FullTextAggregateValue::Number(value) => format_fulltext_aggregate_number(*value),
        FullTextAggregateValue::List(values) => values
            .iter()
            .map(fulltext_aggregate_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn format_fulltext_aggregate_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn normalize_fulltext_aggregate_field(field: &str) -> String {
    field.trim().trim_start_matches('@').to_string()
}
