fn parse_filter(filter: &str) -> Result<Vec<FilterPredicate>, Error> {
    let mut predicates = Vec::new();
    let normalized = filter.replace("&&", " AND ");
    for part in normalized.split(" AND ") {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let before_len = predicates.len();
        if let Some((field, values)) = parse_in_predicate(part)? {
            predicates.push(FilterPredicate::TagIn(field, values));
            continue;
        }
        if let Some((field, value)) = split_binary(part, "==") {
            predicates.push(FilterPredicate::TagEq(
                normalize_filter_field(field),
                trim_filter_string(value.trim()),
            ));
            continue;
        }
        for (op_text, op) in [
            (">=", NumericOp::Ge),
            ("<=", NumericOp::Le),
            (">", NumericOp::Gt),
            ("<", NumericOp::Lt),
        ] {
            if let Some((field, value)) = split_binary(part, op_text) {
                let value = value
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| Error::msg("ERR invalid vector numeric filter"))?;
                if !value.is_finite() {
                    return Err(Error::msg("ERR invalid vector numeric filter"));
                }
                predicates.push(FilterPredicate::NumericCmp(
                    normalize_filter_field(field),
                    op,
                    value,
                ));
                break;
            }
        }
        if predicates.len() == before_len {
            return Err(Error::msg("ERR unsupported vector filter"));
        }
    }
    Ok(predicates)
}

fn split_binary<'a>(input: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    input.split_once(op)
}

fn parse_in_predicate(part: &str) -> Result<Option<(String, Vec<String>)>, Error> {
    let Some((field, values)) = part.split_once(" IN ") else {
        return Ok(None);
    };
    let values = values.trim();
    if !values.starts_with('(') || !values.ends_with(')') {
        return Err(Error::msg("ERR invalid vector IN filter"));
    }
    let values = values[1..values.len() - 1]
        .split(',')
        .map(|value| trim_filter_string(value.trim()))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(Error::msg("ERR invalid vector IN filter"));
    }
    Ok(Some((normalize_filter_field(field), values)))
}

fn normalize_filter_field(field: &str) -> String {
    field.trim().trim_start_matches('.').to_string()
}

fn trim_filter_string(value: &str) -> String {
    value.trim_matches('"').trim_matches('\'').to_string()
}

fn matches_filters(attrs: &JsonValue, predicates: &[FilterPredicate]) -> bool {
    predicates.iter().all(|predicate| match predicate {
        FilterPredicate::TagEq(field, expected) => attr_tag_matches(attrs.get(field), expected),
        FilterPredicate::TagIn(field, expected) => expected
            .iter()
            .any(|expected| attr_tag_matches(attrs.get(field), expected)),
        FilterPredicate::NumericCmp(field, op, expected) => attrs
            .get(field)
            .and_then(JsonValue::as_f64)
            .is_some_and(|actual| match op {
                NumericOp::Gt => actual > *expected,
                NumericOp::Ge => actual >= *expected,
                NumericOp::Lt => actual < *expected,
                NumericOp::Le => actual <= *expected,
            }),
    })
}

fn attr_tag_matches(value: Option<&JsonValue>, expected: &str) -> bool {
    let Some(value) = value else {
        return false;
    };
    if let Some(text) = value.as_str() {
        return text == expected;
    }
    value
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}
