fn parse_filter(filter: &str) -> Result<Vec<FilterPredicate>, Error> {
    let mut predicates = Vec::new();
    let normalized = filter.replace("&&", " AND ");
    let upper = normalized.to_ascii_uppercase();
    let mut start = 0usize;
    let mut parts = Vec::new();
    for (offset, _) in upper.match_indices(" AND ") {
        parts.push(&normalized[start..offset]);
        start = offset + 5;
    }
    parts.push(&normalized[start..]);
    for part in parts {
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
            let field = normalize_filter_field(field);
            let value = value.trim();
            if let Ok(number) = value.parse::<f64>() {
                if !number.is_finite() {
                    return Err(Error::msg("ERR invalid vector numeric filter"));
                }
                predicates.push(FilterPredicate::NumericCmp(field, NumericOp::Eq, number));
            } else {
                predicates.push(FilterPredicate::TagEq(field, trim_filter_string(value)));
            }
            continue;
        }
        if let Some((field, value)) = split_binary(part, "!=") {
            let field = normalize_filter_field(field);
            let value = value.trim();
            if let Ok(number) = value.parse::<f64>() {
                if !number.is_finite() {
                    return Err(Error::msg("ERR invalid vector numeric filter"));
                }
                predicates.push(FilterPredicate::NumericCmp(field, NumericOp::Ne, number));
            } else {
                predicates.push(FilterPredicate::TagNe(field, trim_filter_string(value)));
            }
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
    let upper = part.to_ascii_uppercase();
    let Some(offset) = upper.find(" IN ") else {
        return Ok(None);
    };
    let field = &part[..offset];
    let values = &part[offset + 4..];
    let values = values.trim();
    let wrapped = (values.starts_with('(') && values.ends_with(')'))
        || (values.starts_with('[') && values.ends_with(']'));
    if !wrapped {
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
        FilterPredicate::TagNe(field, expected) => {
            attrs.get(field).is_some() && !attr_tag_matches(attrs.get(field), expected)
        }
        FilterPredicate::TagIn(field, expected) => expected
            .iter()
            .any(|expected| attr_tag_matches(attrs.get(field), expected)),
        FilterPredicate::NumericCmp(field, op, expected) => attrs
            .get(field)
            .and_then(JsonValue::as_f64)
            .is_some_and(|actual| match op {
                NumericOp::Eq => actual == *expected,
                NumericOp::Ne => actual != *expected,
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
    if let Some(boolean) = value.as_bool() {
        return match expected.to_ascii_lowercase().as_str() {
            "true" | "1" => boolean,
            "false" | "0" => !boolean,
            _ => false,
        };
    }
    value
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}
