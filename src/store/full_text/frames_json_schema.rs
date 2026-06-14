fn compare_fulltext_sort_keys(
    left: &FullTextLiveHit,
    right: &FullTextLiveHit,
    asc: bool,
) -> std::cmp::Ordering {
    let ordering = match (&left.sort_key, &right.sort_key) {
        (Some(left), Some(right)) => match (left.parse::<f64>(), right.parse::<f64>()) {
            (Ok(left), Ok(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => left.cmp(right),
        },
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.key.cmp(&right.key),
    };
    let ordering = if asc { ordering } else { ordering.reverse() };
    ordering.then_with(|| left.key.cmp(&right.key))
}

fn format_fulltext_score(score: f32) -> String {
    if score.fract() == 0.0 {
        format!("{score:.1}")
    } else {
        score.to_string()
    }
}

fn json_index_strings(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::String(value) => vec![value.clone()],
        serde_json::Value::Number(value) => vec![value.to_string()],
        serde_json::Value::Bool(value) => vec![value.to_string()],
        serde_json::Value::Array(values) => values
            .iter()
            .flat_map(json_index_strings)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    }
}

fn json_index_tag_values(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::String(value) => vec![value.clone()],
        serde_json::Value::Number(value) => vec![value.to_string()],
        serde_json::Value::Bool(value) => vec![value.to_string()],
        serde_json::Value::Array(values) => values
            .iter()
            .flat_map(json_index_tag_values)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    }
}

fn json_index_numeric_values(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Number(value) => value
            .as_f64()
            .filter(|number| number.is_finite())
            .map(|number| vec![number.to_string()])
            .unwrap_or_default(),
        serde_json::Value::String(value) => value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .map(|_| vec![value.clone()])
            .unwrap_or_default(),
        serde_json::Value::Array(values) => values
            .iter()
            .flat_map(json_index_numeric_values)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    }
}

fn parse_fulltext_json_path(path: &str) -> Result<Vec<FullTextJsonPathToken>, Error> {
    if path == "$" || path == "." {
        return Ok(Vec::new());
    }

    let bytes = path.as_bytes();
    let mut idx = if bytes.first() == Some(&b'$') { 1 } else { 0 };
    let mut tokens = Vec::new();

    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                if idx < bytes.len() && bytes[idx] == b'*' {
                    tokens.push(FullTextJsonPathToken::Wildcard);
                    idx += 1;
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(Error::msg("ERR invalid JSON path"));
                }
                tokens.push(FullTextJsonPathToken::Field(path[start..idx].to_string()));
            }
            b'[' => {
                idx += 1;
                if idx < bytes.len() && bytes[idx] == b'*' {
                    idx += 1;
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(Error::msg("ERR invalid JSON path"));
                    }
                    idx += 1;
                    tokens.push(FullTextJsonPathToken::Wildcard);
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                    idx += 1;
                }
                if start == idx || idx >= bytes.len() || bytes[idx] != b']' {
                    return Err(Error::msg("ERR invalid JSON path"));
                }
                let index = path[start..idx]
                    .parse::<usize>()
                    .map_err(|_| Error::msg("ERR invalid JSON path"))?;
                idx += 1;
                tokens.push(FullTextJsonPathToken::Index(index));
            }
            _ => return Err(Error::msg("ERR invalid JSON path")),
        }
    }

    Ok(tokens)
}

fn fulltext_json_path_values(
    value: &serde_json::Value,
    tokens: &[FullTextJsonPathToken],
) -> Vec<serde_json::Value> {
    let Some((first, rest)) = tokens.split_first() else {
        return vec![value.clone()];
    };
    match first {
        FullTextJsonPathToken::Field(field) => value
            .as_object()
            .and_then(|object| object.get(field))
            .map(|child| fulltext_json_path_values(child, rest))
            .unwrap_or_default(),
        FullTextJsonPathToken::Index(index) => value
            .as_array()
            .and_then(|array| array.get(*index))
            .map(|child| fulltext_json_path_values(child, rest))
            .unwrap_or_default(),
        FullTextJsonPathToken::Wildcard => match value {
            serde_json::Value::Array(values) => values
                .iter()
                .flat_map(|child| fulltext_json_path_values(child, rest))
                .collect(),
            serde_json::Value::Object(object) => object
                .values()
                .flat_map(|child| fulltext_json_path_values(child, rest))
                .collect(),
            _ => Vec::new(),
        },
    }
}

fn fulltext_schema_frame(schema: &[FullTextFieldSchema]) -> Frame {
    Frame::Array(
        schema
            .iter()
            .map(|field| {
                Frame::Array(vec![
                    Frame::bulk_string("identifier"),
                    Frame::bulk_string(field.name.clone()),
                    Frame::bulk_string("attribute"),
                    Frame::bulk_string(field.attribute_name().to_string()),
                    Frame::bulk_string("type"),
                    Frame::bulk_string(match field.kind {
                        FullTextFieldKind::Text => "TEXT",
                        FullTextFieldKind::Tag => "TAG",
                        FullTextFieldKind::Numeric => "NUMERIC",
                        FullTextFieldKind::Geo => "GEO",
                        FullTextFieldKind::GeoShape => "GEOSHAPE",
                        FullTextFieldKind::Vector => "VECTOR",
                    }),
                    Frame::bulk_string("sortable"),
                    Frame::Integer(i64::from(field.options.sortable)),
                    Frame::bulk_string("sortable_unf"),
                    Frame::Integer(i64::from(field.options.sortable_unf)),
                    Frame::bulk_string("noindex"),
                    Frame::Integer(i64::from(field.options.noindex)),
                    Frame::bulk_string("weight"),
                    field
                        .options
                        .weight
                        .map(|weight| Frame::bulk_string(weight.to_string()))
                        .unwrap_or(Frame::Null),
                    Frame::bulk_string("nostem"),
                    Frame::Integer(i64::from(field.options.nostem)),
                    Frame::bulk_string("phonetic"),
                    field
                        .options
                        .phonetic
                        .clone()
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                    Frame::bulk_string("separator"),
                    field
                        .options
                        .separator
                        .clone()
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                    Frame::bulk_string("casesensitive"),
                    Frame::Integer(i64::from(field.options.case_sensitive)),
                    Frame::bulk_string("withsuffixtrie"),
                    Frame::Integer(i64::from(field.options.with_suffix_trie)),
                    Frame::bulk_string("indexempty"),
                    Frame::Integer(i64::from(field.options.index_empty)),
                    Frame::bulk_string("indexmissing"),
                    Frame::Integer(i64::from(field.options.index_missing)),
                    Frame::bulk_string("geoshape_coordinate_system"),
                    field
                        .options
                        .geoshape_coordinate_system
                        .map(fulltext_geoshape_coordinate_system_name)
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                    Frame::bulk_string("vector"),
                    field
                        .options
                        .vector
                        .as_ref()
                        .map(fulltext_vector_options_frame)
                        .unwrap_or(Frame::Null),
                ])
            })
            .collect(),
    )
}

fn fulltext_geoshape_coordinate_system_name(
    system: FullTextGeoShapeCoordinateSystem,
) -> &'static str {
    match system {
        FullTextGeoShapeCoordinateSystem::Flat => "FLAT",
        FullTextGeoShapeCoordinateSystem::Spherical => "SPHERICAL",
    }
}

fn fulltext_vector_algorithm_name(algorithm: FullTextVectorAlgorithm) -> &'static str {
    match algorithm {
        FullTextVectorAlgorithm::Flat => "FLAT",
        FullTextVectorAlgorithm::Hnsw => "HNSW",
    }
}

fn fulltext_vector_options_frame(options: &FullTextVectorOptions) -> Frame {
    let mut values = Vec::with_capacity(2 + options.attributes.len() * 2);
    values.push(Frame::bulk_string("algorithm"));
    values.push(Frame::bulk_string(fulltext_vector_algorithm_name(
        options.algorithm,
    )));
    for (name, value) in &options.attributes {
        values.push(Frame::bulk_string(name.clone()));
        values.push(Frame::bulk_string(value.clone()));
    }
    Frame::Array(values)
}

