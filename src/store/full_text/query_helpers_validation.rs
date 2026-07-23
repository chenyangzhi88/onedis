fn parse_query_attribute_weight(raw: &str) -> Result<Option<f32>, Error> {
    let Some(offset) = raw.find("$weight") else {
        return Ok(None);
    };
    let rest = raw[offset + "$weight".len()..]
        .trim_start_matches(|ch: char| ch.is_ascii_whitespace() || ch == ':' || ch == '=')
        .trim_start();
    let token = rest
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ';' || ch == '}')
        .next()
        .unwrap_or_default();
    if token.is_empty() {
        return Ok(None);
    }
    let weight = token
        .parse::<f32>()
        .map_err(|_| Error::msg("ERR invalid query attribute"))?;
    if weight.is_finite() && weight > 0.0 {
        Ok(Some(weight))
    } else {
        Err(Error::msg("ERR invalid query attribute"))
    }
}

fn split_tag_values(raw: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in raw.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '|' {
            values.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    values.push(current.trim().to_string());
    values.retain(|value| !value.is_empty());
    values
}

fn unescape_query_token(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut escaped = false;
    for ch in raw.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    out
}

fn parse_numeric_bound(raw: &str) -> Result<FullTextNumericBound, Error> {
    let (exclusive, value) = raw
        .strip_prefix('(')
        .map(|value| (true, value))
        .unwrap_or((false, raw));
    match value.to_ascii_lowercase().as_str() {
        "-inf" => Ok(FullTextNumericBound::NegInf),
        "+inf" | "inf" => Ok(FullTextNumericBound::PosInf),
        _ => {
            let number = value
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR invalid numeric range"))?;
            if !number.is_finite() {
                return Err(Error::msg("ERR invalid numeric range"));
            }
            if exclusive {
                Ok(FullTextNumericBound::Exclusive(number))
            } else {
                Ok(FullTextNumericBound::Inclusive(number))
            }
        }
    }
}

fn numeric_bound_to_tantivy(field: Field, bound: FullTextNumericBound, lower: bool) -> Bound<Term> {
    match (bound, lower) {
        (FullTextNumericBound::NegInf, true) | (FullTextNumericBound::PosInf, false) => {
            Bound::Unbounded
        }
        (FullTextNumericBound::NegInf, false) => {
            Bound::Included(Term::from_field_f64(field, f64::MIN))
        }
        (FullTextNumericBound::PosInf, true) => {
            Bound::Included(Term::from_field_f64(field, f64::MAX))
        }
        (FullTextNumericBound::Inclusive(value), _) => {
            Bound::Included(Term::from_field_f64(field, value))
        }
        (FullTextNumericBound::Exclusive(value), _) => {
            Bound::Excluded(Term::from_field_f64(field, value))
        }
    }
}

fn search_bound_to_tantivy(
    field: Field,
    bound: FullTextSearchBound,
    lower: bool,
) -> Bound<Term> {
    match (bound, lower) {
        (FullTextSearchBound::NegInf, true) | (FullTextSearchBound::PosInf, false) => {
            Bound::Unbounded
        }
        (FullTextSearchBound::NegInf, false) => {
            Bound::Included(Term::from_field_f64(field, f64::MIN))
        }
        (FullTextSearchBound::PosInf, true) => {
            Bound::Included(Term::from_field_f64(field, f64::MAX))
        }
        (FullTextSearchBound::Inclusive(value), _) => {
            Bound::Included(Term::from_field_f64(field, value))
        }
        (FullTextSearchBound::Exclusive(value), _) => {
            Bound::Excluded(Term::from_field_f64(field, value))
        }
    }
}

fn fulltext_wildcard_to_regex(pattern: &str) -> String {
    let mut regex = String::new();
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex
}

fn parse_f64_token(raw: &str, error: &str) -> Result<f64, Error> {
    raw.parse::<f64>()
        .map_err(|_| Error::msg(error.to_string()))
}

fn validate_fulltext_create(options: &FullTextCreateOptions) -> Result<(), Error> {
    if options.prefixes.is_empty() || options.schema.is_empty() {
        return Err(Error::msg("ERR invalid fulltext index definition"));
    }
    if let Some(score) = options.index_options.score
        && (!score.is_finite() || score < 0.0)
    {
        return Err(Error::msg("ERR invalid fulltext score"));
    }
    if let Some(language) = options.index_options.language.as_deref() {
        normalize_fulltext_language(language)?;
    }
    for value in [
        options.index_options.language_field.as_deref(),
        options.index_options.score_field.as_deref(),
        options.index_options.payload_field.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.trim().is_empty() {
            return Err(Error::msg("ERR invalid fulltext index definition"));
        }
    }
    if options.index_options.temporary_seconds == Some(0) {
        return Err(Error::msg("ERR invalid fulltext temporary duration"));
    }
    if options
        .index_options
        .stopwords
        .as_ref()
        .is_some_and(|words| words.iter().any(|word| word.is_empty()))
    {
        return Err(Error::msg("ERR invalid fulltext stopword"));
    }
    let mut seen = std::collections::HashSet::new();
    let mut seen_attributes = std::collections::HashSet::new();
    for field in &options.schema {
        if field.name.is_empty()
            || field.attribute_name().is_empty()
            || !seen.insert(field.name.clone())
            || !seen_attributes.insert(field.attribute_name().to_string())
        {
            return Err(Error::msg("ERR invalid fulltext schema"));
        }
        if matches!(options.source_type, FullTextSourceType::Json) {
            parse_fulltext_json_path(&field.name)?;
        }
        validate_fulltext_field(field)?;
    }
    Ok(())
}

fn validate_fulltext_field(field: &FullTextFieldSchema) -> Result<(), Error> {
    match field.kind {
        FullTextFieldKind::Text => {
            if field.options.separator.is_some()
                || field.options.case_sensitive
                || field.options.geoshape_coordinate_system.is_some()
                || field.options.vector.is_some()
            {
                return Err(Error::msg("ERR invalid fulltext schema option"));
            }
        }
        FullTextFieldKind::Tag => {
            if field.options.weight.is_some()
                || field.options.nostem
                || field.options.phonetic.is_some()
                || field.options.geoshape_coordinate_system.is_some()
                || field.options.vector.is_some()
            {
                return Err(Error::msg("ERR invalid fulltext schema option"));
            }
            if field
                .options
                .separator
                .as_ref()
                .is_some_and(|separator| separator.chars().count() != 1)
            {
                return Err(Error::msg("ERR invalid TAG separator"));
            }
        }
        FullTextFieldKind::Numeric | FullTextFieldKind::Geo => {
            if field.options.weight.is_some()
                || field.options.nostem
                || field.options.phonetic.is_some()
                || field.options.separator.is_some()
                || field.options.case_sensitive
                || field.options.with_suffix_trie
                || field.options.index_empty
                || field.options.geoshape_coordinate_system.is_some()
                || field.options.vector.is_some()
            {
                return Err(Error::msg("ERR invalid fulltext schema option"));
            }
        }
        FullTextFieldKind::GeoShape => {
            if field.options.sortable
                || field.options.sortable_unf
                || field.options.weight.is_some()
                || field.options.nostem
                || field.options.phonetic.is_some()
                || field.options.separator.is_some()
                || field.options.case_sensitive
                || field.options.with_suffix_trie
                || field.options.index_empty
                || field.options.vector.is_some()
            {
                return Err(Error::msg("ERR invalid fulltext schema option"));
            }
            if field.options.geoshape_coordinate_system.is_none() {
                return Err(Error::msg("ERR missing GEOSHAPE coordinate system"));
            }
        }
        FullTextFieldKind::Vector => {
            if field.options.sortable
                || field.options.sortable_unf
                || field.options.weight.is_some()
                || field.options.nostem
                || field.options.phonetic.is_some()
                || field.options.separator.is_some()
                || field.options.case_sensitive
                || field.options.with_suffix_trie
                || field.options.index_empty
                || field.options.index_missing
                || field.options.geoshape_coordinate_system.is_some()
            {
                return Err(Error::msg("ERR invalid fulltext schema option"));
            }
            validate_fulltext_vector_options(
                field
                    .options
                    .vector
                    .as_ref()
                    .ok_or_else(|| Error::msg("ERR missing VECTOR options"))?,
            )?;
        }
    }
    Ok(())
}

fn validate_fulltext_vector_options(options: &FullTextVectorOptions) -> Result<(), Error> {
    let mut seen = HashSet::new();
    let mut has_type = false;
    let mut has_dim = false;
    let mut has_metric = false;
    for (name, value) in &options.attributes {
        let normalized = name.to_ascii_uppercase();
        if !seen.insert(normalized.clone()) {
            return Err(Error::msg("ERR duplicate VECTOR attribute"));
        }
        match normalized.as_str() {
            "TYPE" => match value.to_ascii_uppercase().as_str() {
                "FLOAT32" | "FLOAT64" | "BFLOAT16" | "FLOAT16" | "INT8" | "UINT8" => {
                    has_type = true;
                }
                _ => return Err(Error::msg("ERR invalid VECTOR TYPE")),
            },
            "DIM" => {
                let dim = value
                    .parse::<usize>()
                    .map_err(|_| Error::msg("ERR invalid VECTOR DIM"))?;
                if dim == 0 {
                    return Err(Error::msg("ERR invalid VECTOR DIM"));
                }
                has_dim = true;
            }
            "DISTANCE_METRIC" => match value.to_ascii_uppercase().as_str() {
                "L2" | "IP" | "COSINE" => has_metric = true,
                _ => return Err(Error::msg("ERR invalid VECTOR DISTANCE_METRIC")),
            },
            "INITIAL_CAP" | "BLOCK_SIZE" | "M" | "EF_CONSTRUCTION" | "EF_RUNTIME" | "EPSILON" => {
                value
                    .parse::<f64>()
                    .map_err(|_| Error::msg("ERR invalid VECTOR attribute"))?;
            }
            _ => return Err(Error::msg("ERR unsupported VECTOR attribute")),
        }
    }
    if has_type && has_dim && has_metric {
        Ok(())
    } else {
        Err(Error::msg("ERR missing VECTOR attribute"))
    }
}

fn fulltext_source_type_name(source_type: FullTextSourceType) -> &'static str {
    match source_type {
        FullTextSourceType::Hash => "HASH",
        FullTextSourceType::Json => "JSON",
    }
}

fn fulltext_state_name(state: FullTextIndexState) -> &'static str {
    match state {
        FullTextIndexState::Creating => "creating",
        FullTextIndexState::Backfilling => "backfilling",
        FullTextIndexState::Ready => "ready",
        FullTextIndexState::Dirty => "dirty",
        FullTextIndexState::Rebuilding => "rebuilding",
        FullTextIndexState::Dropping => "dropping",
    }
}
