fn parse_alias_with_index(frame: Frame, command: &str) -> Result<(String, String), Error> {
    if frame.arg_len() != 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    Ok((
        arg(&frame, 1, "ERR invalid fulltext alias")?,
        arg(&frame, 2, "ERR invalid fulltext index")?,
    ))
}

fn fulltext_profile_inner_frame(
    frame: &Frame,
    command: &str,
    index: &str,
    start: usize,
) -> Result<Frame, Error> {
    if start >= frame.arg_len() {
        return Err(Error::msg("ERR syntax error"));
    }
    let mut args = vec![
        Frame::bulk_string(command.to_string()),
        Frame::bulk_string(index.to_string()),
    ];
    for idx in start..frame.arg_len() {
        args.push(Frame::BulkString(
            frame
                .get_arg_bytes(idx)
                .ok_or_else(|| Error::msg("ERR syntax error"))?,
        ));
    }
    Ok(Frame::Array(args))
}

fn fulltext_profile_frame(result: Frame, elapsed_ms: f64, pipeline: &str) -> Frame {
    Frame::Array(vec![
        result,
        Frame::Array(vec![
            Frame::bulk_string("Total profile time"),
            Frame::bulk_string(format!("{elapsed_ms:.3}")),
            Frame::bulk_string("Parsing time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Planning time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Index lookup time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Vector search time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Fetch time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Sort time"),
            Frame::bulk_string("0.000"),
            Frame::bulk_string("Aggregation time"),
            Frame::bulk_string(if pipeline == "Aggregate" {
                format!("{elapsed_ms:.3}")
            } else {
                "0.000".to_string()
            }),
            Frame::bulk_string("Pipeline"),
            Frame::Array(vec![Frame::bulk_string(pipeline.to_string())]),
        ]),
    ])
}

fn parse_fulltext_aggregate_reducer(
    frame: &Frame,
    idx: &mut usize,
) -> Result<FullTextAggregateReducer, Error> {
    if upper_arg(frame, *idx)?.as_str() != "REDUCE" {
        return Err(Error::msg("ERR syntax error"));
    }
    let kind = match upper_arg(frame, *idx + 1)?.as_str() {
        "COUNT" => FullTextAggregateReducerKind::Count,
        "COUNT_DISTINCT" | "COUNT_DISTINCTISH" => FullTextAggregateReducerKind::CountDistinct,
        "SUM" => FullTextAggregateReducerKind::Sum,
        "AVG" => FullTextAggregateReducerKind::Avg,
        "MIN" => FullTextAggregateReducerKind::Min,
        "MAX" => FullTextAggregateReducerKind::Max,
        "FIRST_VALUE" => FullTextAggregateReducerKind::FirstValue,
        "TOLIST" => FullTextAggregateReducerKind::ToList,
        _ => return Err(Error::msg("ERR unsupported aggregate reducer")),
    };
    let arg_count = parse_usize_arg(frame, *idx + 2, "ERR invalid reducer argument count")?;
    *idx += 3;
    if *idx + arg_count > frame.arg_len() {
        return Err(Error::msg("ERR syntax error"));
    }
    let mut args = Vec::with_capacity(arg_count);
    for _ in 0..arg_count {
        args.push(arg(frame, *idx, "ERR invalid reducer argument")?);
        *idx += 1;
    }
    let alias = if *idx + 1 < frame.arg_len() && upper_arg(frame, *idx)?.as_str() == "AS" {
        let alias = arg(frame, *idx + 1, "ERR invalid reducer alias")?;
        *idx += 2;
        Some(alias)
    } else {
        None
    };
    Ok(FullTextAggregateReducer { kind, args, alias })
}

fn parse_schema_fields(frame: &Frame, mut idx: usize) -> Result<Vec<FullTextFieldSchema>, Error> {
    let mut schema = Vec::new();
    while idx < frame.arg_len() {
        let name = arg(frame, idx, "ERR invalid schema field")?;
        idx += 1;
        let mut options = FullTextFieldOptions::default();
        if idx < frame.arg_len() && upper_arg(frame, idx)?.as_str() == "AS" {
            options.alias = Some(arg(frame, idx + 1, "ERR invalid schema field alias")?);
            idx += 2;
        }
        if idx >= frame.arg_len() {
            return Err(Error::msg("ERR invalid schema field type"));
        }
        let kind = match upper_arg(frame, idx)?.as_str() {
            "TEXT" => FullTextFieldKind::Text,
            "TAG" => FullTextFieldKind::Tag,
            "NUMERIC" => FullTextFieldKind::Numeric,
            "GEO" => FullTextFieldKind::Geo,
            "GEOSHAPE" => {
                let coordinate_system = match upper_arg(frame, idx + 1)?.as_str() {
                    "FLAT" => FullTextGeoShapeCoordinateSystem::Flat,
                    "SPHERICAL" => FullTextGeoShapeCoordinateSystem::Spherical,
                    _ => return Err(Error::msg("ERR invalid GEOSHAPE coordinate system")),
                };
                options.geoshape_coordinate_system = Some(coordinate_system);
                idx += 1;
                FullTextFieldKind::GeoShape
            }
            "VECTOR" => {
                let algorithm = match upper_arg(frame, idx + 1)?.as_str() {
                    "FLAT" => FullTextVectorAlgorithm::Flat,
                    "HNSW" => FullTextVectorAlgorithm::Hnsw,
                    _ => return Err(Error::msg("ERR invalid VECTOR algorithm")),
                };
                let attr_count =
                    parse_usize_arg(frame, idx + 2, "ERR invalid VECTOR attribute count")?;
                let attr_start = idx + 3;
                if attr_count % 2 != 0 || attr_start + attr_count > frame.arg_len() {
                    return Err(Error::msg("ERR syntax error"));
                }
                let mut attributes = Vec::with_capacity(attr_count / 2);
                let mut attr_idx = attr_start;
                while attr_idx < attr_start + attr_count {
                    attributes.push((
                        upper_arg(frame, attr_idx)?,
                        arg(frame, attr_idx + 1, "ERR invalid VECTOR attribute")?,
                    ));
                    attr_idx += 2;
                }
                options.vector = Some(FullTextVectorOptions {
                    algorithm,
                    attributes,
                });
                idx += 2 + attr_count;
                FullTextFieldKind::Vector
            }
            _ => return Err(Error::msg("ERR invalid schema field type")),
        };
        idx += 1;

        while idx < frame.arg_len() {
            match upper_arg(frame, idx)?.as_str() {
                "SORTABLE" => {
                    options.sortable = true;
                    idx += 1;
                    if idx < frame.arg_len() && upper_arg(frame, idx)?.as_str() == "UNF" {
                        options.sortable_unf = true;
                        idx += 1;
                    }
                }
                "NOINDEX" => {
                    options.noindex = true;
                    idx += 1;
                }
                "WEIGHT" if matches!(kind, FullTextFieldKind::Text) => {
                    let weight = arg(frame, idx + 1, "ERR invalid schema field weight")?
                        .parse::<f32>()
                        .map_err(|_| Error::msg("ERR invalid schema field weight"))?;
                    if !weight.is_finite() || weight <= 0.0 {
                        return Err(Error::msg("ERR invalid schema field weight"));
                    }
                    options.weight = Some(weight);
                    idx += 2;
                }
                "NOSTEM" => {
                    options.nostem = true;
                    idx += 1;
                }
                "PHONETIC" => {
                    options.phonetic = Some(arg(
                        frame,
                        idx + 1,
                        "ERR invalid schema field phonetic matcher",
                    )?);
                    idx += 2;
                }
                "SEPARATOR" => {
                    options.separator =
                        Some(arg(frame, idx + 1, "ERR invalid schema field separator")?);
                    idx += 2;
                }
                "CASESENSITIVE" => {
                    options.case_sensitive = true;
                    idx += 1;
                }
                "WITHSUFFIXTRIE" => {
                    options.with_suffix_trie = true;
                    idx += 1;
                }
                "INDEXEMPTY" => {
                    options.index_empty = true;
                    idx += 1;
                }
                "INDEXMISSING" => {
                    options.index_missing = true;
                    idx += 1;
                }
                _ => break,
            }
        }

        schema.push(FullTextFieldSchema {
            name,
            kind,
            options,
        });
    }
    if schema.is_empty() {
        return Err(Error::msg("ERR invalid fulltext schema"));
    }
    Ok(schema)
}

fn arg(frame: &Frame, idx: usize, error: &str) -> Result<String, Error> {
    frame
        .get_arg(idx)
        .ok_or_else(|| Error::msg(error.to_string()))
}

fn collect_args(frame: &Frame, start: usize, error: &str) -> Result<Vec<String>, Error> {
    if start >= frame.arg_len() {
        return Err(Error::msg("ERR syntax error"));
    }
    (start..frame.arg_len())
        .map(|idx| arg(frame, idx, error))
        .collect()
}

fn default_fulltext_search_options() -> FullTextSearchOptions {
    FullTextSearchOptions {
        offset: 0,
        limit: 10,
        return_fields: None,
        no_content: false,
        with_scores: false,
        with_payloads: false,
        with_sort_keys: false,
        filters: Vec::new(),
        geo_filters: Vec::new(),
        in_keys: None,
        in_fields: None,
        sort_by: None,
        timeout_ms: None,
        slop: None,
        inorder: false,
        language: None,
        payload: None,
        scorer: FullTextScorer::Bm25Std,
        summarize: false,
        highlight: false,
        explain_score: false,
        params: HashMap::new(),
        dialect: 2,
        dialect_explicit: false,
    }
}

fn upper_arg(frame: &Frame, idx: usize) -> Result<String, Error> {
    Ok(arg(frame, idx, "ERR syntax error")?.to_ascii_uppercase())
}

fn parse_usize_arg(frame: &Frame, idx: usize, error: &str) -> Result<usize, Error> {
    arg(frame, idx, error)?
        .parse::<usize>()
        .map_err(|_| Error::msg(error.to_string()))
}

fn parse_u64_arg(frame: &Frame, idx: usize, error: &str) -> Result<u64, Error> {
    arg(frame, idx, error)?
        .parse::<u64>()
        .map_err(|_| Error::msg(error.to_string()))
}

fn parse_f64_arg(frame: &Frame, idx: usize, error: &str) -> Result<f64, Error> {
    arg(frame, idx, error)?
        .parse::<f64>()
        .map_err(|_| Error::msg(error.to_string()))
}

fn parse_search_bound_arg(
    frame: &Frame,
    idx: usize,
    error: &str,
) -> Result<FullTextSearchBound, Error> {
    let raw = arg(frame, idx, error)?;
    let (exclusive, value) = raw
        .strip_prefix('(')
        .map(|value| (true, value))
        .unwrap_or((false, raw.as_str()));
    match value.to_ascii_lowercase().as_str() {
        "-inf" => Ok(FullTextSearchBound::NegInf),
        "+inf" | "inf" => Ok(FullTextSearchBound::PosInf),
        _ => {
            let number = value
                .parse::<f64>()
                .map_err(|_| Error::msg(error.to_string()))?;
            if !number.is_finite() {
                return Err(Error::msg(error.to_string()));
            }
            if exclusive {
                Ok(FullTextSearchBound::Exclusive(number))
            } else {
                Ok(FullTextSearchBound::Inclusive(number))
            }
        }
    }
}

fn skip_search_display_options(frame: &Frame, mut idx: usize) -> usize {
    while idx < frame.arg_len() {
        match upper_arg(frame, idx).unwrap_or_default().as_str() {
            "FIELDS" => {
                let count =
                    parse_usize_arg(frame, idx + 1, "ERR invalid FIELDS count").unwrap_or(0);
                idx = (idx + 2 + count).min(frame.arg_len());
            }
            "FRAGS" | "LEN" => idx = (idx + 2).min(frame.arg_len()),
            "SEPARATOR" | "TAGS" => idx = (idx + 2).min(frame.arg_len()),
            _ => break,
        }
    }
    idx
}

