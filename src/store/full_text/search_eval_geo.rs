fn fulltext_fields_frame(
    fields: Vec<(String, String)>,
    return_fields: Option<&[FullTextReturnField]>,
    options: &FullTextSearchOptions,
    display_terms: &[String],
) -> Frame {
    let mut values = Vec::new();
    if let Some(return_fields) = return_fields {
        for requested in return_fields {
            if let Some(value) = fulltext_field_value(&fields, &requested.identifier) {
                values.push(Frame::bulk_string(
                    requested
                        .alias
                        .clone()
                        .unwrap_or_else(|| requested.identifier.clone()),
                ));
                values.push(Frame::bulk_string(fulltext_display_value(
                    &value,
                    options,
                    display_terms,
                )));
            }
        }
        return Frame::Array(values);
    }
    for (field, value) in fields {
        values.push(Frame::bulk_string(field));
        values.push(Frame::bulk_string(fulltext_display_value(
            &value,
            options,
            display_terms,
        )));
    }
    Frame::Array(values)
}

fn fulltext_field_value(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.clone())
}

fn fulltext_fields_match_filters(
    fields: &[(String, String)],
    filters: &[FullTextSearchNumericFilter],
) -> bool {
    filters.iter().all(|filter| {
        let Some(value) =
            fulltext_field_value(fields, &filter.field).and_then(|value| value.parse::<f64>().ok())
        else {
            return false;
        };
        fulltext_bound_allows(value, filter.min, true)
            && fulltext_bound_allows(value, filter.max, false)
    })
}

fn fulltext_fields_match_geo_filters(
    fields: &[(String, String)],
    filters: &[FullTextSearchGeoFilter],
) -> Result<bool, Error> {
    for filter in filters {
        let Some(value) = fulltext_field_value(fields, &filter.field) else {
            return Ok(false);
        };
        if !fulltext_geo_value_within(&value, filter.lon, filter.lat, filter.radius, &filter.unit)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn fulltext_eval_ast_against_fields(
    ast: &FullTextQueryAst,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> Result<bool, Error> {
    match ast {
        FullTextQueryAst::All => Ok(true),
        FullTextQueryAst::Text(term) => Ok(fulltext_any_text_field_matches(term, fields, meta)),
        FullTextQueryAst::Phrase(phrase) => {
            let phrase = phrase.to_lowercase();
            Ok(fields
                .iter()
                .filter(|(field, _)| {
                    fulltext_schema_field(meta, field)
                        .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                })
                .any(|(_, value)| value.to_lowercase().contains(&phrase)))
        }
        FullTextQueryAst::Prefix(prefix) => {
            let prefix = prefix.to_lowercase();
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && fulltext_tokenize(value)
                        .iter()
                        .any(|token| token.starts_with(&prefix))
            }))
        }
        FullTextQueryAst::Wildcard(pattern) => {
            let regex = regex::Regex::new(&fulltext_wildcard_to_regex(pattern))
                .map_err(|_| Error::msg("ERR invalid wildcard pattern"))?;
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && regex.is_match(&value.to_lowercase())
            }))
        }
        FullTextQueryAst::Fuzzy(term) => {
            let term = term.to_lowercase();
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && fulltext_tokenize(value)
                        .iter()
                        .any(|token| fulltext_edit_distance(token, &term) <= 1)
            }))
        }
        FullTextQueryAst::Tag { field, values } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            Ok(values.iter().any(|expected| {
                split_tag_values(&value)
                    .iter()
                    .any(|actual| actual.eq_ignore_ascii_case(expected))
                    || value.eq_ignore_ascii_case(expected)
            }))
        }
        FullTextQueryAst::Numeric { field, min, max } => {
            let Some(value) =
                fulltext_field_value(fields, field).and_then(|value| value.parse::<f64>().ok())
            else {
                return Ok(false);
            };
            Ok(fulltext_numeric_bound_allows(value, *min, true)
                && fulltext_numeric_bound_allows(value, *max, false))
        }
        FullTextQueryAst::Geo {
            field,
            lon,
            lat,
            radius,
            unit,
        } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            fulltext_geo_value_within(&value, *lon, *lat, *radius, unit)
        }
        FullTextQueryAst::GeoShape {
            field,
            relation,
            shape,
        } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            fulltext_geoshape_relation_matches(&value, relation, shape)
        }
        FullTextQueryAst::Field {
            fields: scope,
            expr,
        } => {
            let scoped = fields
                .iter()
                .filter(|(field, _)| scope.iter().any(|scope| scope == field))
                .cloned()
                .collect::<Vec<_>>();
            fulltext_eval_ast_against_fields(expr, &scoped, meta, options)
        }
        FullTextQueryAst::And(children) => {
            for child in children {
                if !fulltext_eval_ast_against_fields(child, fields, meta, options)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        FullTextQueryAst::Or(children) => {
            for child in children {
                if fulltext_eval_ast_against_fields(child, fields, meta, options)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        FullTextQueryAst::Not(child) => Ok(!fulltext_eval_ast_against_fields(
            child, fields, meta, options,
        )?),
        FullTextQueryAst::Optional(_) => Ok(true),
        FullTextQueryAst::Attributed { expr, .. } => {
            fulltext_eval_ast_against_fields(expr, fields, meta, options)
        }
        FullTextQueryAst::VectorRange { .. } | FullTextQueryAst::VectorKnn { .. } => Ok(false),
    }
}

fn fulltext_any_text_field_matches(
    term: &str,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
) -> bool {
    fields.iter().any(|(field, value)| {
        let Some(schema) = fulltext_schema_field(meta, field) else {
            return false;
        };
        if !matches!(schema.kind, FullTextFieldKind::Text) {
            return false;
        }
        let settings = FullTextTextFieldSettings {
            nostem: schema.options.nostem,
            phonetic: schema.options.phonetic.is_some(),
            with_suffix_trie: schema.options.with_suffix_trie,
            stopwords: meta
                .index_options
                .stopwords
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|word| word.to_lowercase())
                .collect(),
        };
        let materialized = fulltext_materialize_text(value, &settings);
        let variants = fulltext_query_term_variants(term, Some(&settings), &HashMap::new());
        variants.iter().any(|variant| {
            fulltext_tokenize(&materialized)
                .iter()
                .any(|token| token == variant)
        })
    })
}

fn fulltext_schema_field<'a>(
    meta: &'a FullTextIndexMeta,
    field: &str,
) -> Option<&'a FullTextFieldSchema> {
    meta.schema
        .iter()
        .find(|schema| schema.name == field || schema.attribute_name() == field)
}

fn fulltext_geo_value_within(
    value: &str,
    lon: f64,
    lat: f64,
    radius: f64,
    unit: &str,
) -> Result<bool, Error> {
    if radius < 0.0 || !radius.is_finite() {
        return Err(Error::msg("ERR invalid geo radius"));
    }
    let (value_lon, value_lat) = parse_fulltext_geo_value(value)?;
    let radius_meters = radius * fulltext_geo_unit_meters(unit)?;
    Ok(fulltext_haversine_meters(lat, lon, value_lat, value_lon) <= radius_meters)
}

fn parse_fulltext_geo_value(value: &str) -> Result<(f64, f64), Error> {
    let parts = value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::msg("ERR invalid GEO value"));
    }
    let lon = parts[0]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid GEO value"))?;
    let lat = parts[1]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid GEO value"))?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(Error::msg("ERR invalid GEO value"));
    }
    Ok((lon, lat))
}

fn fulltext_geo_unit_meters(unit: &str) -> Result<f64, Error> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.344),
        "ft" => Ok(0.3048),
        _ => Err(Error::msg("ERR invalid geo unit")),
    }
}

fn fulltext_haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let radius_meters = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * radius_meters * a.sqrt().asin()
}

#[derive(Clone, Debug)]
enum FullTextGeometry {
    Point((f64, f64)),
    Polygon(Vec<(f64, f64)>),
}

fn fulltext_geoshape_relation_matches(
    value: &str,
    relation: &str,
    query_shape: &str,
) -> Result<bool, Error> {
    let value = parse_fulltext_wkt(value)?;
    let query = parse_fulltext_wkt(query_shape)?;
    match relation.to_ascii_uppercase().as_str() {
        "WITHIN" => Ok(fulltext_geometry_within(&value, &query)),
        "CONTAINS" => Ok(fulltext_geometry_contains(&value, &query)),
        _ => Err(Error::msg("ERR invalid GEOSHAPE relation")),
    }
}

fn parse_fulltext_wkt(raw: &str) -> Result<FullTextGeometry, Error> {
    let raw = raw.trim();
    let upper = raw.to_ascii_uppercase();
    if upper.starts_with("POINT") {
        let body = raw
            .trim_start_matches(|ch: char| ch.is_ascii_alphabetic())
            .trim();
        let body = body
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .ok_or_else(|| Error::msg("ERR invalid WKT"))?;
        return Ok(FullTextGeometry::Point(parse_fulltext_wkt_point(body)?));
    }
    if upper.starts_with("POLYGON") {
        let body = raw
            .trim_start_matches(|ch: char| ch.is_ascii_alphabetic())
            .trim();
        let body = body
            .strip_prefix("((")
            .and_then(|value| value.strip_suffix("))"))
            .ok_or_else(|| Error::msg("ERR invalid WKT"))?;
        let points = body
            .split(',')
            .map(parse_fulltext_wkt_point)
            .collect::<Result<Vec<_>, _>>()?;
        if points.len() < 4 {
            return Err(Error::msg("ERR invalid WKT polygon"));
        }
        return Ok(FullTextGeometry::Polygon(points));
    }
    Err(Error::msg("ERR unsupported WKT geometry"))
}

fn parse_fulltext_wkt_point(raw: &str) -> Result<(f64, f64), Error> {
    let parts = raw.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::msg("ERR invalid WKT point"));
    }
    let x = parts[0]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid WKT point"))?;
    let y = parts[1]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid WKT point"))?;
    if !x.is_finite() || !y.is_finite() {
        return Err(Error::msg("ERR invalid WKT point"));
    }
    Ok((x, y))
}

fn fulltext_geometry_within(value: &FullTextGeometry, query: &FullTextGeometry) -> bool {
    match (value, query) {
        (FullTextGeometry::Point(point), FullTextGeometry::Polygon(poly)) => {
            fulltext_point_in_polygon(*point, poly)
        }
        (FullTextGeometry::Point(left), FullTextGeometry::Point(right)) => left == right,
        (FullTextGeometry::Polygon(poly), FullTextGeometry::Polygon(container)) => poly
            .iter()
            .all(|point| fulltext_point_in_polygon(*point, container)),
        (FullTextGeometry::Polygon(_), FullTextGeometry::Point(_)) => false,
    }
}

fn fulltext_geometry_contains(value: &FullTextGeometry, query: &FullTextGeometry) -> bool {
    fulltext_geometry_within(query, value)
}

fn fulltext_point_in_polygon(point: (f64, f64), polygon: &[(f64, f64)]) -> bool {
    let (x, y) = point;
    let mut inside = false;
    let mut j = polygon.len() - 1;
    for i in 0..polygon.len() {
        let (xi, yi) = polygon[i];
        let (xj, yj) = polygon[j];
        let denom = yj - yi;
        if denom.abs() > f64::EPSILON
            && ((yi > y) != (yj > y))
            && (x < (xj - xi) * (y - yi) / denom + xi)
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}

fn fulltext_numeric_bound_allows(value: f64, bound: FullTextNumericBound, lower: bool) -> bool {
    match (bound, lower) {
        (FullTextNumericBound::NegInf, true) | (FullTextNumericBound::PosInf, false) => true,
        (FullTextNumericBound::NegInf, false) => false,
        (FullTextNumericBound::PosInf, true) => false,
        (FullTextNumericBound::Inclusive(bound), true) => value >= bound,
        (FullTextNumericBound::Inclusive(bound), false) => value <= bound,
        (FullTextNumericBound::Exclusive(bound), true) => value > bound,
        (FullTextNumericBound::Exclusive(bound), false) => value < bound,
    }
}

fn fulltext_bound_allows(value: f64, bound: FullTextSearchBound, lower: bool) -> bool {
    match (bound, lower) {
        (FullTextSearchBound::NegInf, true) | (FullTextSearchBound::PosInf, false) => true,
        (FullTextSearchBound::NegInf, false) => false,
        (FullTextSearchBound::PosInf, true) => false,
        (FullTextSearchBound::Inclusive(bound), true) => value >= bound,
        (FullTextSearchBound::Inclusive(bound), false) => value <= bound,
        (FullTextSearchBound::Exclusive(bound), true) => value > bound,
        (FullTextSearchBound::Exclusive(bound), false) => value < bound,
    }
}

