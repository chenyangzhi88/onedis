fn fulltext_validate_search_geo_filters(
    meta: &FullTextIndexMeta,
    filters: &[FullTextSearchGeoFilter],
) -> Result<(), Error> {
    for filter in filters {
        let Some(field) = fulltext_schema_field(meta, &filter.field) else {
            return Err(Error::msg("ERR invalid geo field"));
        };
        if !matches!(field.kind, FullTextFieldKind::Geo) {
            return Err(Error::msg("ERR invalid geo field"));
        }
        if !filter.lon.is_finite() || !filter.lat.is_finite() || !filter.radius.is_finite() {
            return Err(Error::msg("ERR invalid geo filter"));
        }
        if filter.radius < 0.0 {
            return Err(Error::msg("ERR invalid geo radius"));
        }
        let _ = fulltext_geo_unit_meters(&filter.unit)?;
    }
    Ok(())
}

fn fulltext_validate_geo_query_ast(
    meta: &FullTextIndexMeta,
    ast: &FullTextQueryAst,
) -> Result<(), Error> {
    match ast {
        FullTextQueryAst::Geo {
            field,
            lon,
            lat,
            radius,
            unit,
        } => {
            let Some(schema) = fulltext_schema_field(meta, field) else {
                return Err(Error::msg("ERR invalid geo field"));
            };
            if !matches!(schema.kind, FullTextFieldKind::Geo) {
                return Err(Error::msg("ERR invalid geo field"));
            }
            if !lon.is_finite() || !lat.is_finite() || !radius.is_finite() {
                return Err(Error::msg("ERR invalid geo filter"));
            }
            if *radius < 0.0 {
                return Err(Error::msg("ERR invalid geo radius"));
            }
            let _ = fulltext_geo_unit_meters(unit)?;
            Ok(())
        }
        FullTextQueryAst::GeoShape { field, shape, .. } => {
            let Some(schema) = fulltext_schema_field(meta, field) else {
                return Err(Error::msg("ERR invalid geoshape field"));
            };
            if !matches!(schema.kind, FullTextFieldKind::GeoShape) {
                return Err(Error::msg("ERR invalid geoshape field"));
            }
            let _ = parse_fulltext_wkt(shape)?;
            Ok(())
        }
        FullTextQueryAst::Field { expr, .. }
        | FullTextQueryAst::Not(expr)
        | FullTextQueryAst::Optional(expr)
        | FullTextQueryAst::Attributed { expr, .. } => fulltext_validate_geo_query_ast(meta, expr),
        FullTextQueryAst::And(children) | FullTextQueryAst::Or(children) => {
            for child in children {
                fulltext_validate_geo_query_ast(meta, child)?;
            }
            Ok(())
        }
        FullTextQueryAst::All
        | FullTextQueryAst::Text(_)
        | FullTextQueryAst::Phrase(_)
        | FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::VectorKnn { .. }
        | FullTextQueryAst::VectorRange { .. } => Ok(()),
    }
}
