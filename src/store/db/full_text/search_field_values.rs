use super::*;
pub(super) fn fulltext_fields_frame(
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
                    &requested.identifier,
                    &value,
                    options,
                    display_terms,
                )));
            }
        }
        return Frame::Array(values);
    }
    for (field, value) in fields {
        values.push(Frame::bulk_string(field.clone()));
        values.push(Frame::bulk_string(fulltext_display_value(
            &field,
            &value,
            options,
            display_terms,
        )));
    }
    Frame::Array(values)
}

pub(super) fn fulltext_field_value(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.clone())
}

pub(super) fn fulltext_field_values<'a>(
    fields: &'a [(String, String)],
    name: &'a str,
) -> impl Iterator<Item = &'a str> {
    fields
        .iter()
        .filter(move |(field, _)| field == name)
        .map(|(_, value)| value.as_str())
}

pub(super) fn fulltext_sort_field_value(
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    name: &str,
) -> Option<String> {
    let schema_field = meta
        .schema
        .iter()
        .find(|field| field.name == name || field.attribute_name() == name)?;
    let value = fulltext_field_value(fields, name)
        .or_else(|| fulltext_field_value(fields, schema_field.attribute_name()))
        .or_else(|| fulltext_field_value(fields, &schema_field.name))?;
    match schema_field.kind {
        FullTextFieldKind::Numeric => value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(|_| value),
        FullTextFieldKind::Text | FullTextFieldKind::Tag => Some(value),
        _ => None,
    }
}

pub(super) fn fulltext_fields_match_filters(
    fields: &[(String, String)],
    filters: &[FullTextSearchNumericFilter],
) -> bool {
    filters.iter().all(|filter| {
        fulltext_field_values(fields, &filter.field).any(|value| {
            value.parse::<f64>().ok().is_some_and(|value| {
                fulltext_bound_allows(value, filter.min, true)
                    && fulltext_bound_allows(value, filter.max, false)
            })
        })
    })
}

pub(super) fn fulltext_fields_match_geo_filters(
    fields: &[(String, String)],
    filters: &[FullTextSearchGeoFilter],
) -> Result<bool, Error> {
    for filter in filters {
        let mut matched = false;
        for value in fulltext_field_values(fields, &filter.field) {
            if fulltext_geo_value_within(
                value,
                filter.lon,
                filter.lat,
                filter.radius,
                &filter.unit,
            )? {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}
