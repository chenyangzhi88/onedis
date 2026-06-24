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
