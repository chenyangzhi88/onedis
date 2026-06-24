fn fulltext_aggregate_row_from_hit(
    hit: FullTextLiveHit,
    load: Option<&[FullTextAggregateLoadField]>,
) -> Result<FullTextAggregateRow, Error> {
    let mut values = HashMap::new();
    values.insert(
        "__key".to_string(),
        FullTextAggregateValue::String(hit.key.clone()),
    );
    values.insert(
        "__score".to_string(),
        FullTextAggregateValue::Number(hit.score as f64),
    );
    for (field, value) in &hit.fields {
        values.insert(field.clone(), FullTextAggregateValue::String(value.clone()));
    }

    let mut output = Vec::new();
    if let Some(load) = load {
        for field in load {
            let source = normalize_fulltext_aggregate_field(&field.identifier);
            if source == "*" {
                output.extend(
                    hit.fields
                        .iter()
                        .cloned()
                        .map(|(field, value)| (field, FullTextAggregateValue::String(value))),
                );
                continue;
            }
            if let Some(value) = values.get(&source).cloned() {
                output.push((
                    field.alias.clone().unwrap_or_else(|| source.clone()),
                    value.clone(),
                ));
                if let Some(alias) = &field.alias {
                    values.insert(alias.clone(), value);
                }
            }
        }
    }

    Ok(FullTextAggregateRow { values, output })
}

fn fulltext_aggregate_set_output(
    row: &mut FullTextAggregateRow,
    field: String,
    value: FullTextAggregateValue,
) {
    if let Some((_, current)) = row.output.iter_mut().find(|(name, _)| name == &field) {
        *current = value;
    } else {
        row.output.push((field, value));
    }
}
