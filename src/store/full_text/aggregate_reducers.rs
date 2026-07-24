fn fulltext_aggregate_group(
    rows: Vec<FullTextAggregateRow>,
    fields: &[String],
    reducers: &[FullTextAggregateReducer],
) -> Result<Vec<FullTextAggregateRow>, Error> {
    let mut groups: BTreeMap<Vec<String>, Vec<FullTextAggregateRow>> = BTreeMap::new();
    for row in rows {
        let key = fields
            .iter()
            .map(|field| {
                let field = normalize_fulltext_aggregate_field(field);
                row.values
                    .get(&field)
                    .map(fulltext_aggregate_value_to_string)
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(row);
    }

    let mut out = Vec::new();
    for (key, members) in groups {
        let mut values = HashMap::new();
        let mut output = Vec::new();
        for (idx, field) in fields.iter().enumerate() {
            let field = normalize_fulltext_aggregate_field(field);
            let value = FullTextAggregateValue::String(key.get(idx).cloned().unwrap_or_default());
            values.insert(field.clone(), value.clone());
            output.push((field, value));
        }
        for reducer in reducers {
            let (name, value) = fulltext_aggregate_reduce(reducer, &members)?;
            values.insert(name.clone(), value.clone());
            output.push((name, value));
        }
        out.push(FullTextAggregateRow { values, output });
    }
    Ok(out)
}

fn fulltext_aggregate_reduce(
    reducer: &FullTextAggregateReducer,
    rows: &[FullTextAggregateRow],
) -> Result<(String, FullTextAggregateValue), Error> {
    let default_name = fulltext_aggregate_reducer_default_name(reducer);
    let name = reducer.alias.clone().unwrap_or(default_name);
    let value = match reducer.kind {
        FullTextAggregateReducerKind::Count => FullTextAggregateValue::Number(rows.len() as f64),
        FullTextAggregateReducerKind::CountDistinct => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR COUNT_DISTINCT requires one argument"))?;
            let mut seen = HashSet::new();
            for row in rows {
                seen.insert(fulltext_aggregate_arg_value(row, arg));
            }
            FullTextAggregateValue::Number(seen.len() as f64)
        }
        FullTextAggregateReducerKind::Sum => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR SUM requires one argument"))?;
            FullTextAggregateValue::Number(
                rows.iter()
                    .filter_map(|row| fulltext_aggregate_arg_number(row, arg).ok())
                    .sum(),
            )
        }
        FullTextAggregateReducerKind::Avg => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR AVG requires one argument"))?;
            let values = rows
                .iter()
                .filter_map(|row| fulltext_aggregate_arg_number(row, arg).ok())
                .collect::<Vec<_>>();
            let avg = if values.is_empty() {
                0.0
            } else {
                values.iter().sum::<f64>() / values.len() as f64
            };
            FullTextAggregateValue::Number(avg)
        }
        FullTextAggregateReducerKind::Min => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR MIN requires one argument"))?;
            let value = rows
                .iter()
                .filter_map(|row| fulltext_aggregate_arg_number(row, arg).ok())
                .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(0.0);
            FullTextAggregateValue::Number(value)
        }
        FullTextAggregateReducerKind::Max => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR MAX requires one argument"))?;
            let value = rows
                .iter()
                .filter_map(|row| fulltext_aggregate_arg_number(row, arg).ok())
                .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(0.0);
            FullTextAggregateValue::Number(value)
        }
        FullTextAggregateReducerKind::FirstValue => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR FIRST_VALUE requires one argument"))?;
            rows.first()
                .map(|row| {
                    eval_fulltext_aggregate_expression(arg, row)
                        .unwrap_or(FullTextAggregateValue::Null)
                })
                .unwrap_or(FullTextAggregateValue::Null)
        }
        FullTextAggregateReducerKind::ToList => {
            let arg = reducer
                .args
                .first()
                .ok_or_else(|| Error::msg("ERR TOLIST requires one argument"))?;
            FullTextAggregateValue::List(
                rows.iter()
                    .map(|row| {
                        eval_fulltext_aggregate_expression(arg, row)
                            .unwrap_or(FullTextAggregateValue::Null)
                    })
                    .collect(),
            )
        }
    };
    Ok((name, value))
}

fn fulltext_aggregate_reducer_default_name(reducer: &FullTextAggregateReducer) -> String {
    match reducer.kind {
        FullTextAggregateReducerKind::Count => "count".to_string(),
        FullTextAggregateReducerKind::CountDistinct => "count_distinct".to_string(),
        FullTextAggregateReducerKind::Sum => "sum".to_string(),
        FullTextAggregateReducerKind::Avg => "avg".to_string(),
        FullTextAggregateReducerKind::Min => "min".to_string(),
        FullTextAggregateReducerKind::Max => "max".to_string(),
        FullTextAggregateReducerKind::FirstValue => "first_value".to_string(),
        FullTextAggregateReducerKind::ToList => "tolist".to_string(),
    }
}

fn fulltext_aggregate_arg_value(row: &FullTextAggregateRow, arg: &str) -> String {
    eval_fulltext_aggregate_expression(arg, row)
        .map(|value| fulltext_aggregate_value_to_string(&value))
        .unwrap_or_default()
}

fn fulltext_aggregate_arg_number(row: &FullTextAggregateRow, arg: &str) -> Result<f64, Error> {
    fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(arg, row)?)
}
