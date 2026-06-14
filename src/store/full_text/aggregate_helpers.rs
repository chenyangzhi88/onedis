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
                        .unwrap_or_else(|_| FullTextAggregateValue::Null)
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
                            .unwrap_or_else(|_| FullTextAggregateValue::Null)
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

fn eval_fulltext_aggregate_filter(
    expression: &str,
    row: &FullTextAggregateRow,
) -> Result<bool, Error> {
    for op in ["!=", ">=", "<=", "==", "=", ">", "<"] {
        if let Some(idx) = find_fulltext_aggregate_operator(expression, op) {
            let left = eval_fulltext_aggregate_expression(&expression[..idx], row)?;
            let right = eval_fulltext_aggregate_expression(&expression[idx + op.len()..], row)?;
            return compare_fulltext_aggregate_values(&left, &right, op);
        }
    }
    Ok(fulltext_aggregate_value_truthy(
        &eval_fulltext_aggregate_expression(expression, row)?,
    ))
}

fn eval_fulltext_aggregate_expression(
    expression: &str,
    row: &FullTextAggregateRow,
) -> Result<FullTextAggregateValue, Error> {
    let expression = trim_wrapping_parens(expression.trim());
    if expression.is_empty() {
        return Err(Error::msg("ERR invalid aggregate expression"));
    }
    if let Some(value) = parse_quoted_fulltext_aggregate_string(expression) {
        return Ok(FullTextAggregateValue::String(value));
    }
    if let Ok(number) = expression.parse::<f64>()
        && number.is_finite()
    {
        return Ok(FullTextAggregateValue::Number(number));
    }
    if let Some(value) = eval_fulltext_aggregate_function(expression, row)? {
        return Ok(value);
    }
    if let Some((idx, op)) = find_fulltext_aggregate_arithmetic(expression, &['+', '-']) {
        let left = fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(
            &expression[..idx],
            row,
        )?)?;
        let right = fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(
            &expression[idx + op.len_utf8()..],
            row,
        )?)?;
        return Ok(FullTextAggregateValue::Number(if op == '+' {
            left + right
        } else {
            left - right
        }));
    }
    if let Some((idx, op)) = find_fulltext_aggregate_arithmetic(expression, &['*', '/']) {
        let left = fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(
            &expression[..idx],
            row,
        )?)?;
        let right = fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(
            &expression[idx + op.len_utf8()..],
            row,
        )?)?;
        return Ok(FullTextAggregateValue::Number(if op == '*' {
            left * right
        } else {
            left / right
        }));
    }
    let field = normalize_fulltext_aggregate_field(expression);
    if let Some(value) = row.values.get(&field) {
        return Ok(value.clone());
    }
    Ok(FullTextAggregateValue::String(expression.to_string()))
}

fn eval_fulltext_aggregate_function(
    expression: &str,
    row: &FullTextAggregateRow,
) -> Result<Option<FullTextAggregateValue>, Error> {
    let Some(open) = expression.find('(') else {
        return Ok(None);
    };
    if !expression.ends_with(')') {
        return Ok(None);
    }
    let name = expression[..open].trim().to_ascii_lowercase();
    let inner = &expression[open + 1..expression.len() - 1];
    let value = eval_fulltext_aggregate_expression(inner, row)?;
    Ok(Some(match name.as_str() {
        "tolower" | "lower" => FullTextAggregateValue::String(
            fulltext_aggregate_value_to_string(&value).to_lowercase(),
        ),
        "toupper" | "upper" => FullTextAggregateValue::String(
            fulltext_aggregate_value_to_string(&value).to_uppercase(),
        ),
        "sqrt" => {
            FullTextAggregateValue::Number(fulltext_aggregate_value_to_number(&value)?.sqrt())
        }
        "abs" => FullTextAggregateValue::Number(fulltext_aggregate_value_to_number(&value)?.abs()),
        "floor" => {
            FullTextAggregateValue::Number(fulltext_aggregate_value_to_number(&value)?.floor())
        }
        "ceil" => {
            FullTextAggregateValue::Number(fulltext_aggregate_value_to_number(&value)?.ceil())
        }
        _ => return Err(Error::msg("ERR unsupported aggregate expression function")),
    }))
}

fn find_fulltext_aggregate_operator(expression: &str, op: &str) -> Option<usize> {
    find_fulltext_aggregate_top_level(expression, &[op])
}

fn find_fulltext_aggregate_arithmetic(expression: &str, ops: &[char]) -> Option<(usize, char)> {
    let mut depth = 0usize;
    let mut quote = None;
    for (idx, ch) in expression.char_indices().rev() {
        if quote.is_some() {
            if quote == Some(ch) {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            ')' => depth += 1,
            '(' => depth = depth.saturating_sub(1),
            _ if depth == 0 && ops.contains(&ch) => {
                if ch == '-' || ch == '+' {
                    let before = expression[..idx].trim_end();
                    if before.is_empty()
                        || before.ends_with(['+', '-', '*', '/', '(', '>', '<', '='])
                    {
                        continue;
                    }
                }
                return Some((idx, ch));
            }
            _ => {}
        }
    }
    None
}

fn find_fulltext_aggregate_top_level(expression: &str, ops: &[&str]) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote = None;
    for (idx, ch) in expression.char_indices() {
        if quote.is_some() {
            if quote == Some(ch) {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && ops.iter().any(|op| expression[idx..].starts_with(op)) => {
                return Some(idx);
            }
            _ => {}
        }
    }
    None
}

fn trim_wrapping_parens(mut expression: &str) -> &str {
    loop {
        let trimmed = expression.trim();
        if !(trimmed.starts_with('(') && trimmed.ends_with(')')) {
            return trimmed;
        }
        let mut depth = 0usize;
        let mut wraps = true;
        for (idx, ch) in trimmed.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && idx + 1 < trimmed.len() {
                        wraps = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if wraps {
            expression = &trimmed[1..trimmed.len() - 1];
        } else {
            return trimmed;
        }
    }
}

fn parse_quoted_fulltext_aggregate_string(expression: &str) -> Option<String> {
    let quote = expression.chars().next()?;
    if (quote == '"' || quote == '\'') && expression.ends_with(quote) && expression.len() >= 2 {
        Some(expression[1..expression.len() - 1].to_string())
    } else {
        None
    }
}

fn compare_fulltext_aggregate_values(
    left: &FullTextAggregateValue,
    right: &FullTextAggregateValue,
    op: &str,
) -> Result<bool, Error> {
    let ordering = match (
        fulltext_aggregate_value_to_number(left),
        fulltext_aggregate_value_to_number(right),
    ) {
        (Ok(left), Ok(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => {
            fulltext_aggregate_value_to_string(left).cmp(&fulltext_aggregate_value_to_string(right))
        }
    };
    Ok(match op {
        "==" | "=" => ordering == std::cmp::Ordering::Equal,
        "!=" => ordering != std::cmp::Ordering::Equal,
        ">" => ordering == std::cmp::Ordering::Greater,
        ">=" => matches!(
            ordering,
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ),
        "<" => ordering == std::cmp::Ordering::Less,
        "<=" => matches!(
            ordering,
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
        ),
        _ => return Err(Error::msg("ERR invalid aggregate filter")),
    })
}

fn fulltext_aggregate_value_truthy(value: &FullTextAggregateValue) -> bool {
    match value {
        FullTextAggregateValue::Null => false,
        FullTextAggregateValue::String(value) => !value.is_empty() && value != "0",
        FullTextAggregateValue::Number(value) => *value != 0.0,
        FullTextAggregateValue::List(values) => !values.is_empty(),
    }
}

fn fulltext_aggregate_value_to_number(value: &FullTextAggregateValue) -> Result<f64, Error> {
    match value {
        FullTextAggregateValue::Number(value) => Ok(*value),
        FullTextAggregateValue::String(value) => value
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR aggregate expression expected numeric value")),
        FullTextAggregateValue::Null | FullTextAggregateValue::List(_) => Err(Error::msg(
            "ERR aggregate expression expected numeric value",
        )),
    }
}

fn fulltext_aggregate_value_to_string(value: &FullTextAggregateValue) -> String {
    match value {
        FullTextAggregateValue::Null => String::new(),
        FullTextAggregateValue::String(value) => value.clone(),
        FullTextAggregateValue::Number(value) => format_fulltext_aggregate_number(*value),
        FullTextAggregateValue::List(values) => values
            .iter()
            .map(fulltext_aggregate_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn format_fulltext_aggregate_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn compare_fulltext_aggregate_rows(
    left: &FullTextAggregateRow,
    right: &FullTextAggregateRow,
    sort_by: &[FullTextAggregateSortBy],
) -> std::cmp::Ordering {
    for sort_by in sort_by {
        let field = normalize_fulltext_aggregate_field(&sort_by.field);
        let left_value = left
            .values
            .get(&field)
            .unwrap_or(&FullTextAggregateValue::Null);
        let right_value = right
            .values
            .get(&field)
            .unwrap_or(&FullTextAggregateValue::Null);
        let ordering = match (
            fulltext_aggregate_value_to_number(left_value),
            fulltext_aggregate_value_to_number(right_value),
        ) {
            (Ok(left), Ok(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => fulltext_aggregate_value_to_string(left_value)
                .cmp(&fulltext_aggregate_value_to_string(right_value)),
        };
        let ordering = if sort_by.asc {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    fulltext_aggregate_value_to_string(
        left.values
            .get("__key")
            .unwrap_or(&FullTextAggregateValue::Null),
    )
    .cmp(&fulltext_aggregate_value_to_string(
        right
            .values
            .get("__key")
            .unwrap_or(&FullTextAggregateValue::Null),
    ))
}

fn fulltext_aggregate_frame(total: usize, rows: Vec<FullTextAggregateRow>) -> Frame {
    let mut out = Vec::with_capacity(rows.len() + 1);
    out.push(Frame::Integer(total as i64));
    for row in rows {
        let mut fields = Vec::with_capacity(row.output.len() * 2);
        for (field, value) in row.output {
            fields.push(Frame::bulk_string(field));
            fields.push(fulltext_aggregate_value_frame(value));
        }
        out.push(Frame::Array(fields));
    }
    Frame::Array(out)
}

fn fulltext_aggregate_value_frame(value: FullTextAggregateValue) -> Frame {
    match value {
        FullTextAggregateValue::Null => Frame::Null,
        FullTextAggregateValue::String(value) => Frame::bulk_string(value),
        FullTextAggregateValue::Number(value) => {
            Frame::bulk_string(format_fulltext_aggregate_number(value))
        }
        FullTextAggregateValue::List(values) => Frame::Array(
            values
                .into_iter()
                .map(fulltext_aggregate_value_frame)
                .collect(),
        ),
    }
}

fn normalize_fulltext_aggregate_field(field: &str) -> String {
    field.trim().trim_start_matches('@').to_string()
}

