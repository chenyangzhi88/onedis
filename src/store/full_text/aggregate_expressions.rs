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
