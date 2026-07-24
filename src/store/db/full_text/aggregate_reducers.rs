use super::*;
pub(super) struct FullTextAggregateGroupState {
    fields: Vec<String>,
    reducers: Vec<FullTextAggregateReducer>,
    groups: BTreeMap<Vec<String>, Vec<FullTextAggregateAccumulator>>,
    estimated_bytes: usize,
    memory_budget: usize,
}

enum FullTextAggregateAccumulator {
    Count(u64),
    CountDistinct(HashSet<String>),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Option<f64>),
    Max(Option<f64>),
    FirstValue(Option<FullTextAggregateValue>),
    ToList(Vec<FullTextAggregateValue>),
}

impl FullTextAggregateGroupState {
    pub(super) fn new(
        fields: &[String],
        reducers: &[FullTextAggregateReducer],
        memory_budget: usize,
    ) -> Result<Self, Error> {
        for reducer in reducers {
            if !matches!(reducer.kind, FullTextAggregateReducerKind::Count)
                && reducer.args.is_empty()
            {
                return Err(Error::msg(format!(
                    "ERR {} requires one argument",
                    fulltext_aggregate_reducer_default_name(reducer).to_ascii_uppercase()
                )));
            }
        }
        Ok(Self {
            fields: fields.to_vec(),
            reducers: reducers.to_vec(),
            groups: BTreeMap::new(),
            estimated_bytes: 0,
            memory_budget,
        })
    }

    pub(super) fn push(&mut self, row: &FullTextAggregateRow) -> Result<(), Error> {
        let key = self
            .fields
            .iter()
            .map(|field| {
                let field = normalize_fulltext_aggregate_field(field);
                row.values
                    .get(&field)
                    .map(fulltext_aggregate_value_to_string)
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        if !self.groups.contains_key(&key) {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(std::mem::size_of::<Vec<String>>())
                .saturating_add(key.iter().map(String::len).sum::<usize>())
                .saturating_add(
                    self.reducers
                        .len()
                        .saturating_mul(std::mem::size_of::<FullTextAggregateAccumulator>()),
                );
            let accumulators = self
                .reducers
                .iter()
                .map(fulltext_aggregate_accumulator)
                .collect::<Result<Vec<_>, Error>>()?;
            self.groups.insert(key.clone(), accumulators);
        }
        let accumulators = self
            .groups
            .get_mut(&key)
            .ok_or_else(|| Error::msg("ERR fulltext aggregate group state missing"))?;
        for (accumulator, reducer) in accumulators.iter_mut().zip(&self.reducers) {
            self.estimated_bytes =
                self.estimated_bytes
                    .saturating_add(update_fulltext_aggregate_accumulator(
                        accumulator,
                        reducer,
                        row,
                    )?);
        }
        if self.estimated_bytes > self.memory_budget {
            return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Vec<FullTextAggregateRow>, Error> {
        if self.estimated_bytes.saturating_mul(2) > self.memory_budget {
            return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
        }
        let mut out = Vec::with_capacity(self.groups.len());
        for (key, accumulators) in self.groups {
            let mut values = HashMap::new();
            let mut output = Vec::new();
            for (idx, field) in self.fields.iter().enumerate() {
                let field = normalize_fulltext_aggregate_field(field);
                let value =
                    FullTextAggregateValue::String(key.get(idx).cloned().unwrap_or_default());
                values.insert(field.clone(), value.clone());
                output.push((field, value));
            }
            for (reducer, accumulator) in self.reducers.iter().zip(accumulators) {
                let name = reducer
                    .alias
                    .clone()
                    .unwrap_or_else(|| fulltext_aggregate_reducer_default_name(reducer));
                let value = finish_fulltext_aggregate_accumulator(accumulator);
                values.insert(name.clone(), value.clone());
                output.push((name, value));
            }
            out.push(FullTextAggregateRow { values, output });
        }
        Ok(out)
    }
}

fn fulltext_aggregate_accumulator(
    reducer: &FullTextAggregateReducer,
) -> Result<FullTextAggregateAccumulator, Error> {
    Ok(match reducer.kind {
        FullTextAggregateReducerKind::Count => FullTextAggregateAccumulator::Count(0),
        FullTextAggregateReducerKind::CountDistinct => {
            FullTextAggregateAccumulator::CountDistinct(HashSet::new())
        }
        FullTextAggregateReducerKind::Sum => FullTextAggregateAccumulator::Sum(0.0),
        FullTextAggregateReducerKind::Avg => {
            FullTextAggregateAccumulator::Avg { sum: 0.0, count: 0 }
        }
        FullTextAggregateReducerKind::Min => FullTextAggregateAccumulator::Min(None),
        FullTextAggregateReducerKind::Max => FullTextAggregateAccumulator::Max(None),
        FullTextAggregateReducerKind::FirstValue => FullTextAggregateAccumulator::FirstValue(None),
        FullTextAggregateReducerKind::ToList => FullTextAggregateAccumulator::ToList(Vec::new()),
    })
}

fn update_fulltext_aggregate_accumulator(
    accumulator: &mut FullTextAggregateAccumulator,
    reducer: &FullTextAggregateReducer,
    row: &FullTextAggregateRow,
) -> Result<usize, Error> {
    let argument = reducer.args.first().map(String::as_str);
    Ok(match accumulator {
        FullTextAggregateAccumulator::Count(count) => {
            *count = count.saturating_add(1);
            0
        }
        FullTextAggregateAccumulator::CountDistinct(seen) => {
            let value = fulltext_aggregate_arg_value(
                row,
                argument.ok_or_else(|| Error::msg("ERR COUNT_DISTINCT requires one argument"))?,
            );
            if seen.insert(value.clone()) {
                std::mem::size_of::<String>()
                    .saturating_add(value.len())
                    .saturating_add(2 * std::mem::size_of::<usize>())
            } else {
                0
            }
        }
        FullTextAggregateAccumulator::Sum(sum) => {
            if let Some(value) =
                argument.and_then(|arg| fulltext_aggregate_arg_number(row, arg).ok())
            {
                *sum += value;
            }
            0
        }
        FullTextAggregateAccumulator::Avg { sum, count } => {
            if let Some(value) =
                argument.and_then(|arg| fulltext_aggregate_arg_number(row, arg).ok())
            {
                *sum += value;
                *count = count.saturating_add(1);
            }
            0
        }
        FullTextAggregateAccumulator::Min(current) => {
            if let Some(value) =
                argument.and_then(|arg| fulltext_aggregate_arg_number(row, arg).ok())
                && current.is_none_or(|current| value < current)
            {
                *current = Some(value);
            }
            0
        }
        FullTextAggregateAccumulator::Max(current) => {
            if let Some(value) =
                argument.and_then(|arg| fulltext_aggregate_arg_number(row, arg).ok())
                && current.is_none_or(|current| value > current)
            {
                *current = Some(value);
            }
            0
        }
        FullTextAggregateAccumulator::FirstValue(current) => {
            if current.is_some() {
                0
            } else {
                let value = argument
                    .and_then(|arg| eval_fulltext_aggregate_expression(arg, row).ok())
                    .unwrap_or(FullTextAggregateValue::Null);
                let bytes = estimate_fulltext_aggregate_value_bytes(&value);
                *current = Some(value);
                bytes
            }
        }
        FullTextAggregateAccumulator::ToList(values) => {
            let value = argument
                .and_then(|arg| eval_fulltext_aggregate_expression(arg, row).ok())
                .unwrap_or(FullTextAggregateValue::Null);
            let bytes = estimate_fulltext_aggregate_value_bytes(&value);
            values.push(value);
            bytes
        }
    })
}

fn finish_fulltext_aggregate_accumulator(
    accumulator: FullTextAggregateAccumulator,
) -> FullTextAggregateValue {
    match accumulator {
        FullTextAggregateAccumulator::Count(count) => FullTextAggregateValue::Number(count as f64),
        FullTextAggregateAccumulator::CountDistinct(seen) => {
            FullTextAggregateValue::Number(seen.len() as f64)
        }
        FullTextAggregateAccumulator::Sum(sum) => FullTextAggregateValue::Number(sum),
        FullTextAggregateAccumulator::Avg { sum, count } => {
            FullTextAggregateValue::Number(if count == 0 { 0.0 } else { sum / count as f64 })
        }
        FullTextAggregateAccumulator::Min(value) | FullTextAggregateAccumulator::Max(value) => {
            FullTextAggregateValue::Number(value.unwrap_or(0.0))
        }
        FullTextAggregateAccumulator::FirstValue(value) => {
            value.unwrap_or(FullTextAggregateValue::Null)
        }
        FullTextAggregateAccumulator::ToList(values) => FullTextAggregateValue::List(values),
    }
}

#[cfg(test)]
pub(super) fn fulltext_aggregate_group(
    rows: Vec<FullTextAggregateRow>,
    fields: &[String],
    reducers: &[FullTextAggregateReducer],
) -> Result<Vec<FullTextAggregateRow>, Error> {
    let mut state = FullTextAggregateGroupState::new(fields, reducers, usize::MAX)?;
    for row in &rows {
        state.push(row)?;
    }
    state.finish()
}

#[cfg(test)]
pub(super) fn fulltext_aggregate_reduce(
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

pub(super) fn fulltext_aggregate_reducer_default_name(
    reducer: &FullTextAggregateReducer,
) -> String {
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

pub(super) fn fulltext_aggregate_arg_value(row: &FullTextAggregateRow, arg: &str) -> String {
    eval_fulltext_aggregate_expression(arg, row)
        .map(|value| fulltext_aggregate_value_to_string(&value))
        .unwrap_or_default()
}

pub(super) fn fulltext_aggregate_arg_number(
    row: &FullTextAggregateRow,
    arg: &str,
) -> Result<f64, Error> {
    fulltext_aggregate_value_to_number(&eval_fulltext_aggregate_expression(arg, row)?)
}
