use super::*;
impl Db {
    pub fn fulltext_aggregate(
        &self,
        index: &str,
        query: &str,
        mut options: FullTextAggregateOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.AGGREGATE")?;
        options.search_options = self.fulltext_effective_search_options(options.search_options)?;
        let hits = self.fulltext_collect_live_hits(
            index,
            query,
            &options.search_options,
            FullTextCollectMode::All,
        )?;
        let max_results = self.fulltext_config_usize("MAXAGGREGATERESULTS", 10_000)?;
        if hits.hits.len() > max_results {
            return Err(Error::msg("ERR fulltext aggregate result limit exceeded"));
        }
        let aggregate_budget =
            self.fulltext_config_usize("MEMORY_BUDGET_SORT_BYTES", 16_777_216)?;
        let pipeline_started = Instant::now();
        let pipeline_deadline = pipeline_started
            .checked_add(Duration::from_millis(
                options.search_options.timeout_ms.unwrap_or(500),
            ))
            .unwrap_or_else(|| pipeline_started + Duration::from_secs(100 * 365 * 24 * 60 * 60));
        let fail_on_timeout = self
            .fulltext_config_string("ON_TIMEOUT", "RETURN")?
            .eq_ignore_ascii_case("FAIL");

        let requires_materialization = !options.sort_by.is_empty()
            || options
                .steps
                .iter()
                .any(|step| matches!(step, FullTextAggregateStep::GroupBy { .. }));
        if !requires_materialization {
            let mut total = 0usize;
            let mut selected = Vec::new();
            let selection_end = options.offset.saturating_add(options.limit);
            let mut selected_bytes = 0usize;
            for hit in hits.hits {
                if fulltext_search_timeout_reached(pipeline_deadline, fail_on_timeout)? {
                    break;
                }
                let mut row = fulltext_aggregate_row_from_hit(hit, options.load.as_deref())?;
                if !fulltext_apply_streaming_aggregate_steps(&mut row, &options.steps)? {
                    continue;
                }
                if total >= options.offset && total < selection_end {
                    selected_bytes =
                        selected_bytes.saturating_add(estimate_fulltext_aggregate_row_bytes(&row));
                    if selected_bytes > aggregate_budget {
                        return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
                    }
                    selected.push(row);
                }
                total = total.saturating_add(1);
            }
            return self.fulltext_finish_aggregate(index, total, selected, &options);
        }

        let first_group = options
            .steps
            .iter()
            .position(|step| matches!(step, FullTextAggregateStep::GroupBy { .. }));
        let mut next_step;
        let mut rows = if let Some(group_index) = first_group {
            let FullTextAggregateStep::GroupBy { fields, reducers } = &options.steps[group_index]
            else {
                unreachable!();
            };
            let mut state = FullTextAggregateGroupState::new(fields, reducers, aggregate_budget)?;
            for hit in hits.hits {
                if fulltext_search_timeout_reached(pipeline_deadline, fail_on_timeout)? {
                    break;
                }
                let mut row = fulltext_aggregate_row_from_hit(hit, options.load.as_deref())?;
                if fulltext_apply_streaming_aggregate_steps(
                    &mut row,
                    &options.steps[..group_index],
                )? {
                    state.push(&row)?;
                }
            }
            next_step = group_index + 1;
            state.finish()?
        } else {
            next_step = options.steps.len();
            let mut rows = Vec::new();
            let mut used = 0usize;
            for hit in hits.hits {
                if fulltext_search_timeout_reached(pipeline_deadline, fail_on_timeout)? {
                    break;
                }
                let mut row = fulltext_aggregate_row_from_hit(hit, options.load.as_deref())?;
                if fulltext_apply_streaming_aggregate_steps(&mut row, &options.steps)? {
                    used = used.saturating_add(estimate_fulltext_aggregate_row_bytes(&row));
                    if used > aggregate_budget {
                        return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
                    }
                    rows.push(row);
                }
            }
            rows
        };
        validate_fulltext_aggregate_memory(&rows, aggregate_budget)?;

        while next_step < options.steps.len() {
            let next_group = options.steps[next_step..]
                .iter()
                .position(|step| matches!(step, FullTextAggregateStep::GroupBy { .. }))
                .map(|offset| next_step + offset);
            let segment_end = next_group.unwrap_or(options.steps.len());
            rows = fulltext_stream_aggregate_rows(
                rows,
                &options.steps[next_step..segment_end],
                aggregate_budget,
                pipeline_deadline,
                fail_on_timeout,
            )?;
            let Some(group_index) = next_group else {
                break;
            };
            let FullTextAggregateStep::GroupBy { fields, reducers } = &options.steps[group_index]
            else {
                unreachable!();
            };
            let mut state = FullTextAggregateGroupState::new(fields, reducers, aggregate_budget)?;
            for row in rows {
                if fulltext_search_timeout_reached(pipeline_deadline, fail_on_timeout)? {
                    break;
                }
                state.push(&row)?;
            }
            rows = state.finish()?;
            validate_fulltext_aggregate_memory(&rows, aggregate_budget)?;
            next_step = group_index + 1;
        }

        if !options.sort_by.is_empty() {
            rows.sort_by(|left, right| {
                compare_fulltext_aggregate_rows(left, right, &options.sort_by)
            });
        }

        let total = rows.len();
        let selected = rows
            .into_iter()
            .skip(options.offset)
            .take(options.limit)
            .collect::<Vec<_>>();

        self.fulltext_finish_aggregate(index, total, selected, &options)
    }

    pub(super) fn fulltext_finish_aggregate(
        &self,
        index: &str,
        total: usize,
        selected: Vec<FullTextAggregateRow>,
        options: &FullTextAggregateOptions,
    ) -> Result<Frame, Error> {
        if let Some(count) = options.cursor_count {
            let count = count.max(1);
            let mut first = selected;
            let rest = if first.len() > count {
                first.split_off(count)
            } else {
                Vec::new()
            };
            let cursor = if rest.is_empty() {
                0
            } else {
                register_fulltext_aggregate_cursor(
                    self.db_index,
                    index,
                    rest,
                    options.cursor_max_idle_ms.unwrap_or(300_000),
                    self.fulltext_config_usize("MEMORY_BUDGET_AGGREGATE_CURSOR_BYTES", 16_777_216)?,
                )?
            };
            return Ok(Frame::Array(vec![
                fulltext_aggregate_frame(total, first),
                Frame::Integer(cursor as i64),
            ]));
        }

        Ok(fulltext_aggregate_frame(total, selected))
    }

    pub async fn fulltext_aggregate_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextAggregateOptions,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        let query = query.to_string();
        self.run_fulltext_search_task(move |db| db.fulltext_aggregate(&index, &query, options))
            .await
    }

    pub fn fulltext_cursor_read(
        &self,
        index: &str,
        cursor_id: u64,
        count: usize,
    ) -> Result<Frame, Error> {
        let count = count.max(1);
        let (rows, remaining) =
            read_fulltext_aggregate_cursor(self.db_index, index, cursor_id, count)?;
        Ok(Frame::Array(vec![
            fulltext_aggregate_frame(rows.len() + remaining, rows),
            Frame::Integer(if remaining == 0 { 0 } else { cursor_id as i64 }),
        ]))
    }

    pub async fn fulltext_cursor_read_async(
        &self,
        index: &str,
        cursor_id: u64,
        count: usize,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_cursor_read(&index, cursor_id, count))
            .await
    }

    pub fn fulltext_cursor_del(&self, index: &str, cursor_id: u64) -> Result<Frame, Error> {
        delete_fulltext_aggregate_cursor(self.db_index, index, cursor_id)?;
        Ok(Frame::Ok)
    }

    pub async fn fulltext_cursor_del_async(
        &self,
        index: &str,
        cursor_id: u64,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_cursor_del(&index, cursor_id))
            .await
    }
}

pub(super) fn validate_fulltext_aggregate_memory(
    rows: &[FullTextAggregateRow],
    budget: usize,
) -> Result<(), Error> {
    let used = rows.iter().fold(0usize, |used, row| {
        used.saturating_add(estimate_fulltext_aggregate_row_bytes(row))
    });
    if used > budget {
        return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
    }
    Ok(())
}

pub(super) fn fulltext_apply_streaming_aggregate_steps(
    row: &mut FullTextAggregateRow,
    steps: &[FullTextAggregateStep],
) -> Result<bool, Error> {
    for step in steps {
        match step {
            FullTextAggregateStep::Apply { expression, alias } => {
                let value = eval_fulltext_aggregate_expression(expression, row)?;
                row.values.insert(alias.clone(), value.clone());
                fulltext_aggregate_set_output(row, alias.clone(), value);
            }
            FullTextAggregateStep::Filter { expression } => {
                if !eval_fulltext_aggregate_filter(expression, row)? {
                    return Ok(false);
                }
            }
            FullTextAggregateStep::GroupBy { .. } => {
                return Err(Error::msg(
                    "ERR GROUPBY requires materialized aggregate execution",
                ));
            }
        }
    }
    Ok(true)
}

pub(super) fn fulltext_stream_aggregate_rows(
    rows: Vec<FullTextAggregateRow>,
    steps: &[FullTextAggregateStep],
    memory_budget: usize,
    deadline: Instant,
    fail_on_timeout: bool,
) -> Result<Vec<FullTextAggregateRow>, Error> {
    let mut out = Vec::new();
    let mut used = 0usize;
    for mut row in rows {
        if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
            break;
        }
        if fulltext_apply_streaming_aggregate_steps(&mut row, steps)? {
            used = used.saturating_add(estimate_fulltext_aggregate_row_bytes(&row));
            if used > memory_budget {
                return Err(Error::msg("ERR fulltext aggregate memory limit exceeded"));
            }
            out.push(row);
        }
    }
    Ok(out)
}
