impl Db {
    pub fn fulltext_aggregate(
        &self,
        index: &str,
        query: &str,
        mut options: FullTextAggregateOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.AGGREGATE")?;
        options.search_options = self.fulltext_effective_search_options(options.search_options)?;
        let hits = self.fulltext_collect_live_hits(index, query, &options.search_options)?;
        let mut rows = hits
            .into_iter()
            .map(|hit| fulltext_aggregate_row_from_hit(hit, options.load.as_deref()))
            .collect::<Result<Vec<_>, _>>()?;

        for step in &options.steps {
            match step {
                FullTextAggregateStep::Apply { expression, alias } => {
                    for row in &mut rows {
                        let value = eval_fulltext_aggregate_expression(expression, row)?;
                        row.values.insert(alias.clone(), value.clone());
                        fulltext_aggregate_set_output(row, alias.clone(), value);
                    }
                }
                FullTextAggregateStep::Filter { expression } => {
                    let mut filtered = Vec::new();
                    for row in rows {
                        if eval_fulltext_aggregate_filter(expression, &row)? {
                            filtered.push(row);
                        }
                    }
                    rows = filtered;
                }
                FullTextAggregateStep::GroupBy { fields, reducers } => {
                    rows = fulltext_aggregate_group(rows, fields, reducers)?;
                }
            }
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
                register_fulltext_aggregate_cursor(self.db_index, index, rest)
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
        self.fulltext_aggregate(index, query, options)
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
        self.fulltext_cursor_read(index, cursor_id, count)
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
        self.fulltext_cursor_del(index, cursor_id)
    }


}
