impl FtAggregate {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.aggregate' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let query = arg(&frame, 2, "ERR invalid fulltext query")?;
        let mut options = FullTextAggregateOptions {
            search_options: default_fulltext_search_options(),
            load: None,
            steps: Vec::new(),
            sort_by: Vec::new(),
            offset: 0,
            limit: 10,
            cursor_count: None,
            cursor_max_idle_ms: None,
        };
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "LOAD" => {
                    idx += 1;
                    if idx >= frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    if arg(&frame, idx, "ERR invalid LOAD")? == "*" {
                        options.load = Some(vec![FullTextAggregateLoadField {
                            identifier: "*".to_string(),
                            alias: None,
                        }]);
                        idx += 1;
                        continue;
                    }
                    let count = parse_usize_arg(&frame, idx, "ERR invalid LOAD count")?;
                    idx += 1;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let end = idx + count;
                    let mut fields = Vec::new();
                    while idx < end {
                        let identifier = arg(&frame, idx, "ERR invalid LOAD field")?;
                        idx += 1;
                        let alias = if idx + 1 < end
                            && idx < frame.arg_len()
                            && upper_arg(&frame, idx)?.as_str() == "AS"
                        {
                            let alias = arg(&frame, idx + 1, "ERR invalid LOAD alias")?;
                            idx += 2;
                            Some(alias)
                        } else {
                            None
                        };
                        fields.push(FullTextAggregateLoadField { identifier, alias });
                    }
                    options.load = Some(fields);
                }
                "APPLY" => {
                    let expression = arg(&frame, idx + 1, "ERR invalid APPLY expression")?;
                    if upper_arg(&frame, idx + 2)?.as_str() != "AS" {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let alias = arg(&frame, idx + 3, "ERR invalid APPLY alias")?;
                    options
                        .steps
                        .push(FullTextAggregateStep::Apply { expression, alias });
                    idx += 4;
                }
                "FILTER" => {
                    let expression = arg(&frame, idx + 1, "ERR invalid FILTER expression")?;
                    options
                        .steps
                        .push(FullTextAggregateStep::Filter { expression });
                    idx += 2;
                }
                "GROUPBY" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid GROUPBY count")?;
                    idx += 2;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let mut fields = Vec::with_capacity(count);
                    for _ in 0..count {
                        fields.push(arg(&frame, idx, "ERR invalid GROUPBY field")?);
                        idx += 1;
                    }
                    let mut reducers = Vec::new();
                    while idx < frame.arg_len() && upper_arg(&frame, idx)?.as_str() == "REDUCE" {
                        reducers.push(parse_fulltext_aggregate_reducer(&frame, &mut idx)?);
                    }
                    options
                        .steps
                        .push(FullTextAggregateStep::GroupBy { fields, reducers });
                }
                "SORTBY" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid SORTBY count")?;
                    idx += 2;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let end = idx + count;
                    while idx < end {
                        let field = arg(&frame, idx, "ERR invalid SORTBY field")?;
                        idx += 1;
                        let asc = if idx < end {
                            match upper_arg(&frame, idx)?.as_str() {
                                "ASC" => {
                                    idx += 1;
                                    true
                                }
                                "DESC" => {
                                    idx += 1;
                                    false
                                }
                                _ => true,
                            }
                        } else {
                            true
                        };
                        options.sort_by.push(FullTextAggregateSortBy { field, asc });
                    }
                }
                "LIMIT" => {
                    options.offset = parse_usize_arg(&frame, idx + 1, "ERR invalid LIMIT offset")?;
                    options.limit = parse_usize_arg(&frame, idx + 2, "ERR invalid LIMIT count")?;
                    idx += 3;
                }
                "WITHCURSOR" => {
                    options.cursor_count = Some(1000);
                    options.cursor_max_idle_ms = Some(300_000);
                    idx += 1;
                    while idx < frame.arg_len() {
                        match upper_arg(&frame, idx)?.as_str() {
                            "COUNT" => {
                                options.cursor_count = Some(parse_usize_arg(
                                    &frame,
                                    idx + 1,
                                    "ERR invalid cursor COUNT",
                                )?);
                                idx += 2;
                            }
                            "MAXIDLE" => {
                                let max_idle_ms = parse_u64_arg(
                                    &frame,
                                    idx + 1,
                                    "ERR invalid cursor MAXIDLE",
                                )?;
                                if max_idle_ms == 0 {
                                    return Err(Error::msg("ERR invalid cursor MAXIDLE"));
                                }
                                options.cursor_max_idle_ms = Some(max_idle_ms);
                                idx += 2;
                            }
                            _ => break,
                        }
                    }
                }
                "PARAMS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid PARAMS count")?;
                    idx += 2;
                    if count % 2 != 0 || idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    for _ in 0..(count / 2) {
                        let name = arg(&frame, idx, "ERR invalid PARAMS name")?;
                        let value = frame
                            .get_arg_bytes(idx + 1)
                            .ok_or_else(|| Error::msg("ERR invalid PARAMS value"))?;
                        options.search_options.params.insert(name, value);
                        idx += 2;
                    }
                }
                "DIALECT" => {
                    let dialect = parse_u64_arg(&frame, idx + 1, "ERR invalid DIALECT")?;
                    if !(1..=4).contains(&dialect) {
                        return Err(Error::msg("ERR invalid DIALECT"));
                    }
                    options.search_options.dialect = dialect as u8;
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            index,
            query,
            options,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_aggregate(&self.index, &self.query, self.options)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_aggregate_async(&self.index, &self.query, self.options)
            .await
    }
}

impl FtCursor {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.cursor' command",
            ));
        }
        match upper_arg(&frame, 1)?.as_str() {
            "READ" => {
                let index = arg(&frame, 2, "ERR invalid fulltext index")?;
                let cursor_id = parse_u64_arg(&frame, 3, "ERR invalid cursor id")?;
                let mut count = 1000;
                let mut idx = 4;
                while idx < frame.arg_len() {
                    match upper_arg(&frame, idx)?.as_str() {
                        "COUNT" => {
                            count = parse_usize_arg(&frame, idx + 1, "ERR invalid cursor COUNT")?;
                            idx += 2;
                        }
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                }
                Ok(Self::Read {
                    index,
                    cursor_id,
                    count,
                })
            }
            "DEL" if frame.arg_len() == 4 => Ok(Self::Del {
                index: arg(&frame, 2, "ERR invalid fulltext index")?,
                cursor_id: parse_u64_arg(&frame, 3, "ERR invalid cursor id")?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Read {
                index,
                cursor_id,
                count,
            } => db.fulltext_cursor_read(&index, cursor_id, count),
            Self::Del { index, cursor_id } => db.fulltext_cursor_del(&index, cursor_id),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Read {
                index,
                cursor_id,
                count,
            } => {
                db.fulltext_cursor_read_async(&index, cursor_id, count)
                    .await
            }
            Self::Del { index, cursor_id } => db.fulltext_cursor_del_async(&index, cursor_id).await,
        }
    }
}

impl FtProfile {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.profile' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let kind = upper_arg(&frame, 2)?;
        let mut idx = 3;
        if upper_arg(&frame, idx)?.as_str() == "LIMITED" {
            idx += 1;
        }
        if upper_arg(&frame, idx)?.as_str() != "QUERY" {
            return Err(Error::msg("ERR syntax error"));
        }
        idx += 1;
        match kind.as_str() {
            "SEARCH" => {
                let search_frame = fulltext_profile_inner_frame(&frame, "FT.SEARCH", &index, idx)?;
                Ok(Self {
                    target: FtProfileTarget::Search(FtSearch::parse_from_frame(search_frame)?),
                })
            }
            "AGGREGATE" => {
                let aggregate_frame =
                    fulltext_profile_inner_frame(&frame, "FT.AGGREGATE", &index, idx)?;
                Ok(Self {
                    target: FtProfileTarget::Aggregate(FtAggregate::parse_from_frame(
                        aggregate_frame,
                    )?),
                })
            }
            _ => Err(Error::msg("ERR unsupported fulltext profile target")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self.target {
            FtProfileTarget::Search(search) => {
                let start = Instant::now();
                let result = search.apply(db)?;
                Ok(fulltext_profile_frame(
                    result,
                    start.elapsed().as_secs_f64() * 1000.0,
                    "Search",
                ))
            }
            FtProfileTarget::Aggregate(aggregate) => {
                let start = Instant::now();
                let result = aggregate.apply(db)?;
                Ok(fulltext_profile_frame(
                    result,
                    start.elapsed().as_secs_f64() * 1000.0,
                    "Aggregate",
                ))
            }
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self.target {
            FtProfileTarget::Search(search) => {
                let start = Instant::now();
                let result = search.apply_async(db).await?;
                Ok(fulltext_profile_frame(
                    result,
                    start.elapsed().as_secs_f64() * 1000.0,
                    "Search",
                ))
            }
            FtProfileTarget::Aggregate(aggregate) => {
                let start = Instant::now();
                let result = aggregate.apply_async(db).await?;
                Ok(fulltext_profile_frame(
                    result,
                    start.elapsed().as_secs_f64() * 1000.0,
                    "Aggregate",
                ))
            }
        }
    }
}
