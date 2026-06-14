impl FtSearch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.search' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let query = arg(&frame, 2, "ERR invalid fulltext query")?;
        let mut options = default_fulltext_search_options();
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "LIMIT" => {
                    options.offset = parse_usize_arg(&frame, idx + 1, "ERR invalid LIMIT offset")?;
                    options.limit = parse_usize_arg(&frame, idx + 2, "ERR invalid LIMIT count")?;
                    idx += 3;
                }
                "NOCONTENT" => {
                    options.no_content = true;
                    idx += 1;
                }
                "WITHSCORES" => {
                    options.with_scores = true;
                    idx += 1;
                }
                "WITHPAYLOADS" => {
                    options.with_payloads = true;
                    idx += 1;
                }
                "WITHSORTKEYS" => {
                    options.with_sort_keys = true;
                    idx += 1;
                }
                "RETURN" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid RETURN count")?;
                    idx += 2;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let mut fields = Vec::with_capacity(count);
                    for _ in 0..count {
                        let identifier = arg(&frame, idx, "ERR invalid RETURN field")?;
                        idx += 1;
                        let alias = if idx + 1 < frame.arg_len()
                            && upper_arg(&frame, idx)?.as_str() == "AS"
                        {
                            let alias = arg(&frame, idx + 1, "ERR invalid RETURN alias")?;
                            idx += 2;
                            Some(alias)
                        } else {
                            None
                        };
                        fields.push(FullTextReturnField { identifier, alias });
                    }
                    options.return_fields = Some(fields);
                }
                "FILTER" => {
                    options.filters.push(FullTextSearchNumericFilter {
                        field: arg(&frame, idx + 1, "ERR invalid FILTER field")?,
                        min: parse_search_bound_arg(&frame, idx + 2, "ERR invalid FILTER min")?,
                        max: parse_search_bound_arg(&frame, idx + 3, "ERR invalid FILTER max")?,
                    });
                    idx += 4;
                }
                "GEOFILTER" => {
                    options.geo_filters.push(FullTextSearchGeoFilter {
                        field: arg(&frame, idx + 1, "ERR invalid GEOFILTER field")?,
                        lon: parse_f64_arg(&frame, idx + 2, "ERR invalid GEOFILTER lon")?,
                        lat: parse_f64_arg(&frame, idx + 3, "ERR invalid GEOFILTER lat")?,
                        radius: parse_f64_arg(&frame, idx + 4, "ERR invalid GEOFILTER radius")?,
                        unit: arg(&frame, idx + 5, "ERR invalid GEOFILTER unit")?,
                    });
                    idx += 6;
                }
                "INKEYS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid INKEYS count")?;
                    idx += 2;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let mut keys = HashSet::with_capacity(count);
                    for _ in 0..count {
                        keys.insert(arg(&frame, idx, "ERR invalid INKEYS key")?);
                        idx += 1;
                    }
                    options.in_keys = Some(keys);
                }
                "INFIELDS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid INFIELDS count")?;
                    idx += 2;
                    if idx + count > frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let mut fields = Vec::with_capacity(count);
                    for _ in 0..count {
                        fields.push(arg(&frame, idx, "ERR invalid INFIELDS field")?);
                        idx += 1;
                    }
                    options.in_fields = Some(fields);
                }
                "SORTBY" => {
                    let field = arg(&frame, idx + 1, "ERR invalid SORTBY field")?;
                    idx += 2;
                    let asc = if idx < frame.arg_len() {
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
                    options.sort_by = Some(FullTextSortBy { field, asc });
                }
                "SUMMARIZE" => {
                    options.summarize = true;
                    idx = skip_search_display_options(&frame, idx + 1);
                }
                "HIGHLIGHT" => {
                    options.highlight = true;
                    idx = skip_search_display_options(&frame, idx + 1);
                }
                "SLOP" => {
                    options.slop = Some(parse_u64_arg(&frame, idx + 1, "ERR invalid SLOP")? as u32);
                    idx += 2;
                }
                "TIMEOUT" => {
                    options.timeout_ms =
                        Some(parse_u64_arg(&frame, idx + 1, "ERR invalid TIMEOUT")?);
                    idx += 2;
                }
                "INORDER" => {
                    options.inorder = true;
                    idx += 1;
                }
                "LANGUAGE" => {
                    options.language = Some(arg(&frame, idx + 1, "ERR invalid LANGUAGE")?);
                    idx += 2;
                }
                "EXPANDER" => {
                    let expander = upper_arg(&frame, idx + 1)?;
                    if expander != "DEFAULT" {
                        return Err(Error::msg("ERR unsupported fulltext expander"));
                    }
                    idx += 2;
                }
                "SCORER" => {
                    let scorer = upper_arg(&frame, idx + 1)?;
                    if !matches!(scorer.as_str(), "BM25" | "BM25STD" | "DISMAX" | "DOCSCORE") {
                        return Err(Error::msg("ERR unsupported fulltext scorer"));
                    }
                    idx += 2;
                }
                "EXPLAINSCORE" => {
                    options.explain_score = true;
                    options.with_scores = true;
                    idx += 1;
                }
                "PAYLOAD" => {
                    options.payload = Some(arg(&frame, idx + 1, "ERR invalid PAYLOAD")?);
                    idx += 2;
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
                        options.params.insert(name, value);
                        idx += 2;
                    }
                }
                "DIALECT" => {
                    let dialect = parse_u64_arg(&frame, idx + 1, "ERR invalid DIALECT")?;
                    if !(1..=4).contains(&dialect) {
                        return Err(Error::msg("ERR invalid DIALECT"));
                    }
                    options.dialect = dialect as u8;
                    options.dialect_explicit = true;
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
        db.fulltext_search(&self.index, &self.query, self.options)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_search_async(&self.index, &self.query, self.options)
            .await
    }
}

impl FtHybrid {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.hybrid' command",
            ));
        }
        Ok(Self {
            search: FtSearch::parse_from_frame(frame)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.search.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.search.apply_async(db).await
    }
}

