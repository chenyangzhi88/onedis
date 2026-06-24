impl VAdd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vadd' command",
            ));
        }
        let key = arg(&frame, 1, "ERR invalid vector key")?;
        let mut idx = 2;
        let mut reduce = None;
        if upper_arg(&frame, idx)? == "REDUCE" {
            reduce = Some(parse_usize_arg(
                &frame,
                idx + 1,
                "ERR invalid vector REDUCE",
            )?);
            idx += 2;
        }
        let mut vector = parse_redis_vector_arg(&frame, &mut idx)?;
        if let Some(dim) = reduce {
            if dim == 0 || dim > vector.len() {
                return Err(Error::msg("ERR invalid vector REDUCE"));
            }
            vector.truncate(dim);
        }
        let element = arg(&frame, idx, "ERR invalid vector element")?;
        idx += 1;
        let mut attrs_json = None;
        let mut m = None;
        let mut ef = None;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "CAS" | "NOQUANT" | "Q8" | "BIN" => idx += 1,
                "SETATTR" => {
                    let attrs = arg(&frame, idx + 1, "ERR invalid vector attrs")?;
                    attrs_json = (!attrs.is_empty()).then_some(attrs);
                    idx += 2;
                }
                "EF" => {
                    ef = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector EF")?);
                    idx += 2;
                }
                "M" => {
                    m = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector M")?);
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key,
            element,
            vector,
            attrs_json,
            m,
            ef,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            if db.vector_add_autocreate(
                &self.key,
                &self.element,
                self.vector,
                self.attrs_json,
                self.m,
                self.ef,
            )? {
                1
            } else {
                0
            },
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            if db
                .vector_add_autocreate_async(
                    &self.key,
                    &self.element,
                    self.vector,
                    self.attrs_json,
                    self.m,
                    self.ef,
                )
                .await?
            {
                1
            } else {
                0
            },
        ))
    }
}

impl VSim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vsim' command",
            ));
        }
        let key = arg(&frame, 1, "ERR invalid vector key")?;
        let mut idx = 2;
        let query = match upper_arg(&frame, idx)?.as_str() {
            "ELE" => {
                let element = arg(&frame, idx + 1, "ERR invalid vector element")?;
                idx += 2;
                VSimQuery::Element(element)
            }
            "FP32" | "VALUES" => VSimQuery::Vector(parse_redis_vector_arg(&frame, &mut idx)?),
            _ => return Err(Error::msg("ERR syntax error")),
        };
        let mut with_scores = false;
        let mut with_attrs = false;
        let mut count = 10usize;
        let mut ef = None;
        let mut filter = None;
        let mut epsilon = None;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "WITHSCORES" => {
                    with_scores = true;
                    idx += 1;
                }
                "WITHATTRIBS" => {
                    with_attrs = true;
                    idx += 1;
                }
                "COUNT" => {
                    count = parse_usize_arg(&frame, idx + 1, "ERR invalid vector COUNT")?;
                    idx += 2;
                }
                "EF" => {
                    ef = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector EF")?);
                    idx += 2;
                }
                "FILTER" => {
                    filter = Some(arg(&frame, idx + 1, "ERR invalid vector filter")?);
                    idx += 2;
                }
                "EPSILON" => {
                    epsilon = Some(parse_f32_arg(
                        &frame,
                        idx + 1,
                        "ERR invalid vector EPSILON",
                    )?);
                    idx += 2;
                }
                "FILTER-EF" => {
                    let _ = parse_usize_arg(&frame, idx + 1, "ERR invalid vector FILTER-EF")?;
                    idx += 2;
                }
                "TRUTH" | "NOTHREAD" => idx += 1,
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key,
            query,
            with_scores,
            with_attrs,
            count,
            ef,
            filter,
            epsilon,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let vector = match &self.query {
            VSimQuery::Vector(vector) => vector.clone(),
            VSimQuery::Element(element) => {
                db.vector_element(&self.key, element)?
                    .ok_or_else(|| Error::msg("ERR vector element does not exist"))?
                    .vector
            }
        };
        let options = VectorSearchOptions {
            k: self.count,
            filter: self.filter.clone(),
            with_scores: false,
            with_attrs: Vec::new(),
            ef: self.ef,
            offset: 0,
            limit: Some(self.count),
        };
        let mut results = db.vector_search(&self.key, &vector, options)?;
        if let Some(epsilon) = self.epsilon {
            results.retain(|result| vector_similarity_score(result.score) >= 1.0 - epsilon);
        }
        redis_vsim_results_frame(db, &self.key, results, self.with_scores, self.with_attrs)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let vector = match &self.query {
            VSimQuery::Vector(vector) => vector.clone(),
            VSimQuery::Element(element) => {
                db.vector_element_async(&self.key, element)
                    .await?
                    .ok_or_else(|| Error::msg("ERR vector element does not exist"))?
                    .vector
            }
        };
        let options = VectorSearchOptions {
            k: self.count,
            filter: self.filter.clone(),
            with_scores: false,
            with_attrs: Vec::new(),
            ef: self.ef,
            offset: 0,
            limit: Some(self.count),
        };
        let mut results = db.vector_search_async(&self.key, &vector, options).await?;
        if let Some(epsilon) = self.epsilon {
            results.retain(|result| vector_similarity_score(result.score) >= 1.0 - epsilon);
        }
        redis_vsim_results_frame_async(db, &self.key, results, self.with_scores, self.with_attrs)
            .await
    }
}
