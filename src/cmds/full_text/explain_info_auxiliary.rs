impl FtExplain {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for fulltext explain command",
            ));
        }
        let cli = upper_arg(&frame, 0)?.as_str() == "FT.EXPLAINCLI";
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let query = arg(&frame, 2, "ERR invalid fulltext query")?;
        let mut options = default_fulltext_search_options();
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
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
            cli,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_explain(&self.index, &self.query, self.options, self.cli)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_explain_async(&self.index, &self.query, self.options, self.cli)
            .await
    }
}

impl FtTagVals {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.tagvals' command",
            ));
        }
        Ok(Self {
            index: arg(&frame, 1, "ERR invalid fulltext index")?,
            field: arg(&frame, 2, "ERR invalid TAG field")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_tagvals(&self.index, &self.field)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_tagvals_async(&self.index, &self.field).await
    }
}

impl FtInfo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.info' command",
            ));
        }
        Ok(Self {
            index: arg(&frame, 1, "ERR invalid fulltext index")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_info(&self.index)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_info_async(&self.index).await
    }
}

impl FtDict {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for fulltext dictionary command",
            ));
        }
        match upper_arg(&frame, 0)?.as_str() {
            "FT.DICTADD" if frame.arg_len() >= 3 => Ok(Self::Add {
                dict: arg(&frame, 1, "ERR invalid dictionary")?,
                terms: collect_args(&frame, 2, "ERR invalid dictionary term")?,
            }),
            "FT.DICTDEL" if frame.arg_len() >= 3 => Ok(Self::Del {
                dict: arg(&frame, 1, "ERR invalid dictionary")?,
                terms: collect_args(&frame, 2, "ERR invalid dictionary term")?,
            }),
            "FT.DICTDUMP" if frame.arg_len() == 2 => Ok(Self::Dump {
                dict: arg(&frame, 1, "ERR invalid dictionary")?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Add { dict, terms } => db.fulltext_dict_add(&dict, terms),
            Self::Del { dict, terms } => db.fulltext_dict_del(&dict, terms),
            Self::Dump { dict } => db.fulltext_dict_dump(&dict),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Add { dict, terms } => db.fulltext_dict_add_async(&dict, terms).await,
            Self::Del { dict, terms } => db.fulltext_dict_del_async(&dict, terms).await,
            Self::Dump { dict } => db.fulltext_dict_dump_async(&dict).await,
        }
    }
}

impl FtSpellCheck {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.spellcheck' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let query = arg(&frame, 2, "ERR invalid spellcheck query")?;
        let mut distance = 1usize;
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "DISTANCE" => {
                    distance = parse_usize_arg(&frame, idx + 1, "ERR invalid DISTANCE")?;
                    idx += 2;
                }
                "TERMS" => {
                    match upper_arg(&frame, idx + 1)?.as_str() {
                        "INCLUDE" => include.push(arg(&frame, idx + 2, "ERR invalid dictionary")?),
                        "EXCLUDE" => exclude.push(arg(&frame, idx + 2, "ERR invalid dictionary")?),
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                    idx += 3;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            index,
            query,
            distance,
            include,
            exclude,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_spellcheck(
            &self.index,
            &self.query,
            self.distance,
            self.include,
            self.exclude,
        )
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_spellcheck_async(
            &self.index,
            &self.query,
            self.distance,
            self.include,
            self.exclude,
        )
        .await
    }
}

impl FtSug {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        match upper_arg(&frame, 0)?.as_str() {
            "FT.SUGADD" if frame.arg_len() >= 4 => {
                let key = arg(&frame, 1, "ERR invalid suggestion key")?;
                let string = arg(&frame, 2, "ERR invalid suggestion string")?;
                let score = parse_f64_arg(&frame, 3, "ERR invalid suggestion score")?;
                let mut incr = false;
                let mut payload = None;
                let mut idx = 4;
                while idx < frame.arg_len() {
                    match upper_arg(&frame, idx)?.as_str() {
                        "INCR" => {
                            incr = true;
                            idx += 1;
                        }
                        "PAYLOAD" => {
                            payload = Some(arg(&frame, idx + 1, "ERR invalid suggestion payload")?);
                            idx += 2;
                        }
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                }
                Ok(Self::Add {
                    key,
                    string,
                    score,
                    incr,
                    payload,
                })
            }
            "FT.SUGGET" if frame.arg_len() >= 3 => {
                let key = arg(&frame, 1, "ERR invalid suggestion key")?;
                let prefix = arg(&frame, 2, "ERR invalid suggestion prefix")?;
                let mut fuzzy = false;
                let mut with_scores = false;
                let mut with_payloads = false;
                let mut max = 5usize;
                let mut idx = 3;
                while idx < frame.arg_len() {
                    match upper_arg(&frame, idx)?.as_str() {
                        "FUZZY" => {
                            fuzzy = true;
                            idx += 1;
                        }
                        "WITHSCORES" => {
                            with_scores = true;
                            idx += 1;
                        }
                        "WITHPAYLOADS" => {
                            with_payloads = true;
                            idx += 1;
                        }
                        "MAX" => {
                            max = parse_usize_arg(&frame, idx + 1, "ERR invalid MAX")?;
                            idx += 2;
                        }
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                }
                Ok(Self::Get {
                    key,
                    prefix,
                    fuzzy,
                    with_scores,
                    with_payloads,
                    max,
                })
            }
            "FT.SUGDEL" if frame.arg_len() == 3 => Ok(Self::Del {
                key: arg(&frame, 1, "ERR invalid suggestion key")?,
                string: arg(&frame, 2, "ERR invalid suggestion string")?,
            }),
            "FT.SUGLEN" if frame.arg_len() == 2 => Ok(Self::Len {
                key: arg(&frame, 1, "ERR invalid suggestion key")?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Add {
                key,
                string,
                score,
                incr,
                payload,
            } => db.fulltext_sugadd(&key, &string, score, incr, payload),
            Self::Get {
                key,
                prefix,
                fuzzy,
                with_scores,
                with_payloads,
                max,
            } => db.fulltext_sugget(&key, &prefix, fuzzy, with_scores, with_payloads, max),
            Self::Del { key, string } => db.fulltext_sugdel(&key, &string),
            Self::Len { key } => db.fulltext_suglen(&key),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Add {
                key,
                string,
                score,
                incr,
                payload,
            } => {
                db.fulltext_sugadd_async(&key, &string, score, incr, payload)
                    .await
            }
            Self::Get {
                key,
                prefix,
                fuzzy,
                with_scores,
                with_payloads,
                max,
            } => {
                db.fulltext_sugget_async(&key, &prefix, fuzzy, with_scores, with_payloads, max)
                    .await
            }
            Self::Del { key, string } => db.fulltext_sugdel_async(&key, &string).await,
            Self::Len { key } => db.fulltext_suglen_async(&key).await,
        }
    }
}

impl FtSyn {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        match upper_arg(&frame, 0)?.as_str() {
            "FT.SYNUPDATE" if frame.arg_len() >= 4 => {
                let index = arg(&frame, 1, "ERR invalid fulltext index")?;
                let group = arg(&frame, 2, "ERR invalid synonym group")?;
                let mut idx = 3;
                if upper_arg(&frame, idx).unwrap_or_default().as_str() == "SKIPINITIALSCAN" {
                    idx += 1;
                }
                Ok(Self::Update {
                    index,
                    group,
                    terms: collect_args(&frame, idx, "ERR invalid synonym term")?,
                })
            }
            "FT.SYNDUMP" if frame.arg_len() == 2 => Ok(Self::Dump {
                index: arg(&frame, 1, "ERR invalid fulltext index")?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Update {
                index,
                group,
                terms,
            } => db.fulltext_synupdate(&index, &group, terms),
            Self::Dump { index } => db.fulltext_syndump(&index),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Update {
                index,
                group,
                terms,
            } => db.fulltext_synupdate_async(&index, &group, terms).await,
            Self::Dump { index } => db.fulltext_syndump_async(&index).await,
        }
    }
}

impl FtUnsupported {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let command_name = arg(&frame, 0, "ERR empty command")?.to_ascii_uppercase();
        Ok(Self { command_name })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(Frame::Error(format!(
            "ERR unsupported full-text command {}",
            self.command_name
        )))
    }

    pub async fn apply_async(self) -> Result<Frame, Error> {
        self.apply()
    }
}

