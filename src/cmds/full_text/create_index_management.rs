impl FtCreate {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.create' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let mut idx = 2;
        let mut source_type = FullTextSourceType::Hash;
        let mut prefixes = vec![String::new()];
        let mut index_options = FullTextIndexOptions::default();
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "ON" => {
                    source_type = match upper_arg(&frame, idx + 1)?.as_str() {
                        "HASH" => FullTextSourceType::Hash,
                        "JSON" => FullTextSourceType::Json,
                        _ => return Err(Error::msg("ERR syntax error")),
                    };
                    idx += 2;
                }
                "PREFIX" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid PREFIX count")?;
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    prefixes.clear();
                    while idx < end {
                        prefixes.push(arg(&frame, idx, "ERR invalid PREFIX")?);
                        idx += 1;
                    }
                }
                "SCHEMA" => {
                    idx += 1;
                    let schema = parse_schema_fields(&frame, idx)?;
                    return Ok(Self {
                        index,
                        options: FullTextCreateOptions {
                            source_type,
                            prefixes,
                            schema,
                            index_options,
                        },
                    });
                }
                "SKIPINITIALSCAN" => {
                    index_options.skip_initial_scan = true;
                    idx += 1;
                }
                "FILTER" => {
                    index_options.filter = Some(arg(&frame, idx + 1, "ERR invalid FILTER")?);
                    idx += 2;
                }
                "LANGUAGE" => {
                    index_options.language = Some(arg(&frame, idx + 1, "ERR invalid LANGUAGE")?);
                    idx += 2;
                }
                "LANGUAGE_FIELD" => {
                    index_options.language_field =
                        Some(arg(&frame, idx + 1, "ERR invalid LANGUAGE_FIELD")?);
                    idx += 2;
                }
                "SCORE" => {
                    index_options.score =
                        Some(parse_f64_arg(&frame, idx + 1, "ERR invalid SCORE")?);
                    idx += 2;
                }
                "SCORE_FIELD" => {
                    index_options.score_field =
                        Some(arg(&frame, idx + 1, "ERR invalid SCORE_FIELD")?);
                    idx += 2;
                }
                "PAYLOAD_FIELD" => {
                    index_options.payload_field =
                        Some(arg(&frame, idx + 1, "ERR invalid PAYLOAD_FIELD")?);
                    idx += 2;
                }
                "MAXTEXTFIELDS" => {
                    index_options.max_text_fields = true;
                    idx += 1;
                }
                "TEMPORARY" => {
                    index_options.temporary_seconds =
                        Some(parse_u64_arg(&frame, idx + 1, "ERR invalid TEMPORARY")?);
                    idx += 2;
                }
                "NOOFFSETS" => {
                    index_options.no_offsets = true;
                    idx += 1;
                }
                "NOHL" => {
                    index_options.no_hl = true;
                    idx += 1;
                }
                "NOFIELDS" => {
                    index_options.no_fields = true;
                    idx += 1;
                }
                "NOFREQS" => {
                    index_options.no_freqs = true;
                    idx += 1;
                }
                "STOPWORDS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid STOPWORDS count")?;
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    let mut stopwords = Vec::with_capacity(count);
                    while idx < end {
                        stopwords.push(arg(&frame, idx, "ERR invalid STOPWORDS")?);
                        idx += 1;
                    }
                    index_options.stopwords = Some(stopwords);
                }
                "INDEXALL" => {
                    index_options.index_all = Some(match upper_arg(&frame, idx + 1)?.as_str() {
                        "ENABLE" => true,
                        "DISABLE" => false,
                        _ => return Err(Error::msg("ERR syntax error")),
                    });
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Err(Error::msg("ERR syntax error"))
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_create(&self.index, self.options)?;
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_create_async(&self.index, self.options).await?;
        Ok(Frame::Ok)
    }
}

impl FtList {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft._list' command",
            ));
        }
        Ok(Self)
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_list()
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_list_async().await
    }
}

impl FtDropIndex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.dropindex' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let delete_documents = if frame.arg_len() == 3 {
            if upper_arg(&frame, 2)?.as_str() != "DD" {
                return Err(Error::msg("ERR syntax error"));
            }
            true
        } else {
            false
        };
        Ok(Self {
            index,
            delete_documents,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_drop_index(&self.index, self.delete_documents)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_drop_index_async(&self.index, self.delete_documents)
            .await
    }
}

impl FtAlter {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.alter' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let skip_initial_scan =
            upper_arg(&frame, 2)?.as_str() == "SKIPINITIALSCAN";
        let schema_index = if skip_initial_scan { 3 } else { 2 };
        if upper_arg(&frame, schema_index)?.as_str() != "SCHEMA"
            || upper_arg(&frame, schema_index + 1)?.as_str() != "ADD"
        {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self {
            index,
            skip_initial_scan,
            fields: parse_schema_fields(&frame, schema_index + 2)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alter_with_options(
            &self.index,
            self.fields,
            self.skip_initial_scan,
        )
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alter_with_options_async(
            &self.index,
            self.fields,
            self.skip_initial_scan,
        )
        .await
    }
}

impl FtAliasAdd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_alias_with_index(frame, "ft.aliasadd").map(|(alias, index)| Self { alias, index })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_add(&self.alias, &self.index)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_add_async(&self.alias, &self.index).await
    }
}

impl FtAliasUpdate {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_alias_with_index(frame, "ft.aliasupdate").map(|(alias, index)| Self { alias, index })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_update(&self.alias, &self.index)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_update_async(&self.alias, &self.index)
            .await
    }
}

impl FtAliasDel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.aliasdel' command",
            ));
        }
        Ok(Self {
            alias: arg(&frame, 1, "ERR invalid fulltext alias")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_del(&self.alias)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_alias_del_async(&self.alias).await
    }
}

impl FtConfig {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.config' command",
            ));
        }
        match upper_arg(&frame, 1)?.as_str() {
            "GET" if frame.arg_len() == 3 => Ok(Self::Get {
                name: arg(&frame, 2, "ERR invalid config name")?,
            }),
            "SET" if frame.arg_len() == 4 => Ok(Self::Set {
                name: arg(&frame, 2, "ERR invalid config name")?,
                value: arg(&frame, 3, "ERR invalid config value")?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Get { name } => db.fulltext_config_get(&name),
            Self::Set { name, value } => db.fulltext_config_set(&name, &value),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Get { name } => db.fulltext_config_get_async(&name).await,
            Self::Set { name, value } => db.fulltext_config_set_async(&name, &value).await,
        }
    }
}
