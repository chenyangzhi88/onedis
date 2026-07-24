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
        let mut dialect = None;
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "DISTANCE" => {
                    distance = parse_usize_arg(&frame, idx + 1, "ERR invalid DISTANCE")?;
                    if !(1..=4).contains(&distance) {
                        return Err(Error::msg("ERR DISTANCE must be between 1 and 4"));
                    }
                    idx += 2;
                }
                "TERMS" => {
                    let mode = upper_arg(&frame, idx + 1)?;
                    let name = arg(&frame, idx + 2, "ERR invalid dictionary")?;
                    idx += 3;
                    let mut terms = Vec::new();
                    while idx < frame.arg_len()
                        && !matches!(
                            upper_arg(&frame, idx)?.as_str(),
                            "DISTANCE" | "TERMS" | "DIALECT"
                        )
                    {
                        terms.push(arg(&frame, idx, "ERR invalid dictionary term")?);
                        idx += 1;
                    }
                    let dictionary = FullTextSpellcheckDictionary { name, terms };
                    match mode.as_str() {
                        "INCLUDE" => include.push(dictionary),
                        "EXCLUDE" => exclude.push(dictionary),
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                }
                "DIALECT" => {
                    let value = parse_u64_arg(&frame, idx + 1, "ERR invalid DIALECT")?;
                    if !(1..=4).contains(&value) {
                        return Err(Error::msg("ERR invalid DIALECT"));
                    }
                    dialect = Some(value as u8);
                    idx += 2;
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
            dialect,
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
