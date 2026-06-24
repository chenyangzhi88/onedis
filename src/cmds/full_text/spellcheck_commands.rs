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
