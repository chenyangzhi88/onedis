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
