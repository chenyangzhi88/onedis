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
