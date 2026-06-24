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
