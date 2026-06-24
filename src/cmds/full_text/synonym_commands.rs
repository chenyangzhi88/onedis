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
