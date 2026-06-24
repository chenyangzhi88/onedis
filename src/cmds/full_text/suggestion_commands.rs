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
