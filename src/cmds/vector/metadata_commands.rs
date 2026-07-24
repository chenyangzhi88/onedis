impl VRem {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vrem' command",
            ));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_del(&self.key, &[self.element])? as i64
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_del_async(&self.key, &[self.element]).await? as i64,
        ))
    }
}

impl VCard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vcard")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(db.vector_card(&self.key)? as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_card_async(&self.key).await? as i64,
        ))
    }
}

impl VDim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vdim")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(db
            .vector_dim(&self.key)?
            .map(|dim| Frame::Integer(dim as i64))
            .unwrap_or(Frame::Null))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(db
            .vector_dim_async(&self.key)
            .await?
            .map(|dim| Frame::Integer(dim as i64))
            .unwrap_or(Frame::Null))
    }
}

impl VEmb {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 || frame.arg_len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vemb' command",
            ));
        }
        let raw = frame.arg_len() == 4;
        if raw && upper_arg(&frame, 3)? != "RAW" {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            raw,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(match db.vector_element(&self.key, &self.element)? {
            Some(element) => vector_embedding_frame(element.vector, self.raw),
            None => Frame::Null,
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(
            match db.vector_element_async(&self.key, &self.element).await? {
                Some(element) => vector_embedding_frame(element.vector, self.raw),
                None => Frame::Null,
            },
        )
    }
}

impl VGetAttr {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vgetattr' command",
            ));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_attr_frame(
            db.vector_element(&self.key, &self.element)?
                .map(|element| element.attrs_json),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_attr_frame(
            db.vector_element_async(&self.key, &self.element)
                .await?
                .map(|element| element.attrs_json),
        ))
    }
}

impl VSetAttr {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vsetattr' command",
            ));
        }
        let attrs = arg(&frame, 3, "ERR invalid vector attrs")?;
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            attrs_json: (!attrs.is_empty()).then_some(attrs),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_set_attrs(&self.key, &self.element, self.attrs_json)? as i64,
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_set_attrs_async(&self.key, &self.element, self.attrs_json)
                .await? as i64,
        ))
    }
}

impl VInfo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vinfo")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(info_frame(db.vector_info(&self.key)?))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(info_frame(db.vector_info_async(&self.key).await?))
    }
}

impl VRandMember {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vrandmember' command",
            ));
        }
        let count = if frame.arg_len() == 3 {
            Some(
                arg(&frame, 2, "ERR invalid vector count")?
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR invalid vector count"))?,
            )
        } else {
            None
        };
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_vrandmember_frame(
            db.vector_ids(&self.key)?,
            self.count,
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_vrandmember_frame(
            db.vector_ids_async(&self.key).await?,
            self.count,
        ))
    }
}

impl VLinks {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 || frame.arg_len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vlinks' command",
            ));
        }
        let with_scores = frame.arg_len() == 4;
        if with_scores && upper_arg(&frame, 3)? != "WITHSCORES" {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            with_scores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let element = db
            .vector_element(&self.key, &self.element)?
            .ok_or_else(|| Error::msg("ERR vector element does not exist"))?;
        let results = db.vector_search(
            &self.key,
            &element.vector,
            VectorSearchOptions {
                k: 17,
                filter: None,
                with_scores: false,
                with_attrs: Vec::new(),
                ef: None,
                offset: 0,
                limit: Some(17),
            },
        )?;
        Ok(redis_vlinks_frame(results, &self.element, self.with_scores))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let element = db
            .vector_element_async(&self.key, &self.element)
            .await?
            .ok_or_else(|| Error::msg("ERR vector element does not exist"))?;
        let results = db
            .vector_search_async(
                &self.key,
                &element.vector,
                VectorSearchOptions {
                    k: 17,
                    filter: None,
                    with_scores: false,
                    with_attrs: Vec::new(),
                    ef: None,
                    offset: 0,
                    limit: Some(17),
                },
            )
            .await?;
        Ok(redis_vlinks_frame(results, &self.element, self.with_scores))
    }
}
