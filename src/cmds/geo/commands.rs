impl Geoadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geoadd' command",
            ));
        }

        let key = text_arg(&frame, 1)?;
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;
        let mut idx = 2;
        while idx < frame.arg_len() {
            match text_arg(&frame, idx)?.to_ascii_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "CH" => ch = true,
                _ => break,
            }
            idx += 1;
        }
        if nx && xx {
            return Err(Error::msg(
                "ERR XX and NX options at the same time are not compatible",
            ));
        }
        if idx >= frame.arg_len() || !(frame.arg_len() - idx).is_multiple_of(3) {
            return Err(Error::msg("ERR syntax error"));
        }

        let mut items = Vec::new();
        while idx < frame.arg_len() {
            let lon = parse_f(&text_arg(&frame, idx)?)?;
            let lat = parse_f(&text_arg(&frame, idx + 1)?)?;
            validate_coord(lon, lat)?;
            items.push((lon, lat, text_arg(&frame, idx + 2)?));
            idx += 3;
        }
        Ok(Self {
            key,
            items,
            nx,
            xx,
            ch,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let writes = self
            .items
            .into_iter()
            .map(|(lon, lat, member)| (encode_score(lon, lat) as f64, member))
            .collect::<Vec<_>>();
        let outcome = db.zset_add_with_options(
            &self.key,
            &writes,
            ZsetAddOptions {
                nx: self.nx,
                xx: self.xx,
                ..ZsetAddOptions::default()
            },
        )?;
        Ok(Frame::Integer(if self.ch {
            outcome.changed
        } else {
            outcome.added
        } as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let writes = self
            .items
            .into_iter()
            .map(|(lon, lat, member)| (encode_score(lon, lat) as f64, member))
            .collect::<Vec<_>>();
        let outcome = db
            .zset_add_with_options_async(
                &self.key,
                &writes,
                ZsetAddOptions {
                    nx: self.nx,
                    xx: self.xx,
                    ..ZsetAddOptions::default()
                },
            )
            .await?;
        Ok(Frame::Integer(if self.ch {
            outcome.changed
        } else {
            outcome.added
        } as i64))
    }
}

impl Geopos {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geopos' command",
            ));
        }
        if frame.arg_len() - 2 > (MAX_FRAME_NODES - 1) / 3 {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        Ok(Self {
            key: text_arg(&frame, 1)?,
            members: (2..frame.arg_len())
                .map(|i| text_arg(&frame, i))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score(&self.key, &member) {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::Array(vec![bulk_f(lon), bulk_f(lat)]));
                }
                Ok(None) => frames.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        bounded_geo_frame(Frame::Array(frames))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let scores = match db.zset_multi_score_async(&self.key, &self.members).await {
            Ok(scores) => scores,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        let frames = scores
            .into_iter()
            .map(|score| match score {
                Some(score) => {
                    let (lon, lat) = decode_score(score as u64);
                    Frame::Array(vec![bulk_f(lon), bulk_f(lat)])
                }
                None => Frame::Null,
            })
            .collect();
        bounded_geo_frame(Frame::Array(frames))
    }
}

impl Geodist {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || frame.arg_len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geodist' command",
            ));
        }
        let unit = if frame.arg_len() == 5 {
            text_arg(&frame, 4)?
        } else {
            "m".to_string()
        };
        unit_factor(&unit)?;
        Ok(Self {
            key: text_arg(&frame, 1)?,
            a: text_arg(&frame, 2)?,
            b: text_arg(&frame, 3)?,
            unit,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let factor = unit_factor(&self.unit)?;
        let Some(a) = db.zset_score(&self.key, &self.a)? else {
            return Ok(Frame::Null);
        };
        let Some(b) = db.zset_score(&self.key, &self.b)? else {
            return Ok(Frame::Null);
        };
        let meters = distance_m(decode_score(a as u64), decode_score(b as u64));
        Ok(Frame::bulk_string(format!("{:.4}", meters / factor)))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let factor = unit_factor(&self.unit)?;
        let scores = db
            .zset_multi_score_async(&self.key, &[self.a, self.b])
            .await?;
        let Some(a) = scores[0] else {
            return Ok(Frame::Null);
        };
        let Some(b) = scores[1] else {
            return Ok(Frame::Null);
        };
        let meters = distance_m(decode_score(a as u64), decode_score(b as u64));
        Ok(Frame::bulk_string(format!("{:.4}", meters / factor)))
    }
}

impl Geohash {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geohash' command",
            ));
        }
        Ok(Self {
            key: text_arg(&frame, 1)?,
            members: (2..frame.arg_len())
                .map(|i| text_arg(&frame, i))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score(&self.key, &member) {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::bulk_string(redis_geohash(lon, lat)));
                }
                Ok(None) => frames.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        bounded_geo_frame(Frame::Array(frames))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let scores = match db.zset_multi_score_async(&self.key, &self.members).await {
            Ok(scores) => scores,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        let frames = scores
            .into_iter()
            .map(|score| match score {
                Some(score) => {
                    let (lon, lat) = decode_score(score as u64);
                    Frame::bulk_string(redis_geohash(lon, lat))
                }
                None => Frame::Null,
            })
            .collect();
        bounded_geo_frame(Frame::Array(frames))
    }
}

impl Geosearch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_search(frame, false).map(|(_, search)| search)
    }

    pub fn parse_georadius_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'georadius' command",
            ));
        }
        let read_only_alias = frame
            .get_arg(0)
            .is_some_and(|name| name.eq_ignore_ascii_case("GEORADIUS_RO"));
        let key = text_arg(&frame, 1)?;
        let lon = parse_f(&text_arg(&frame, 2)?)?;
        let lat = parse_f(&text_arg(&frame, 3)?)?;
        validate_coord(lon, lat)?;
        let unit = text_arg(&frame, 5)?;
        let radius = parse_distance(&text_arg(&frame, 4)?, &unit)?;
        let (options, store) = parse_search_options(&frame, 6, !read_only_alias, false)?;
        validate_store_options(&options, &store)?;
        validate_search_response_count(&options, store.is_some())?;
        Ok(Self {
            key,
            center: GeoCenter::Coord(lon, lat),
            shape: GeoShape::Radius {
                meters: radius,
                unit,
            },
            options,
            store,
            read_only_alias,
        })
    }

    pub fn parse_georadiusbymember_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'georadiusbymember' command",
            ));
        }
        let read_only_alias = frame
            .get_arg(0)
            .is_some_and(|name| name.eq_ignore_ascii_case("GEORADIUSBYMEMBER_RO"));
        let key = text_arg(&frame, 1)?;
        let member = text_arg(&frame, 2)?;
        let unit = text_arg(&frame, 4)?;
        let radius = parse_distance(&text_arg(&frame, 3)?, &unit)?;
        let (options, store) = parse_search_options(&frame, 5, !read_only_alias, false)?;
        validate_store_options(&options, &store)?;
        validate_search_response_count(&options, store.is_some())?;
        Ok(Self {
            key,
            center: GeoCenter::Member(member),
            shape: GeoShape::Radius {
                meters: radius,
                unit,
            },
            options,
            store,
            read_only_alias,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match search_entries(db, &self) {
            Ok(entries) => {
                if let Some(store) = &self.store {
                    return store_entries(db, store, &entries, self.shape.unit_factor());
                }
                bounded_geo_frame(Frame::Array(
                    entries
                        .into_iter()
                        .map(|entry| {
                            render_search_entry(entry, &self.options, self.shape.unit_factor())
                        })
                        .collect(),
                ))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match search_entries_async(db, &self).await {
            Ok(entries) => {
                if let Some(store) = &self.store {
                    return store_entries_async(db, store, &entries, self.shape.unit_factor())
                        .await;
                }
                bounded_geo_frame(Frame::Array(
                    entries
                        .into_iter()
                        .map(|entry| {
                            render_search_entry(entry, &self.options, self.shape.unit_factor())
                        })
                        .collect(),
                ))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

impl Geosearchstore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let (dest, search) = parse_search(frame, true)?;
        Ok(Self { dest, search })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match search_entries(db, &self.search).and_then(|entries| {
            store_entries(
                db,
                &GeoStore {
                    dest: self.dest,
                    dist: self.search.store.as_ref().is_some_and(|store| store.dist),
                },
                &entries,
                self.search.shape.unit_factor(),
            )
        }) {
            Ok(frame) => Ok(frame),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match search_entries_async(db, &self.search).await {
            Ok(entries) => {
                store_entries_async(
                    db,
                    &GeoStore {
                        dest: self.dest,
                        dist: self.search.store.as_ref().is_some_and(|store| store.dist),
                    },
                    &entries,
                    self.search.shape.unit_factor(),
                )
                .await
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
