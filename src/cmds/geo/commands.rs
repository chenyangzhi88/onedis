impl Geoadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geoadd' command",
            ));
        }

        let key = frame.get_arg(1).unwrap();
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;
        let mut idx = 2;
        while idx < frame.arg_len() {
            match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
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
            let lon = parse_f(&frame.get_arg(idx).unwrap())?;
            let lat = parse_f(&frame.get_arg(idx + 1).unwrap())?;
            validate_coord(lon, lat)?;
            items.push((lon, lat, frame.get_arg(idx + 2).unwrap()));
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
        let mut unique = std::collections::HashMap::new();
        for (lon, lat, member) in self.items {
            unique.insert(member, (lon, lat));
        }
        let mut writes = Vec::new();
        let mut changed = 0usize;
        let mut added = 0usize;
        for (member, (lon, lat)) in unique {
            let score = encode_score(lon, lat);
            let previous = db.zset_score(&self.key, &member)?;
            if self.nx && previous.is_some() {
                continue;
            }
            if self.xx && previous.is_none() {
                continue;
            }
            if previous.is_none() {
                added += 1;
                changed += 1;
            } else if previous != Some(score as f64) {
                changed += 1;
            }
            writes.push((score as f64, member));
        }

        if !writes.is_empty() {
            db.zset_add(&self.key, &writes)?;
        }
        Ok(Frame::Integer(if self.ch { changed } else { added } as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut unique = std::collections::HashMap::new();
        for (lon, lat, member) in self.items {
            unique.insert(member, (lon, lat));
        }
        let mut writes = Vec::new();
        let mut changed = 0usize;
        let mut added = 0usize;
        for (member, (lon, lat)) in unique {
            let score = encode_score(lon, lat);
            let previous = db.zset_score_async(&self.key, &member).await?;
            if self.nx && previous.is_some() {
                continue;
            }
            if self.xx && previous.is_none() {
                continue;
            }
            if previous.is_none() {
                added += 1;
                changed += 1;
            } else if previous != Some(score as f64) {
                changed += 1;
            }
            writes.push((score as f64, member));
        }

        if !writes.is_empty() {
            db.zset_add_async(&self.key, &writes).await?;
        }
        Ok(Frame::Integer(if self.ch { changed } else { added } as i64))
    }
}

impl Geopos {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geopos' command",
            ));
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            members: (2..frame.arg_len())
                .map(|i| frame.get_arg(i).unwrap())
                .collect(),
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
        Ok(Frame::Array(frames))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score_async(&self.key, &member).await {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::Array(vec![bulk_f(lon), bulk_f(lat)]));
                }
                Ok(None) => frames.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Array(frames))
    }
}

impl Geodist {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || frame.arg_len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'geodist' command",
            ));
        }
        let unit = frame.get_arg(4).unwrap_or_else(|| "m".to_string());
        unit_factor(&unit)?;
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            a: frame.get_arg(2).unwrap(),
            b: frame.get_arg(3).unwrap(),
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
        let Some(a) = db.zset_score_async(&self.key, &self.a).await? else {
            return Ok(Frame::Null);
        };
        let Some(b) = db.zset_score_async(&self.key, &self.b).await? else {
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
            key: frame.get_arg(1).unwrap(),
            members: (2..frame.arg_len())
                .map(|i| frame.get_arg(i).unwrap())
                .collect(),
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
        Ok(Frame::Array(frames))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score_async(&self.key, &member).await {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::bulk_string(redis_geohash(lon, lat)));
                }
                Ok(None) => frames.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Array(frames))
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
        let key = frame.get_arg(1).unwrap();
        let lon = parse_f(&frame.get_arg(2).unwrap())?;
        let lat = parse_f(&frame.get_arg(3).unwrap())?;
        validate_coord(lon, lat)?;
        let unit = frame.get_arg(5).unwrap();
        let radius = parse_non_negative_f(&frame.get_arg(4).unwrap())? * unit_factor(&unit)?;
        let (options, store) = parse_search_options(&frame, 6, !read_only_alias, false)?;
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
        let key = frame.get_arg(1).unwrap();
        let member = frame.get_arg(2).unwrap();
        let unit = frame.get_arg(4).unwrap();
        let radius = parse_non_negative_f(&frame.get_arg(3).unwrap())? * unit_factor(&unit)?;
        let (options, store) = parse_search_options(&frame, 5, !read_only_alias, false)?;
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
                Ok(Frame::Array(
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
                    return store_entries_async(db, store, &entries, self.shape.unit_factor()).await;
                }
                Ok(Frame::Array(
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
