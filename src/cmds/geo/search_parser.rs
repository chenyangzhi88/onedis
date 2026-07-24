fn parse_search(frame: Frame, store: bool) -> Result<(String, Geosearch), Error> {
    let mut idx = if store { 3 } else { 2 };
    if frame.arg_len() <= idx {
        return Err(Error::msg("ERR syntax error"));
    }
    let dest = if store {
        text_arg(&frame, 1)?
    } else {
        String::new()
    };
    let key = text_arg(&frame, if store { 2 } else { 1 })?;
    let center = match text_arg(&frame, idx)?.to_ascii_uppercase().as_str() {
        "FROMMEMBER" if idx + 1 < frame.arg_len() => {
            idx += 2;
            GeoCenter::Member(text_arg(&frame, idx - 1)?)
        }
        "FROMLONLAT" if idx + 2 < frame.arg_len() => {
            let lon = parse_f(&text_arg(&frame, idx + 1)?)?;
            let lat = parse_f(&text_arg(&frame, idx + 2)?)?;
            validate_coord(lon, lat)?;
            idx += 3;
            GeoCenter::Coord(lon, lat)
        }
        _ => return Err(Error::msg("ERR syntax error")),
    };

    if idx >= frame.arg_len() {
        return Err(Error::msg("ERR syntax error"));
    }
    let shape = match text_arg(&frame, idx)?.to_ascii_uppercase().as_str() {
        "BYRADIUS" if idx + 2 < frame.arg_len() => {
            let unit = text_arg(&frame, idx + 2)?;
            let meters = parse_distance(&text_arg(&frame, idx + 1)?, &unit)?;
            idx += 3;
            GeoShape::Radius { meters, unit }
        }
        "BYBOX" if idx + 3 < frame.arg_len() => {
            let unit = text_arg(&frame, idx + 3)?;
            let width_m = parse_distance(&text_arg(&frame, idx + 1)?, &unit)?;
            let height_m = parse_distance(&text_arg(&frame, idx + 2)?, &unit)?;
            idx += 4;
            GeoShape::Box {
                width_m,
                height_m,
                unit,
            }
        }
        _ => return Err(Error::msg("ERR syntax error")),
    };
    let (options, store_options) = parse_search_options(&frame, idx, false, store)?;
    if store && (options.withcoord || options.withdist || options.withhash) {
        return Err(Error::msg("ERR syntax error"));
    }
    validate_search_response_count(&options, store)?;
    Ok((
        dest,
        Geosearch {
            key,
            center,
            shape,
            options,
            store: if store {
                Some(GeoStore {
                    dest: String::new(),
                    dist: store_options.as_ref().is_some_and(|s| s.dist),
                })
            } else {
                store_options
            },
            read_only_alias: false,
        },
    ))
}

fn validate_search_response_count(
    options: &SearchOptions,
    stores_result: bool,
) -> Result<(), Error> {
    if !stores_result
        && options
            .count
            .is_some_and(|count| count > max_geo_result_count(options))
    {
        return Err(Error::msg("ERR COUNT exceeds configured response limit"));
    }
    Ok(())
}

fn parse_search_options(
    frame: &Frame,
    mut idx: usize,
    allow_store_destination: bool,
    allow_storedist_flag: bool,
) -> Result<(SearchOptions, Option<GeoStore>), Error> {
    let mut options = SearchOptions::default();
    let mut store = None;
    while idx < frame.arg_len() {
        match text_arg(frame, idx)?.to_ascii_uppercase().as_str() {
            "WITHDIST" => options.withdist = true,
            "WITHHASH" => options.withhash = true,
            "WITHCOORD" => options.withcoord = true,
            "ASC" => options.sort = Some(GeoSort::Asc),
            "DESC" => options.sort = Some(GeoSort::Desc),
            "COUNT" if idx + 1 < frame.arg_len() => {
                let count = text_arg(frame, idx + 1)?
                    .parse()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                if count == 0 {
                    return Err(Error::msg("ERR COUNT must be > 0"));
                }
                options.count = Some(count);
                idx += 1;
            }
            "ANY" => options.count_any = true,
            "STORE" if allow_store_destination && idx + 1 < frame.arg_len() => {
                store = Some(GeoStore {
                    dest: text_arg(frame, idx + 1)?,
                    dist: false,
                });
                idx += 1;
            }
            "STOREDIST" if allow_store_destination && idx + 1 < frame.arg_len() => {
                store = Some(GeoStore {
                    dest: text_arg(frame, idx + 1)?,
                    dist: true,
                });
                idx += 1;
            }
            "STOREDIST" if allow_storedist_flag => {
                store = Some(GeoStore {
                    dest: String::new(),
                    dist: true,
                });
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
        idx += 1;
    }
    if options.count_any && options.count.is_none() {
        return Err(Error::msg("ERR the ANY argument requires COUNT argument"));
    }
    Ok((options, store))
}

fn parse_f(value: &str) -> Result<f64, Error> {
    value
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR value is not a valid float"))
        .and_then(|v| {
            if v.is_finite() {
                Ok(v)
            } else {
                Err(Error::msg("ERR value is not a valid float"))
            }
        })
}

fn parse_non_negative_f(value: &str) -> Result<f64, Error> {
    let value = parse_f(value)?;
    if value < 0.0 {
        Err(Error::msg("ERR value is out of range, must be positive"))
    } else {
        Ok(value)
    }
}

fn parse_distance(value: &str, unit: &str) -> Result<f64, Error> {
    let meters = parse_non_negative_f(value)? * unit_factor(unit)?;
    if meters.is_finite() {
        Ok(meters)
    } else {
        Err(Error::msg("ERR value is not a valid float"))
    }
}

fn validate_store_options(options: &SearchOptions, store: &Option<GeoStore>) -> Result<(), Error> {
    if store.is_some() && (options.withcoord || options.withdist || options.withhash) {
        Err(Error::msg(
            "ERR STORE option is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ))
    } else {
        Ok(())
    }
}

fn validate_coord(lon: f64, lat: f64) -> Result<(), Error> {
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        Err(Error::msg(
            "ERR invalid longitude,latitude pair; longitude must be between -180 and 180, latitude between -85.05112878 and 85.05112878",
        ))
    } else {
        Ok(())
    }
}
