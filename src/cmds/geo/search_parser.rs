fn parse_search(frame: Frame, store: bool) -> Result<(String, Geosearch), Error> {
    let mut idx = if store { 3 } else { 2 };
    if frame.arg_len() <= idx {
        return Err(Error::msg("ERR syntax error"));
    }
    let dest = if store {
        frame.get_arg(1).unwrap()
    } else {
        String::new()
    };
    let key = frame.get_arg(if store { 2 } else { 1 }).unwrap();
    let center = match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
        "FROMMEMBER" if idx + 1 < frame.arg_len() => {
            idx += 2;
            GeoCenter::Member(frame.get_arg(idx - 1).unwrap())
        }
        "FROMLONLAT" if idx + 2 < frame.arg_len() => {
            let lon = parse_f(&frame.get_arg(idx + 1).unwrap())?;
            let lat = parse_f(&frame.get_arg(idx + 2).unwrap())?;
            validate_coord(lon, lat)?;
            idx += 3;
            GeoCenter::Coord(lon, lat)
        }
        _ => return Err(Error::msg("ERR syntax error")),
    };

    if idx >= frame.arg_len() {
        return Err(Error::msg("ERR syntax error"));
    }
    let shape = match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
        "BYRADIUS" if idx + 2 < frame.arg_len() => {
            let unit = frame.get_arg(idx + 2).unwrap();
            let meters =
                parse_non_negative_f(&frame.get_arg(idx + 1).unwrap())? * unit_factor(&unit)?;
            idx += 3;
            GeoShape::Radius { meters, unit }
        }
        "BYBOX" if idx + 3 < frame.arg_len() => {
            let unit = frame.get_arg(idx + 3).unwrap();
            let factor = unit_factor(&unit)?;
            let width_m = parse_non_negative_f(&frame.get_arg(idx + 1).unwrap())? * factor;
            let height_m = parse_non_negative_f(&frame.get_arg(idx + 2).unwrap())? * factor;
            idx += 4;
            GeoShape::Box {
                width_m,
                height_m,
                unit,
            }
        }
        _ => return Err(Error::msg("ERR syntax error")),
    };
    let (mut options, store_options) =
        parse_search_options(&frame, idx, false, shape_unit(&shape))?;
    if store {
        options.withcoord = false;
        options.withdist = false;
        options.withhash = false;
    }
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
        },
    ))
}

fn parse_search_options(
    frame: &Frame,
    mut idx: usize,
    allow_store: bool,
    unit: String,
) -> Result<(SearchOptions, Option<GeoStore>), Error> {
    let mut options = SearchOptions::default();
    let mut store = None;
    while idx < frame.arg_len() {
        match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
            "WITHDIST" => options.withdist = true,
            "WITHHASH" => options.withhash = true,
            "WITHCOORD" => options.withcoord = true,
            "ASC" => options.sort = Some(GeoSort::Asc),
            "DESC" => options.sort = Some(GeoSort::Desc),
            "COUNT" if idx + 1 < frame.arg_len() => {
                options.count = Some(
                    frame
                        .get_arg(idx + 1)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                );
                idx += 1;
                if idx + 1 < frame.arg_len()
                    && frame.get_arg(idx + 1).unwrap().eq_ignore_ascii_case("ANY")
                {
                    options.count_any = true;
                    idx += 1;
                }
            }
            "STORE" if allow_store && idx + 1 < frame.arg_len() => {
                store = Some(GeoStore {
                    dest: frame.get_arg(idx + 1).unwrap(),
                    dist: false,
                });
                idx += 1;
            }
            "STOREDIST" if allow_store && idx + 1 < frame.arg_len() => {
                store = Some(GeoStore {
                    dest: frame.get_arg(idx + 1).unwrap(),
                    dist: true,
                });
                idx += 1;
            }
            "STOREDIST" if !allow_store => {
                store = Some(GeoStore {
                    dest: String::new(),
                    dist: true,
                });
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
        idx += 1;
    }
    let _ = unit;
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

fn validate_coord(lon: f64, lat: f64) -> Result<(), Error> {
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        Err(Error::msg(
            "ERR invalid longitude,latitude pair; longitude must be between -180 and 180, latitude between -85.05112878 and 85.05112878",
        ))
    } else {
        Ok(())
    }
}
