fn fulltext_geo_value_within(
    value: &str,
    lon: f64,
    lat: f64,
    radius: f64,
    unit: &str,
) -> Result<bool, Error> {
    if radius < 0.0 || !radius.is_finite() {
        return Err(Error::msg("ERR invalid geo radius"));
    }
    let (value_lon, value_lat) = parse_fulltext_geo_value(value)?;
    let radius_meters = radius * fulltext_geo_unit_meters(unit)?;
    Ok(fulltext_haversine_meters(lat, lon, value_lat, value_lon) <= radius_meters)
}

fn parse_fulltext_geo_value(value: &str) -> Result<(f64, f64), Error> {
    let parts = value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::msg("ERR invalid GEO value"));
    }
    let lon = parts[0]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid GEO value"))?;
    let lat = parts[1]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid GEO value"))?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(Error::msg("ERR invalid GEO value"));
    }
    Ok((lon, lat))
}

fn fulltext_geo_unit_meters(unit: &str) -> Result<f64, Error> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.344),
        "ft" => Ok(0.3048),
        _ => Err(Error::msg("ERR invalid geo unit")),
    }
}

fn fulltext_haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let radius_meters = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * radius_meters * a.sqrt().asin()
}
