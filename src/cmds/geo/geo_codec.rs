fn encode_score(lon: f64, lat: f64) -> u64 {
    let lon_bits = encode_axis(lon, GEO_LON_MIN, GEO_LON_MAX);
    let lat_bits = encode_axis(lat, GEO_LAT_MIN, GEO_LAT_MAX);
    interleave(lon_bits, lat_bits)
}

fn encode_axis(value: f64, mut min: f64, mut max: f64) -> u32 {
    let mut bits = 0u32;
    for bit in (0..GEO_STEP).rev() {
        let mid = (min + max) / 2.0;
        if value >= mid {
            bits |= 1 << bit;
            min = mid;
        } else {
            max = mid;
        }
    }
    bits
}

fn interleave(lon_bits: u32, lat_bits: u32) -> u64 {
    let mut out = 0u64;
    for bit in (0..GEO_STEP).rev() {
        out = (out << 1) | ((lon_bits >> bit) & 1) as u64;
        out = (out << 1) | ((lat_bits >> bit) & 1) as u64;
    }
    out
}

fn decode_score(score: u64) -> (f64, f64) {
    let mut lon = (GEO_LON_MIN, GEO_LON_MAX);
    let mut lat = (GEO_LAT_MIN, GEO_LAT_MAX);
    for bit_pair in (0..GEO_STEP).rev() {
        let lon_bit = (score >> (bit_pair * 2 + 1)) & 1;
        let lat_bit = (score >> (bit_pair * 2)) & 1;
        split_range(&mut lon, lon_bit == 1);
        split_range(&mut lat, lat_bit == 1);
    }
    ((lon.0 + lon.1) / 2.0, (lat.0 + lat.1) / 2.0)
}

fn split_range(range: &mut (f64, f64), upper: bool) {
    let mid = (range.0 + range.1) / 2.0;
    if upper {
        range.0 = mid;
    } else {
        range.1 = mid;
    }
}

fn redis_geohash(lon: f64, lat: f64) -> String {
    let mut out = standard_geohash(lon, lat, 10);
    out.push('0');
    out
}

fn standard_geohash(lon: f64, lat: f64, precision: usize) -> String {
    let mut lon_range = (GEO_LON_MIN, GEO_LON_MAX);
    let mut lat_range = (-90.0, 90.0);
    let mut even = true;
    let mut value = 0u8;
    let mut bits = 0u8;
    let mut out = String::with_capacity(precision);
    while out.len() < precision {
        value <<= 1;
        if even {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                value |= 1;
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                value |= 1;
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
        even = !even;
        bits += 1;
        if bits == 5 {
            out.push(GEOHASH_ALPHABET[value as usize] as char);
            bits = 0;
            value = 0;
        }
    }
    out
}

fn unit_factor(unit: &str) -> Result<f64, Error> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.344),
        "ft" => Ok(0.3048),
        _ => Err(Error::msg(
            "ERR unsupported unit provided. please use m, km, ft, mi",
        )),
    }
}

fn shape_unit(shape: &GeoShape) -> String {
    match shape {
        GeoShape::Radius { unit, .. } | GeoShape::Box { unit, .. } => unit.clone(),
    }
}

fn bulk_f(value: f64) -> Frame {
    Frame::bulk_string(format!("{:.17}", value))
}

fn distance_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    let dlat = (b.1 - a.1).to_radians();
    let dlon = (b.0 - a.0).to_radians();
    let lat1 = a.1.to_radians();
    let lat2 = b.1.to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().asin()
}
