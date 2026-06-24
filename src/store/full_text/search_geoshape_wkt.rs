#[derive(Clone, Debug)]
enum FullTextGeometry {
    Point((f64, f64)),
    Polygon(Vec<(f64, f64)>),
}

fn fulltext_geoshape_relation_matches(
    value: &str,
    relation: &str,
    query_shape: &str,
) -> Result<bool, Error> {
    let value = parse_fulltext_wkt(value)?;
    let query = parse_fulltext_wkt(query_shape)?;
    match relation.to_ascii_uppercase().as_str() {
        "WITHIN" => Ok(fulltext_geometry_within(&value, &query)),
        "CONTAINS" => Ok(fulltext_geometry_contains(&value, &query)),
        _ => Err(Error::msg("ERR invalid GEOSHAPE relation")),
    }
}

fn parse_fulltext_wkt(raw: &str) -> Result<FullTextGeometry, Error> {
    let raw = raw.trim();
    let upper = raw.to_ascii_uppercase();
    if upper.starts_with("POINT") {
        let body = raw
            .trim_start_matches(|ch: char| ch.is_ascii_alphabetic())
            .trim();
        let body = body
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .ok_or_else(|| Error::msg("ERR invalid WKT"))?;
        return Ok(FullTextGeometry::Point(parse_fulltext_wkt_point(body)?));
    }
    if upper.starts_with("POLYGON") {
        let body = raw
            .trim_start_matches(|ch: char| ch.is_ascii_alphabetic())
            .trim();
        let body = body
            .strip_prefix("((")
            .and_then(|value| value.strip_suffix("))"))
            .ok_or_else(|| Error::msg("ERR invalid WKT"))?;
        let points = body
            .split(',')
            .map(parse_fulltext_wkt_point)
            .collect::<Result<Vec<_>, _>>()?;
        if points.len() < 4 {
            return Err(Error::msg("ERR invalid WKT polygon"));
        }
        return Ok(FullTextGeometry::Polygon(points));
    }
    Err(Error::msg("ERR unsupported WKT geometry"))
}

fn parse_fulltext_wkt_point(raw: &str) -> Result<(f64, f64), Error> {
    let parts = raw.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::msg("ERR invalid WKT point"));
    }
    let x = parts[0]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid WKT point"))?;
    let y = parts[1]
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR invalid WKT point"))?;
    if !x.is_finite() || !y.is_finite() {
        return Err(Error::msg("ERR invalid WKT point"));
    }
    Ok((x, y))
}

fn fulltext_geometry_within(value: &FullTextGeometry, query: &FullTextGeometry) -> bool {
    match (value, query) {
        (FullTextGeometry::Point(point), FullTextGeometry::Polygon(poly)) => {
            fulltext_point_in_polygon(*point, poly)
        }
        (FullTextGeometry::Point(left), FullTextGeometry::Point(right)) => left == right,
        (FullTextGeometry::Polygon(poly), FullTextGeometry::Polygon(container)) => poly
            .iter()
            .all(|point| fulltext_point_in_polygon(*point, container)),
        (FullTextGeometry::Polygon(_), FullTextGeometry::Point(_)) => false,
    }
}

fn fulltext_geometry_contains(value: &FullTextGeometry, query: &FullTextGeometry) -> bool {
    fulltext_geometry_within(query, value)
}

fn fulltext_point_in_polygon(point: (f64, f64), polygon: &[(f64, f64)]) -> bool {
    let (x, y) = point;
    let mut inside = false;
    let mut j = polygon.len() - 1;
    for i in 0..polygon.len() {
        let (xi, yi) = polygon[i];
        let (xj, yj) = polygon[j];
        let denom = yj - yi;
        if denom.abs() > f64::EPSILON
            && ((yi > y) != (yj > y))
            && (x < (xj - xi) * (y - yi) / denom + xi)
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}
