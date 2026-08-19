use super::*;
pub(super) const FULLTEXT_GEOSHAPE_OVERSIZE_CELL: &str = "__oversize";
const FULLTEXT_GEOSHAPE_CELL_SIZE: f64 = 0.25;
const FULLTEXT_GEOSHAPE_MAX_CELLS: usize = 256;
#[derive(Clone, Debug)]
pub(super) enum FullTextGeometry {
    Point((f64, f64)),
    Polygon(Vec<(f64, f64)>),
}

pub(super) fn fulltext_geoshape_relation_matches(
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

pub(super) fn parse_fulltext_wkt(raw: &str) -> Result<FullTextGeometry, Error> {
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

pub(super) fn parse_fulltext_wkt_point(raw: &str) -> Result<(f64, f64), Error> {
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

pub(super) fn fulltext_geometry_within(value: &FullTextGeometry, query: &FullTextGeometry) -> bool {
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

pub(super) fn fulltext_geometry_contains(
    value: &FullTextGeometry,
    query: &FullTextGeometry,
) -> bool {
    fulltext_geometry_within(query, value)
}

pub(super) fn fulltext_geometry_bounds(
    geometry: &FullTextGeometry,
) -> Option<(f64, f64, f64, f64)> {
    let points: &[(f64, f64)] = match geometry {
        FullTextGeometry::Point(point) => std::slice::from_ref(point),
        FullTextGeometry::Polygon(points) => points,
    };
    let first = *points.first()?;
    Some(points.iter().skip(1).fold(
        (first.0, first.0, first.1, first.1),
        |(min_x, max_x, min_y, max_y), (x, y)| {
            (min_x.min(*x), max_x.max(*x), min_y.min(*y), max_y.max(*y))
        },
    ))
}

/// Returns a bounded set of grid cells covering a geometry bounding box.
/// `None` marks an oversized shape, which is indexed with a sentinel so small
/// queries cannot accidentally exclude a large containing geometry.
pub(super) fn fulltext_geoshape_cells(bounds: (f64, f64, f64, f64)) -> Option<Vec<String>> {
    let (min_x, max_x, min_y, max_y) = bounds;
    if ![min_x, max_x, min_y, max_y].into_iter().all(f64::is_finite) {
        return None;
    }
    let min_cell_x = (min_x / FULLTEXT_GEOSHAPE_CELL_SIZE).floor() as i64;
    let max_cell_x = (max_x / FULLTEXT_GEOSHAPE_CELL_SIZE).floor() as i64;
    let min_cell_y = (min_y / FULLTEXT_GEOSHAPE_CELL_SIZE).floor() as i64;
    let max_cell_y = (max_y / FULLTEXT_GEOSHAPE_CELL_SIZE).floor() as i64;
    let width = max_cell_x.checked_sub(min_cell_x)?.checked_add(1)?;
    let height = max_cell_y.checked_sub(min_cell_y)?.checked_add(1)?;
    let count = usize::try_from(width)
        .ok()?
        .checked_mul(usize::try_from(height).ok()?)?;
    if count > FULLTEXT_GEOSHAPE_MAX_CELLS {
        return None;
    }
    let mut cells = Vec::with_capacity(count);
    for x in min_cell_x..=max_cell_x {
        for y in min_cell_y..=max_cell_y {
            cells.push(format!("{x}:{y}"));
        }
    }
    Some(cells)
}

pub(super) fn fulltext_point_in_polygon(point: (f64, f64), polygon: &[(f64, f64)]) -> bool {
    let (x, y) = point;
    // Redis GEOSHAPE WITHIN uses strict interior semantics.  Ray casting by
    // itself classifies left and bottom edges inconsistently, so reject every
    // boundary segment before testing the interior.
    for edge in polygon.windows(2) {
        if fulltext_point_on_segment(point, edge[0], edge[1]) {
            return false;
        }
    }
    if polygon.first() != polygon.last()
        && fulltext_point_on_segment(point, *polygon.last().unwrap(), polygon[0])
    {
        return false;
    }
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

fn fulltext_point_on_segment(point: (f64, f64), start: (f64, f64), end: (f64, f64)) -> bool {
    let cross = (point.0 - start.0) * (end.1 - start.1) - (point.1 - start.1) * (end.0 - start.0);
    let scale = (end.0 - start.0).abs() + (end.1 - start.1).abs() + 1.0;
    if cross.abs() > f64::EPSILON * scale * 8.0 {
        return false;
    }
    point.0 >= start.0.min(end.0)
        && point.0 <= start.0.max(end.0)
        && point.1 >= start.1.min(end.1)
        && point.1 <= start.1.max(end.1)
}
