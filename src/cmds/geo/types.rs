pub struct Geoadd {
    key: String,
    items: Vec<(f64, f64, String)>,
    nx: bool,
    xx: bool,
    ch: bool,
}

pub struct Geopos {
    key: String,
    members: Vec<String>,
}

pub struct Geodist {
    key: String,
    a: String,
    b: String,
    unit: String,
}

pub struct Geohash {
    key: String,
    members: Vec<String>,
}

pub struct Geosearch {
    key: String,
    center: GeoCenter,
    shape: GeoShape,
    options: SearchOptions,
    store: Option<GeoStore>,
}

pub struct Geosearchstore {
    dest: String,
    search: Geosearch,
}

enum GeoCenter {
    Member(String),
    Coord(f64, f64),
}

enum GeoShape {
    Radius {
        meters: f64,
        unit: String,
    },
    Box {
        width_m: f64,
        height_m: f64,
        unit: String,
    },
}

#[derive(Default)]
struct SearchOptions {
    withdist: bool,
    withhash: bool,
    withcoord: bool,
    sort: Option<GeoSort>,
    count: Option<usize>,
    count_any: bool,
}

#[derive(Clone, Copy)]
enum GeoSort {
    Asc,
    Desc,
}

struct GeoStore {
    dest: String,
    dist: bool,
}

struct GeoResult {
    member: String,
    score: u64,
    lon: f64,
    lat: f64,
    distance_m: f64,
}

pub type Georadius = Geosearch;
pub type Georadiusbymember = Geosearch;

impl GeoShape {
    fn unit_factor(&self) -> f64 {
        match self {
            GeoShape::Radius { unit, .. } | GeoShape::Box { unit, .. } => {
                unit_factor(unit).unwrap_or(1.0)
            }
        }
    }
}
