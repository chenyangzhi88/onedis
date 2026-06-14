use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
use std::cmp::Ordering;

const GEO_STEP: usize = 26;
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const EARTH_RADIUS_M: f64 = 6372797.560856;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

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
        if idx >= frame.arg_len() || (frame.arg_len() - idx) % 3 != 0 {
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
        let mut writes = Vec::new();
        let mut changed = 0usize;
        let mut added = 0usize;
        for (lon, lat, member) in self.items {
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
        let mut writes = Vec::new();
        let mut changed = 0usize;
        let mut added = 0usize;
        for (lon, lat, member) in self.items {
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
        Ok(Frame::Array(
            self.members
                .into_iter()
                .map(|member| match db.zset_score(&self.key, &member) {
                    Ok(Some(score)) => {
                        let (lon, lat) = decode_score(score as u64);
                        Frame::Array(vec![bulk_f(lon), bulk_f(lat)])
                    }
                    _ => Frame::Null,
                })
                .collect(),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score_async(&self.key, &member).await {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::Array(vec![bulk_f(lon), bulk_f(lat)]));
                }
                _ => frames.push(Frame::Null),
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
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            a: frame.get_arg(2).unwrap(),
            b: frame.get_arg(3).unwrap(),
            unit: frame.get_arg(4).unwrap_or_else(|| "m".to_string()),
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
        Ok(Frame::Array(
            self.members
                .into_iter()
                .map(|member| match db.zset_score(&self.key, &member) {
                    Ok(Some(score)) => {
                        let (lon, lat) = decode_score(score as u64);
                        Frame::bulk_string(redis_geohash(lon, lat))
                    }
                    _ => Frame::Null,
                })
                .collect(),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut frames = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score_async(&self.key, &member).await {
                Ok(Some(score)) => {
                    let (lon, lat) = decode_score(score as u64);
                    frames.push(Frame::bulk_string(redis_geohash(lon, lat)));
                }
                _ => frames.push(Frame::Null),
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
        let key = frame.get_arg(1).unwrap();
        let lon = parse_f(&frame.get_arg(2).unwrap())?;
        let lat = parse_f(&frame.get_arg(3).unwrap())?;
        validate_coord(lon, lat)?;
        let unit = frame.get_arg(5).unwrap();
        let radius = parse_non_negative_f(&frame.get_arg(4).unwrap())? * unit_factor(&unit)?;
        let (options, store) = parse_search_options(&frame, 6, true, unit.clone())?;
        Ok(Self {
            key,
            center: GeoCenter::Coord(lon, lat),
            shape: GeoShape::Radius {
                meters: radius,
                unit,
            },
            options,
            store,
        })
    }

    pub fn parse_georadiusbymember_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'georadiusbymember' command",
            ));
        }
        let key = frame.get_arg(1).unwrap();
        let member = frame.get_arg(2).unwrap();
        let unit = frame.get_arg(4).unwrap();
        let radius = parse_non_negative_f(&frame.get_arg(3).unwrap())? * unit_factor(&unit)?;
        let (options, store) = parse_search_options(&frame, 5, true, unit.clone())?;
        Ok(Self {
            key,
            center: GeoCenter::Member(member),
            shape: GeoShape::Radius {
                meters: radius,
                unit,
            },
            options,
            store,
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
                    return store_entries_async(db, store, &entries, self.shape.unit_factor())
                        .await;
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
                options.count =
                    Some(
                        frame.get_arg(idx + 1).unwrap().parse().map_err(|_| {
                            Error::msg("ERR value is not an integer or out of range")
                        })?,
                    );
                idx += 1;
                if idx + 1 < frame.arg_len()
                    && frame.get_arg(idx + 1).unwrap().eq_ignore_ascii_case("ANY")
                {
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

fn search_entries(db: &Db, search: &Geosearch) -> Result<Vec<GeoResult>, Error> {
    let center = match &search.center {
        GeoCenter::Coord(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => db
            .zset_score(&search.key, member)?
            .map(|score| decode_score(score as u64))
            .ok_or_else(|| Error::msg("ERR could not decode requested zset member"))?,
    };
    let mut entries = db
        .zset_all_entries(&search.key)?
        .into_iter()
        .filter_map(|(member, raw_score)| {
            let score = raw_score as u64;
            let (lon, lat) = decode_score(score);
            let distance_m = distance_m(center, (lon, lat));
            shape_contains(&search.shape, center, (lon, lat), distance_m).then_some(GeoResult {
                member,
                score,
                lon,
                lat,
                distance_m,
            })
        })
        .collect::<Vec<_>>();

    if let Some(sort) = search.options.sort {
        entries.sort_by(|a, b| {
            let ord = a
                .distance_m
                .partial_cmp(&b.distance_m)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.member.cmp(&b.member));
            match sort {
                GeoSort::Asc => ord,
                GeoSort::Desc => ord.reverse(),
            }
        });
    }
    if let Some(count) = search.options.count {
        entries.truncate(count);
    }
    Ok(entries)
}

async fn search_entries_async(db: &Db, search: &Geosearch) -> Result<Vec<GeoResult>, Error> {
    let center = match &search.center {
        GeoCenter::Coord(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => db
            .zset_score_async(&search.key, member)
            .await?
            .map(|score| decode_score(score as u64))
            .ok_or_else(|| Error::msg("ERR could not decode requested zset member"))?,
    };
    let mut entries = db
        .zset_all_entries_async(&search.key)
        .await?
        .into_iter()
        .filter_map(|(member, raw_score)| {
            let score = raw_score as u64;
            let (lon, lat) = decode_score(score);
            let distance_m = distance_m(center, (lon, lat));
            shape_contains(&search.shape, center, (lon, lat), distance_m).then_some(GeoResult {
                member,
                score,
                lon,
                lat,
                distance_m,
            })
        })
        .collect::<Vec<_>>();

    if let Some(sort) = search.options.sort {
        entries.sort_by(|a, b| {
            let ord = a
                .distance_m
                .partial_cmp(&b.distance_m)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.member.cmp(&b.member));
            match sort {
                GeoSort::Asc => ord,
                GeoSort::Desc => ord.reverse(),
            }
        });
    }
    if let Some(count) = search.options.count {
        entries.truncate(count);
    }
    Ok(entries)
}

fn shape_contains(shape: &GeoShape, center: (f64, f64), point: (f64, f64), distance: f64) -> bool {
    match shape {
        GeoShape::Radius { meters, .. } => distance <= *meters,
        GeoShape::Box {
            width_m, height_m, ..
        } => {
            let horizontal = distance_m((center.0, point.1), point);
            let vertical = distance_m((point.0, center.1), point);
            horizontal <= *width_m / 2.0 && vertical <= *height_m / 2.0
        }
    }
}

fn render_search_entry(entry: GeoResult, options: &SearchOptions, unit_factor: f64) -> Frame {
    if !options.withdist && !options.withhash && !options.withcoord {
        return Frame::bulk_string(entry.member);
    }
    let mut parts = vec![Frame::bulk_string(entry.member)];
    if options.withdist {
        parts.push(Frame::bulk_string(format!(
            "{:.4}",
            entry.distance_m / unit_factor
        )));
    }
    if options.withhash {
        parts.push(Frame::Integer(entry.score as i64));
    }
    if options.withcoord {
        parts.push(Frame::Array(vec![bulk_f(entry.lon), bulk_f(entry.lat)]));
    }
    Frame::Array(parts)
}

fn store_entries(
    db: &Db,
    store: &GeoStore,
    entries: &[GeoResult],
    unit_factor: f64,
) -> Result<Frame, Error> {
    let stored = entries
        .iter()
        .map(|entry| {
            (
                entry.member.clone(),
                if store.dist {
                    entry.distance_m / unit_factor
                } else {
                    entry.score as f64
                },
            )
        })
        .collect::<Vec<_>>();
    db.zset_store_entries(&store.dest, stored)
        .map(|n| Frame::Integer(n as i64))
}

async fn store_entries_async(
    db: &Db,
    store: &GeoStore,
    entries: &[GeoResult],
    unit_factor: f64,
) -> Result<Frame, Error> {
    let stored = entries
        .iter()
        .map(|entry| {
            (
                entry.member.clone(),
                if store.dist {
                    entry.distance_m / unit_factor
                } else {
                    entry.score as f64
                },
            )
        })
        .collect::<Vec<_>>();
    db.zset_store_entries_async(&store.dest, stored)
        .await
        .map(|n| Frame::Integer(n as i64))
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

#[cfg(test)]
mod tests {
    use super::{
        decode_score, distance_m, encode_score, parse_f, parse_non_negative_f, redis_geohash,
        unit_factor, validate_coord,
    };
    use crate::command::Command;
    use crate::frame::Frame;
    use crate::store::db::Db;
    use crate::store::kv_store::KvStore;
    use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
    use std::sync::Arc;

    fn test_db() -> Db {
        let unique = format!(
            "onedis-geo-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"))
            .join(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    fn frame(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn apply(db: &Db, args: &[&str]) -> Frame {
        let command = Command::parse_from_frame(frame(args)).unwrap();
        db.handle_command(command).unwrap()
    }

    async fn apply_async(db: &Db, args: &[&str]) -> Frame {
        let command = Command::parse_from_frame(frame(args)).unwrap();
        db.handle_command_async(command).await.unwrap()
    }

    fn parse_err(args: &[&str]) -> String {
        match Command::parse_from_frame(frame(args)) {
            Ok(command) => panic!("expected parse error, got {}", command.name()),
            Err(error) => error.to_string(),
        }
    }

    fn array(frame: Frame) -> Vec<Frame> {
        match frame {
            Frame::Array(values) => values,
            other => panic!("expected array, got {}", other.to_string()),
        }
    }

    #[tokio::test]
    async fn geo_commands_cover_sync_async_store_and_legacy_shapes() {
        let db = test_db();

        assert!(matches!(
            apply(
                &db,
                &[
                    "geoadd",
                    "places",
                    "13.361389",
                    "38.115556",
                    "palermo",
                    "15.087269",
                    "37.502669",
                    "catania",
                    "12.496366",
                    "41.902782",
                    "rome",
                ],
            ),
            Frame::Integer(3)
        ));
        assert!(matches!(
            apply(&db, &["geoadd", "places", "NX", "13.0", "38.0", "palermo"]),
            Frame::Integer(0)
        ));
        assert!(matches!(
            apply(
                &db,
                &["geoadd", "places", "XX", "CH", "13.5", "38.2", "palermo"]
            ),
            Frame::Integer(1)
        ));
        assert!(matches!(
            apply(&db, &["geoadd", "places", "XX", "1", "1", "missing"]),
            Frame::Integer(0)
        ));

        let positions = array(apply(
            &db,
            &["geopos", "places", "palermo", "missing", "catania"],
        ));
        assert!(matches!(positions[0], Frame::Array(_)));
        assert!(matches!(positions[1], Frame::Null));
        assert!(matches!(positions[2], Frame::Array(_)));

        assert!(matches!(
            apply(&db, &["geodist", "places", "palermo", "catania", "km"]),
            Frame::BulkString(value) if !value.is_empty()
        ));
        assert!(matches!(
            apply(&db, &["geodist", "places", "palermo", "missing"]),
            Frame::Null
        ));
        assert!(matches!(
            apply(&db, &["geohash", "places", "palermo", "missing"]),
            Frame::Array(values) if matches!(values.first(), Some(Frame::BulkString(hash)) if hash.len() == 11)
                && matches!(values.get(1), Some(Frame::Null))
        ));

        let rich = array(apply(
            &db,
            &[
                "geosearch",
                "places",
                "fromlonlat",
                "15",
                "37",
                "byradius",
                "200",
                "km",
                "withdist",
                "withhash",
                "withcoord",
                "asc",
                "count",
                "2",
                "any",
            ],
        ));
        assert_eq!(rich.len(), 2);
        assert!(matches!(rich[0], Frame::Array(_)));

        let box_result = array(apply(
            &db,
            &[
                "geosearch",
                "places",
                "frommember",
                "palermo",
                "bybox",
                "400",
                "400",
                "km",
                "desc",
            ],
        ));
        assert!(!box_result.is_empty());

        assert!(matches!(
            apply(
                &db,
                &[
                    "georadius",
                    "places",
                    "15",
                    "37",
                    "200",
                    "km",
                    "store",
                    "stored",
                ],
            ),
            Frame::Integer(n) if n > 0
        ));
        assert!(matches!(apply(&db, &["zcard", "stored"]), Frame::Integer(n) if n > 0));
        assert!(matches!(
            apply(
                &db,
                &[
                    "georadiusbymember",
                    "places",
                    "palermo",
                    "200",
                    "km",
                    "storedist",
                    "distances",
                ],
            ),
            Frame::Integer(n) if n > 0
        ));
        assert!(matches!(
            apply(
                &db,
                &[
                    "geosearchstore",
                    "copy",
                    "places",
                    "frommember",
                    "palermo",
                    "byradius",
                    "500",
                    "km",
                    "storedist",
                ],
            ),
            Frame::Integer(n) if n > 0
        ));

        assert!(matches!(
            apply_async(
                &db,
                &[
                    "geosearch",
                    "places",
                    "fromlonlat",
                    "15",
                    "37",
                    "byradius",
                    "300",
                    "km",
                    "desc",
                    "count",
                    "1",
                ],
            )
            .await,
            Frame::Array(values) if values.len() == 1
        ));
        assert!(matches!(
            apply_async(&db, &["geodist", "places", "palermo", "catania", "mi"]).await,
            Frame::BulkString(_)
        ));
        assert!(matches!(
            apply_async(&db, &["geohash", "places", "palermo"]).await,
            Frame::Array(values) if values.len() == 1
        ));
    }

    #[test]
    fn geo_parser_validation_and_math_helpers_cover_error_edges() {
        assert!(parse_err(&["geoadd", "g"]).contains("wrong number"));
        assert!(parse_err(&["geoadd", "g", "NX", "XX", "1", "1", "m"]).contains("not compatible"));
        assert!(parse_err(&["geoadd", "g", "nan", "1", "m"]).contains("valid float"));
        assert!(parse_err(&["geoadd", "g", "181", "1", "m"]).contains("invalid longitude"));
        assert!(parse_err(&["geopos", "g"]).contains("wrong number"));
        assert!(parse_err(&["geodist", "g", "a"]).contains("wrong number"));
        assert!(parse_err(&["geohash", "g"]).contains("wrong number"));
        assert!(parse_err(&["geosearch", "g"]).contains("syntax"));
        assert!(parse_err(&["geosearch", "g", "frommember", "m"]).contains("syntax"));
        assert!(
            parse_err(&[
                "geosearch",
                "g",
                "fromlonlat",
                "0",
                "0",
                "byradius",
                "-1",
                "m"
            ])
            .contains("out of range")
        );
        assert!(
            parse_err(&[
                "geosearch",
                "g",
                "fromlonlat",
                "0",
                "0",
                "byradius",
                "1",
                "bad"
            ])
            .contains("unsupported unit")
        );
        assert!(
            parse_err(&[
                "geosearch",
                "g",
                "fromlonlat",
                "0",
                "0",
                "bybox",
                "1",
                "2",
                "m",
                "bad"
            ])
            .contains("syntax")
        );
        assert!(
            parse_err(&[
                "geosearch",
                "g",
                "fromlonlat",
                "0",
                "0",
                "byradius",
                "1",
                "m",
                "count",
                "x"
            ])
            .contains("integer")
        );
        assert!(parse_err(&["georadius", "g", "0"]).contains("wrong number"));
        assert!(parse_err(&["georadiusbymember", "g", "m"]).contains("wrong number"));
        assert!(parse_err(&["geosearchstore", "dst", "g"]).contains("syntax"));

        assert!(parse_f("1.25").unwrap() == 1.25);
        assert!(parse_f("inf").is_err());
        assert!(parse_non_negative_f("-0.1").is_err());
        assert!(unit_factor("m").unwrap() == 1.0);
        assert!(unit_factor("km").unwrap() == 1000.0);
        assert!(unit_factor("ft").unwrap() < 1.0);
        assert!(unit_factor("bad").is_err());
        assert!(validate_coord(-180.0, -85.05112878).is_ok());
        assert!(validate_coord(180.0, 85.05112878).is_ok());

        let score = encode_score(13.361389, 38.115556);
        let decoded = decode_score(score);
        assert!(distance_m((13.361389, 38.115556), decoded) < 1.0);
        assert_eq!(redis_geohash(13.361389, 38.115556).len(), 11);
    }

    #[test]
    fn geo_search_missing_center_member_returns_resp_error_frame() {
        let db = test_db();
        let frame = apply(
            &db,
            &[
                "geosearch",
                "places",
                "frommember",
                "missing",
                "byradius",
                "10",
                "km",
            ],
        );
        assert!(matches!(frame, Frame::Error(message) if message.contains("could not decode")));
    }
}
