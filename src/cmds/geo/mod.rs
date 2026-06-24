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

include!("types.rs");
include!("commands.rs");
include!("search_parser.rs");
include!("search_runtime.rs");
include!("geo_codec.rs");

#[cfg(test)]
mod tests;
