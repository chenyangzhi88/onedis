use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES, MAX_FRAME_NODES},
    store::db::{Db, ZsetAddOptions},
};
use anyhow::Error;

const GEO_STEP: usize = 26;
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const EARTH_RADIUS_M: f64 = 6372797.560856;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR command arguments must be valid UTF-8"))
}

fn bounded_geo_frame(frame: Frame) -> Result<Frame, Error> {
    fn charge(frame: &Frame, nodes: &mut usize, bytes: &mut usize) -> Result<(), Error> {
        *nodes = nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= MAX_FRAME_NODES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        let payload = match frame {
            Frame::BulkString(value) => value.len(),
            Frame::SimpleString(value) | Frame::Error(value) => value.len(),
            Frame::Integer(value) => value.to_string().len(),
            Frame::Ok => 2,
            Frame::Boolean(_) => 1,
            Frame::Double(value) => value.to_string().len(),
            Frame::BigNumber(value) => value.len(),
            Frame::BlobError(value) => value.len(),
            Frame::VerbatimString { data, .. } => data.len(),
            Frame::Null
            | Frame::Array(_)
            | Frame::Map(_)
            | Frame::Set(_)
            | Frame::Attribute { .. }
            | Frame::Push(_) => 0,
        };
        *bytes = bytes
            .checked_add(payload.saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        if let Frame::Array(values) = frame {
            if values.len() > MAX_ARRAY_ELEMENTS {
                return Err(Error::msg("ERR response exceeds configured limit"));
            }
            for value in values {
                charge(value, nodes, bytes)?;
            }
        }
        let nested: Vec<&Frame> = match frame {
            Frame::Set(values) | Frame::Push(values) => values.iter().collect(),
            Frame::Map(entries) => entries
                .iter()
                .flat_map(|(key, value)| [key, value])
                .collect(),
            Frame::Attribute { attributes, data } => attributes
                .iter()
                .flat_map(|(key, value)| [key, value])
                .chain(std::iter::once(data.as_ref()))
                .collect(),
            _ => Vec::new(),
        };
        for value in nested {
            charge(value, nodes, bytes)?;
        }
        Ok(())
    }

    let mut nodes = 0;
    let mut bytes = 0;
    charge(&frame, &mut nodes, &mut bytes)?;
    Ok(frame)
}

fn max_geo_result_count(options: &SearchOptions) -> usize {
    let nodes_per_result = if options.withdist || options.withhash || options.withcoord {
        2 + usize::from(options.withdist)
            + usize::from(options.withhash)
            + if options.withcoord { 3 } else { 0 }
    } else {
        1
    };
    ((MAX_FRAME_NODES - 1) / nodes_per_result).min(MAX_ARRAY_ELEMENTS)
}

include!("types.rs");
include!("commands.rs");
include!("search_parser.rs");
include!("search_runtime.rs");
include!("geo_codec.rs");

#[cfg(test)]
mod tests;
