pub mod append;
pub mod decr;
pub mod decrby;
pub mod get;
pub mod getdel;
pub mod getex;
pub mod getrange;
pub mod getset;
pub mod incr;
pub mod incrby;
pub mod incrbyfloat;
pub mod lcs;
pub mod mget;
pub mod mset;
pub mod msetex;
pub mod msetnx;
pub mod psetex;
pub mod set;
pub mod setex;
pub mod setnx;
pub mod setrange;
pub mod strlen;

use anyhow::Error;

use crate::frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES};

pub(crate) fn checked_string_values(
    values: impl IntoIterator<Item = Option<Vec<u8>>>,
) -> Result<Frame, Error> {
    let mut frames = Vec::new();
    let mut encoded_bytes = 32usize;
    for value in values {
        if frames.len() >= MAX_ARRAY_ELEMENTS {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        encoded_bytes = encoded_bytes
            .checked_add(
                value
                    .as_ref()
                    .map_or(5, |value| value.len().saturating_add(32)),
            )
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(value.map_or(Frame::Null, Frame::bulk_string));
    }
    Ok(Frame::Array(frames))
}
