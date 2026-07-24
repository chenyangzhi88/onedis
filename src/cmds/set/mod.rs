pub mod sadd;
pub mod scard;
pub mod sdiff;
pub mod sdiffstore;
pub mod sinter;
pub mod sintercard;
pub mod sinterstore;
pub mod sismember;
pub mod smembers;
pub mod smismember;
pub mod smove;
pub mod spop;
pub mod srandmember;
pub mod srem;
pub mod sscan;
pub mod sunion;
pub mod sunionstore;

use anyhow::Error;

use crate::frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES};

pub(crate) fn set_array(values: impl IntoIterator<Item = String>) -> Result<Frame, Error> {
    let mut frames = Vec::new();
    let mut encoded_bytes = 32usize;
    for value in values {
        if frames.len() >= MAX_ARRAY_ELEMENTS {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        encoded_bytes = encoded_bytes
            .checked_add(value.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(value));
    }
    Ok(Frame::Array(frames))
}

pub(crate) fn validate_count(count: u64) -> Result<(), Error> {
    if count > MAX_ARRAY_ELEMENTS as u64 {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
