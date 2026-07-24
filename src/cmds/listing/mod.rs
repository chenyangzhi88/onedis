pub mod blmove;
pub mod blmpop;
pub mod blpop;
pub mod brpop;
pub mod brpoplpush;
pub mod lindex;
pub mod linsert;
pub mod llen;
pub mod lmove;
pub mod lmpop;
pub mod lpop;
pub mod lpos;
pub mod lpush;
pub mod lpushx;
pub mod lrange;
pub mod lrem;
pub mod lset;
pub mod ltrim;
pub mod rpop;
pub mod rpoplpush;
pub mod rpush;
pub mod rpushx;

use anyhow::Error;

use crate::frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES};

pub(crate) fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR invalid UTF-8 argument"))
}

pub(crate) fn validate_response_count(count: usize) -> Result<(), Error> {
    if count > MAX_ARRAY_ELEMENTS {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

pub(crate) fn list_array(values: Vec<String>) -> Result<Frame, Error> {
    validate_response_count(values.len())?;
    let mut encoded_bytes = 32usize;
    let mut frames = Vec::with_capacity(values.len());
    for value in values {
        encoded_bytes = encoded_bytes
            .checked_add(value.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(value));
    }
    Ok(Frame::Array(frames))
}

#[cfg(test)]
mod tests;
