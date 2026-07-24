pub mod pfadd;
pub mod pfcount;
pub mod pfmerge;

use anyhow::Error;

use crate::frame::Frame;

pub(crate) fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR command arguments must be valid UTF-8"))
}

pub(crate) fn binary_arg(frame: &Frame, index: usize) -> Result<Vec<u8>, Error> {
    frame
        .get_arg_bytes(index)
        .ok_or_else(|| Error::msg("ERR command argument is not a bulk string"))
}
