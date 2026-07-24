pub mod bitcount;
pub mod bitfield;
pub mod bitop;
pub mod bitpos;
pub mod getbit;
pub mod setbit;

use anyhow::Error;

use crate::frame::Frame;

pub(crate) fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR command arguments must be valid UTF-8"))
}
