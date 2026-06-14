use crate::frame::Frame;
use anyhow::Error;

pub struct Flushall {}

impl Flushall {
    pub fn parse_from_frame(_frame: Frame) -> Result<Self, Error> {
        Ok(Flushall {})
    }
}
