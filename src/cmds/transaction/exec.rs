use crate::frame::Frame;
use anyhow::Error;

pub struct Exec;

impl Exec {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'exec' command",
            ));
        }
        Ok(Exec)
    }

    pub fn apply(&self) -> Result<Frame, Error> {
        // EXEC命令本身不执行任何操作，只是标记需要执行事务
        // 实际的执行将在Handler中处理
        Ok(Frame::Ok)
    }
}
