use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Lpos {
    key: String,
    element: String,
    rank: i64,
    count: Option<usize>,
    maxlen: Option<usize>,
}

impl Lpos {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lpos' command",
            ));
        }

        let key = frame.get_arg(1).unwrap();
        let element = frame.get_arg(2).unwrap();
        let mut rank = 1;
        let mut count = None;
        let mut maxlen = None;
        let mut idx = 3;
        while idx < frame.arg_len() {
            let option = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .to_ascii_uppercase();
            match option.as_str() {
                "RANK" if idx + 1 < frame.arg_len() => {
                    rank = frame
                        .get_arg(idx + 1)
                        .unwrap()
                        .parse::<i64>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    if rank == 0 {
                        return Err(Error::msg("ERR RANK can't be zero"));
                    }
                    idx += 2;
                }
                "COUNT" if idx + 1 < frame.arg_len() => {
                    count = Some(
                        frame
                            .get_arg(idx + 1)
                            .unwrap()
                            .parse::<usize>()
                            .map_err(|_| {
                                Error::msg("ERR value is not an integer or out of range")
                            })?,
                    );
                    idx += 2;
                }
                "MAXLEN" if idx + 1 < frame.arg_len() => {
                    maxlen = Some(
                        frame
                            .get_arg(idx + 1)
                            .unwrap()
                            .parse::<usize>()
                            .map_err(|_| {
                                Error::msg("ERR value is not an integer or out of range")
                            })?,
                    );
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }

        Ok(Self {
            key,
            element,
            rank,
            count,
            maxlen,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_positions(&self.key, &self.element, self.rank, self.count, self.maxlen) {
            Ok(positions) if self.count.is_some() => Ok(Frame::Array(
                positions
                    .into_iter()
                    .map(|position| Frame::Integer(position as i64))
                    .collect(),
            )),
            Ok(mut positions) => match positions.pop() {
                Some(position) => Ok(Frame::Integer(position as i64)),
                None => Ok(Frame::Null),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_positions_async(&self.key, &self.element, self.rank, self.count, self.maxlen)
            .await
        {
            Ok(positions) if self.count.is_some() => Ok(Frame::Array(
                positions
                    .into_iter()
                    .map(|position| Frame::Integer(position as i64))
                    .collect(),
            )),
            Ok(mut positions) => match positions.pop() {
                Some(position) => Ok(Frame::Integer(position as i64)),
                None => Ok(Frame::Null),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
