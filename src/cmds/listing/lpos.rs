use anyhow::Error;

use crate::{
    cmds::listing::{text_arg, validate_response_count},
    frame::{Frame, MAX_ARRAY_ELEMENTS},
    store::db::Db,
};

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

        let key = text_arg(&frame, 1)?;
        let element = text_arg(&frame, 2)?;
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
                    rank = text_arg(&frame, idx + 1)?
                        .parse::<i64>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    if rank == 0 {
                        return Err(Error::msg("ERR RANK can't be zero"));
                    }
                    idx += 2;
                }
                "COUNT" if idx + 1 < frame.arg_len() => {
                    count =
                        Some(text_arg(&frame, idx + 1)?.parse::<usize>().map_err(|_| {
                            Error::msg("ERR value is not an integer or out of range")
                        })?);
                    if count.is_some_and(|count| count > MAX_ARRAY_ELEMENTS) {
                        return Err(Error::msg("ERR count exceeds configured response limit"));
                    }
                    idx += 2;
                }
                "MAXLEN" if idx + 1 < frame.arg_len() => {
                    maxlen =
                        Some(text_arg(&frame, idx + 1)?.parse::<usize>().map_err(|_| {
                            Error::msg("ERR value is not an integer or out of range")
                        })?);
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
        let requested_count = self.count;
        let bounded_count = requested_count.map(|count| {
            if count == 0 {
                MAX_ARRAY_ELEMENTS.saturating_add(1)
            } else {
                count
            }
        });
        match db.list_positions(
            &self.key,
            &self.element,
            self.rank,
            bounded_count,
            self.maxlen,
        ) {
            Ok(positions) if self.count.is_some() => Ok(Frame::Array({
                validate_response_count(positions.len())?;
                positions
                    .into_iter()
                    .map(|position| Frame::Integer(position as i64))
                    .collect()
            })),
            Ok(mut positions) => match positions.pop() {
                Some(position) => Ok(Frame::Integer(position as i64)),
                None => Ok(Frame::Null),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let requested_count = self.count;
        let bounded_count = requested_count.map(|count| {
            if count == 0 {
                MAX_ARRAY_ELEMENTS.saturating_add(1)
            } else {
                count
            }
        });
        match db
            .list_positions_async(
                &self.key,
                &self.element,
                self.rank,
                bounded_count,
                self.maxlen,
            )
            .await
        {
            Ok(positions) if self.count.is_some() => Ok(Frame::Array({
                validate_response_count(positions.len())?;
                positions
                    .into_iter()
                    .map(|position| Frame::Integer(position as i64))
                    .collect()
            })),
            Ok(mut positions) => match positions.pop() {
                Some(position) => Ok(Frame::Integer(position as i64)),
                None => Ok(Frame::Null),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
