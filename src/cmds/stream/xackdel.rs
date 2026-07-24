use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xackdel {
    key: String,
    group: String,
    ids: Vec<StreamId>,
}

impl Xackdel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xackdel' command",
            ));
        }
        let mut idx = 3;
        if idx < args.len() {
            match args[idx].to_ascii_uppercase().as_str() {
                "KEEPREF" => idx += 1,
                "DELREF" | "ACKED" => {
                    return Err(Error::msg("ERR unsupported stream reference policy"));
                }
                _ => {}
            }
        }
        let ids = parse_ids(&args, idx)?;
        Ok(Self {
            key: args[1].clone(),
            group: args[2].clone(),
            ids,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_ack_delete(&self.key, &self.group, &self.ids) {
            Ok(statuses) => Ok(status_frame(statuses)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_ack_delete_async(&self.key, &self.group, &self.ids)
            .await
        {
            Ok(statuses) => Ok(status_frame(statuses)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_ids(args: &[String], start: usize) -> Result<Vec<StreamId>, Error> {
    if start >= args.len() || !args[start].eq_ignore_ascii_case("IDS") {
        return Err(Error::msg("ERR syntax error"));
    }
    let count = args
        .get(start + 1)
        .ok_or_else(|| Error::msg("ERR syntax error"))?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
    if count == 0 {
        return Err(Error::msg("ERR numids should be greater than 0"));
    }
    let ids_start = start + 2;
    let ids_end = ids_start
        .checked_add(count)
        .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
    if args.len() != ids_end {
        return Err(Error::msg("ERR syntax error"));
    }
    let ids = &args[ids_start..];
    ids.iter()
        .map(|id| {
            StreamId::parse(id).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })
        })
        .collect()
}

pub(crate) fn status_frame(statuses: Vec<i64>) -> Frame {
    Frame::Array(statuses.into_iter().map(Frame::Integer).collect())
}
