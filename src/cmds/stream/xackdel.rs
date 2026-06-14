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
        if idx < args.len()
            && matches!(
                args[idx].to_ascii_uppercase().as_str(),
                "DELREF" | "KEEPREF" | "ACKED"
            )
        {
            idx += 1;
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
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_ack_delete_async(&self.key, &self.group, &self.ids)
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_ids(args: &[String], start: usize) -> Result<Vec<StreamId>, Error> {
    if start >= args.len() {
        return Err(Error::msg("ERR syntax error"));
    }
    let ids = if args[start].eq_ignore_ascii_case("IDS") {
        let count = args
            .get(start + 1)
            .ok_or_else(|| Error::msg("ERR syntax error"))?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let ids_start = start + 2;
        if args.len() != ids_start + count {
            return Err(Error::msg("ERR syntax error"));
        }
        &args[ids_start..]
    } else {
        &args[start..]
    };
    ids.iter()
        .map(|id| {
            StreamId::parse(id).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })
        })
        .collect()
}
