use anyhow::Error;

use crate::{
    cmds::stream::stream_entry_frame,
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xclaim {
    key: String,
    group: String,
    consumer: String,
    min_idle_ms: u64,
    ids: Vec<StreamId>,
}

impl Xclaim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xclaim' command",
            ));
        }
        let min_idle_ms = frame
            .get_arg(4)
            .unwrap()
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let mut ids = Vec::new();
        let mut idx = 5;
        while idx < frame.arg_len() {
            let arg = frame.get_arg(idx).unwrap();
            if arg.eq_ignore_ascii_case("IDLE")
                || arg.eq_ignore_ascii_case("TIME")
                || arg.eq_ignore_ascii_case("RETRYCOUNT")
            {
                idx += 2;
                continue;
            }
            if arg.eq_ignore_ascii_case("FORCE")
                || arg.eq_ignore_ascii_case("JUSTID")
                || arg.eq_ignore_ascii_case("LASTID")
            {
                idx += 1;
                continue;
            }
            ids.push(StreamId::parse(&arg).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })?);
            idx += 1;
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            group: frame.get_arg(2).unwrap(),
            consumer: frame.get_arg(3).unwrap(),
            min_idle_ms,
            ids,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_claim(
            &self.key,
            &self.group,
            &self.consumer,
            self.min_idle_ms,
            &self.ids,
        ) {
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_claim_async(
                &self.key,
                &self.group,
                &self.consumer,
                self.min_idle_ms,
                &self.ids,
            )
            .await
        {
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
