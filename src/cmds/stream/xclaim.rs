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
    just_id: bool,
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
        let mut just_id = false;
        let mut idx = 5;
        while idx < frame.arg_len() {
            let arg = frame.get_arg(idx).unwrap();
            if arg.eq_ignore_ascii_case("JUSTID") && !just_id {
                just_id = true;
                idx += 1;
                continue;
            }
            if arg.eq_ignore_ascii_case("IDLE")
                || arg.eq_ignore_ascii_case("TIME")
                || arg.eq_ignore_ascii_case("RETRYCOUNT")
                || arg.eq_ignore_ascii_case("FORCE")
                || arg.eq_ignore_ascii_case("LASTID")
            {
                return Err(Error::msg("ERR unsupported XCLAIM option"));
            }
            ids.push(StreamId::parse(&arg).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })?);
            idx += 1;
        }
        if ids.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xclaim' command",
            ));
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            group: frame.get_arg(2).unwrap(),
            consumer: frame.get_arg(3).unwrap(),
            min_idle_ms,
            ids,
            just_id,
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
            Ok(entries) => Ok(claimed_entries_frame(entries, self.just_id)),
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
            Ok(entries) => Ok(claimed_entries_frame(entries, self.just_id)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn claimed_entries_frame(entries: Vec<crate::store::db::StreamEntry>, just_id: bool) -> Frame {
    if just_id {
        Frame::Array(
            entries
                .into_iter()
                .map(|entry| Frame::bulk_string(entry.id))
                .collect(),
        )
    } else {
        Frame::Array(entries.into_iter().map(stream_entry_frame).collect())
    }
}
