use anyhow::Error;

use crate::{
    cmds::stream::{stream_claimed_frame, text_arg, validate_count},
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xautoclaim {
    key: String,
    group: String,
    consumer: String,
    min_idle_ms: u64,
    start: StreamId,
    count: usize,
    just_id: bool,
}

impl Xautoclaim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xautoclaim' command",
            ));
        }
        let mut count = 100usize;
        let mut count_seen = false;
        let mut just_id = false;
        let mut idx = 6;
        while idx < frame.arg_len() {
            match text_arg(&frame, idx)?.to_ascii_uppercase().as_str() {
                "COUNT" if !count_seen && idx + 1 < frame.arg_len() => {
                    count = text_arg(&frame, idx + 1)?
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    if count == 0 {
                        return Err(Error::msg("ERR COUNT must be > 0"));
                    }
                    validate_count(count)?;
                    count_seen = true;
                    idx += 2;
                }
                "JUSTID" if !just_id => {
                    just_id = true;
                    idx += 1;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key: text_arg(&frame, 1)?,
            group: text_arg(&frame, 2)?,
            consumer: text_arg(&frame, 3)?,
            min_idle_ms: text_arg(&frame, 4)?
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            start: StreamId::parse(&text_arg(&frame, 5)?).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })?,
            count,
            just_id,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_auto_claim(
            &self.key,
            &self.group,
            &self.consumer,
            self.min_idle_ms,
            self.start,
            self.count,
        ) {
            Ok(claimed) => claimed_frame(claimed, self.just_id),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_auto_claim_async(
                &self.key,
                &self.group,
                &self.consumer,
                self.min_idle_ms,
                self.start,
                self.count,
            )
            .await
        {
            Ok(claimed) => claimed_frame(claimed, self.just_id),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn claimed_frame(
    claimed: crate::store::db::StreamClaimedEntries,
    just_id: bool,
) -> Result<Frame, Error> {
    stream_claimed_frame(claimed.next_id, claimed.entries, just_id)
}
