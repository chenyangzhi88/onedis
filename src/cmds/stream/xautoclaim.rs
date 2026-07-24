use anyhow::Error;

use crate::{
    cmds::stream::stream_entry_frame,
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
            match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
                "COUNT" if !count_seen && idx + 1 < frame.arg_len() => {
                    count = frame
                        .get_arg(idx + 1)
                        .unwrap()
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    if count == 0 {
                        return Err(Error::msg("ERR COUNT must be > 0"));
                    }
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
            key: frame.get_arg(1).unwrap(),
            group: frame.get_arg(2).unwrap(),
            consumer: frame.get_arg(3).unwrap(),
            min_idle_ms: frame
                .get_arg(4)
                .unwrap()
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            start: StreamId::parse(&frame.get_arg(5).unwrap()).ok_or_else(|| {
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
            Ok(claimed) => Ok(claimed_frame(claimed, self.just_id)),
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
            Ok(claimed) => Ok(claimed_frame(claimed, self.just_id)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn claimed_frame(claimed: crate::store::db::StreamClaimedEntries, just_id: bool) -> Frame {
    let entries = if just_id {
        claimed
            .entries
            .into_iter()
            .map(|entry| Frame::bulk_string(entry.id))
            .collect()
    } else {
        claimed
            .entries
            .into_iter()
            .map(stream_entry_frame)
            .collect()
    };
    Frame::Array(vec![
        Frame::bulk_string(claimed.next_id),
        Frame::Array(entries),
        Frame::Array(Vec::new()),
    ])
}
