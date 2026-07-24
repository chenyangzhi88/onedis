use anyhow::Error;

use crate::{
    cmds::stream::xackdel::{parse_ids, status_frame},
    frame::Frame,
    store::db::Db,
};

pub struct Xdelex {
    key: String,
    ids: Vec<crate::store::db::StreamId>,
}

impl Xdelex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xdelex' command",
            ));
        }
        let mut idx = 2;
        if idx < args.len() {
            match args[idx].to_ascii_uppercase().as_str() {
                "KEEPREF" => idx += 1,
                "DELREF" | "ACKED" => {
                    return Err(Error::msg("ERR unsupported stream reference policy"));
                }
                _ => {}
            }
        }
        Ok(Self {
            key: args[1].clone(),
            ids: parse_ids(&args, idx)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_delete_with_statuses(&self.key, &self.ids) {
            Ok(statuses) => Ok(status_frame(statuses)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_delete_with_statuses_async(&self.key, &self.ids)
            .await
        {
            Ok(statuses) => Ok(status_frame(statuses)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
