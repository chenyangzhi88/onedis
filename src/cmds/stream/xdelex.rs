use anyhow::Error;

use crate::{cmds::stream::xackdel::parse_ids, frame::Frame, store::db::Db};

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
        if idx < args.len()
            && matches!(
                args[idx].to_ascii_uppercase().as_str(),
                "DELREF" | "KEEPREF" | "ACKED"
            )
        {
            idx += 1;
        }
        Ok(Self {
            key: args[1].clone(),
            ids: parse_ids(&args, idx)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_delete(&self.key, &self.ids) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_delete_async(&self.key, &self.ids).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
