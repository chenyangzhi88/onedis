use anyhow::Error;

use crate::{
    cmds::sorted_set::common::{parse_numkeys_command, parse_weights_and_aggregate, text_arg},
    frame::Frame,
    store::db::Db,
};

pub struct Zunionstore {
    destination: String,
    keys: Vec<String>,
    weights: Vec<f64>,
    aggregate: crate::store::db::ZsetAggregate,
}

impl Zunionstore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zunionstore' command",
            ));
        }
        let shifted_args = (2..frame.arg_len())
            .map(|idx| text_arg(&frame, idx).map(Frame::bulk_string))
            .collect::<Result<Vec<_>, _>>()?;
        let shifted = Frame::Array(
            std::iter::once(Frame::bulk_string("zunion"))
                .chain(shifted_args)
                .collect(),
        );
        let keys = parse_numkeys_command(&shifted, "zunionstore")?;
        let (weights, aggregate, _) =
            parse_weights_and_aggregate(&shifted, 2 + keys.len(), keys.len())?;
        Ok(Self {
            destination: text_arg(&frame, 1)?,
            keys,
            weights,
            aggregate,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_union_or_inter(&self.keys, &self.weights, self.aggregate, false)
            .and_then(|entries| db.zset_store_entries(&self.destination, entries))
        {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_union_or_inter(&self.keys, &self.weights, self.aggregate, false) {
            Ok(entries) => match db
                .zset_store_entries_async(&self.destination, entries)
                .await
            {
                Ok(len) => Ok(Frame::Integer(len as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
