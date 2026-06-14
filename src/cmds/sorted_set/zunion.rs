use anyhow::Error;

use crate::{
    cmds::sorted_set::common::{
        entries_with_scores, parse_numkeys_command, parse_weights_and_aggregate,
    },
    frame::Frame,
    store::db::Db,
};

pub struct Zunion {
    keys: Vec<String>,
    weights: Vec<f64>,
    aggregate: crate::store::db::ZsetAggregate,
    withscores: bool,
}

impl Zunion {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = parse_numkeys_command(&frame, "zunion")?;
        let (weights, aggregate, withscores) =
            parse_weights_and_aggregate(&frame, 2 + keys.len(), keys.len())?;
        Ok(Self {
            keys,
            weights,
            aggregate,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_union_or_inter(&self.keys, &self.weights, self.aggregate, false) {
            Ok(entries) if self.withscores => Ok(Frame::Array(entries_with_scores(entries))),
            Ok(entries) => Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|(m, _)| Frame::bulk_string(m))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_union_or_inter_async(&self.keys, &self.weights, self.aggregate, false)
            .await
        {
            Ok(entries) if self.withscores => Ok(Frame::Array(entries_with_scores(entries))),
            Ok(entries) => Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|(m, _)| Frame::bulk_string(m))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
