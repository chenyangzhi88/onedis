use anyhow::Error;

use crate::{
    cmds::sorted_set::zrange::{ScoreBound, parse_score_bound},
    frame::Frame,
    store::db::Db,
};

pub struct Zcount {
    key: String,
    min: ScoreBound,
    max: ScoreBound,
}

impl Zcount {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zcount' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let min = parse_score_bound(&args[2])?;
        let max = parse_score_bound(&args[3])?;
        Ok(Zcount { key, min, max })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_count_bounded(
            &self.key,
            self.min.value,
            self.min.inclusive,
            self.max.value,
            self.max.inclusive,
        ) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_count_bounded_async(
                &self.key,
                self.min.value,
                self.min.inclusive,
                self.max.value,
                self.max.inclusive,
            )
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
