use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, ZsetAddOptions, ZsetAddOutcome},
};

pub struct Zadd {
    pub key: String,
    pub members: Vec<(f64, String)>,
    options: ZsetAddOptions,
    ch: bool,
}

impl Zadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 {
            return Err(wrong_arity());
        }

        let mut options = ZsetAddOptions::default();
        let mut ch = false;
        let mut idx = 2;
        while idx < args.len() {
            let matched = match args[idx].to_ascii_uppercase().as_str() {
                "NX" if !options.nx => {
                    options.nx = true;
                    true
                }
                "XX" if !options.xx => {
                    options.xx = true;
                    true
                }
                "GT" if !options.gt => {
                    options.gt = true;
                    true
                }
                "LT" if !options.lt => {
                    options.lt = true;
                    true
                }
                "CH" if !ch => {
                    ch = true;
                    true
                }
                "INCR" if !options.increment => {
                    options.increment = true;
                    true
                }
                _ => false,
            };
            if !matched {
                break;
            }
            idx += 1;
        }

        if options.nx && (options.xx || options.gt || options.lt) || options.gt && options.lt {
            return Err(Error::msg("ERR syntax error"));
        }
        let value_count = args.len() - idx;
        if value_count == 0 || !value_count.is_multiple_of(2) {
            return Err(wrong_arity());
        }
        if options.increment && value_count != 2 {
            return Err(Error::msg(
                "ERR INCR option supports a single increment-element pair",
            ));
        }

        let mut members = Vec::with_capacity(value_count / 2);
        for pair in args[idx..].chunks_exact(2) {
            let score = pair[0]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR score is not a valid float"))?;
            if score.is_nan() {
                return Err(Error::msg("ERR score is not a valid float"));
            }
            members.push((score, pair[1].clone()));
        }

        Ok(Self {
            key: args[1].clone(),
            members,
            options,
            ch,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_add_with_options(&self.key, &self.members, self.options) {
            Ok(outcome) => Ok(self.response(outcome)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_add_with_options_async(&self.key, &self.members, self.options)
            .await
        {
            Ok(outcome) => Ok(self.response(outcome)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    fn response(&self, outcome: ZsetAddOutcome) -> Frame {
        if self.options.increment {
            if outcome.applied {
                Frame::bulk_string(outcome.score.unwrap().to_string())
            } else {
                Frame::Null
            }
        } else if self.ch {
            Frame::Integer(outcome.changed as i64)
        } else {
            Frame::Integer(outcome.added as i64)
        }
    }
}

fn wrong_arity() -> Error {
    Error::msg("ERR wrong number of arguments for 'zadd' command")
}
