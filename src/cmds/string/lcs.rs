use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Lcs {
    key1: String,
    key2: String,
    len_only: bool,
    idx: bool,
}

impl Lcs {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lcs' command",
            ));
        }

        let mut len_only = false;
        let mut idx = false;
        let mut idx_arg = 3;
        while idx_arg < args.len() {
            match args[idx_arg].to_ascii_uppercase().as_str() {
                "LEN" => len_only = true,
                "IDX" => idx = true,
                "MINMATCHLEN" => {
                    idx_arg += 1;
                    if idx_arg >= args.len() || args[idx_arg].parse::<usize>().is_err() {
                        return Err(Error::msg("ERR value is not an integer or out of range"));
                    }
                }
                "WITHMATCHLEN" => {}
                _ => return Err(Error::msg("ERR syntax error")),
            }
            idx_arg += 1;
        }

        Ok(Self {
            key1: args[1].clone(),
            key2: args[2].clone(),
            len_only,
            idx,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let left = db.get_string_bytes(&self.key1)?.unwrap_or_default();
        let right = db.get_string_bytes(&self.key2)?.unwrap_or_default();
        let value = lcs_bytes(&left, &right);
        if self.len_only {
            return Ok(Frame::Integer(value.len() as i64));
        }
        if self.idx {
            return Ok(Frame::Array(vec![
                Frame::bulk_string("matches"),
                Frame::Array(Vec::new()),
                Frame::bulk_string("len"),
                Frame::Integer(value.len() as i64),
            ]));
        }
        Ok(Frame::bulk_string(value))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let left = db
            .get_string_bytes_async(&self.key1)
            .await?
            .unwrap_or_default();
        let right = db
            .get_string_bytes_async(&self.key2)
            .await?
            .unwrap_or_default();
        let value = lcs_bytes(&left, &right);
        if self.len_only {
            return Ok(Frame::Integer(value.len() as i64));
        }
        if self.idx {
            return Ok(Frame::Array(vec![
                Frame::bulk_string("matches"),
                Frame::Array(Vec::new()),
                Frame::bulk_string("len"),
                Frame::Integer(value.len() as i64),
            ]));
        }
        Ok(Frame::bulk_string(value))
    }
}

fn lcs_bytes(left: &[u8], right: &[u8]) -> Vec<u8> {
    let rows = left.len();
    let cols = right.len();
    let mut dp = vec![vec![0usize; cols + 1]; rows + 1];
    for i in (0..rows).rev() {
        for j in (0..cols).rev() {
            dp[i][j] = if left[i] == right[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }

    let mut i = 0;
    let mut j = 0;
    let mut out = Vec::with_capacity(dp[0][0]);
    while i < rows && j < cols {
        if left[i] == right[j] {
            out.push(left[i]);
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}
