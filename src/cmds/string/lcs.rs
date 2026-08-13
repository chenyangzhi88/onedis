use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

const MAX_LCS_CELLS: usize = 16 * 1024 * 1024;

pub struct Lcs {
    key1: String,
    key2: String,
    len_only: bool,
    idx: bool,
    min_match_len: usize,
    with_match_len: bool,
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
        let mut min_match_len = 0;
        let mut with_match_len = false;
        let mut index = 3;
        while index < args.len() {
            match args[index].to_ascii_uppercase().as_str() {
                "LEN" if !len_only => len_only = true,
                "IDX" if !idx => idx = true,
                "MINMATCHLEN" => {
                    index += 1;
                    min_match_len = args
                        .get(index)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                }
                "WITHMATCHLEN" if !with_match_len => with_match_len = true,
                _ => return Err(Error::msg("ERR syntax error")),
            }
            index += 1;
        }
        if len_only && idx {
            return Err(Error::msg(
                "ERR If you want both the length and indexes, please just use IDX",
            ));
        }

        Ok(Self {
            key1: args[1].clone(),
            key2: args[2].clone(),
            len_only,
            idx,
            min_match_len,
            with_match_len,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let left = db.get_string_bytes(&self.key1)?.unwrap_or_default();
        let right = db.get_string_bytes(&self.key2)?.unwrap_or_default();
        self.response(&left, &right)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut values = db
            .get_string_bytes_many_checked_async(&[self.key1.clone(), self.key2.clone()])
            .await?
            .into_iter();
        let left = values.next().flatten().unwrap_or_default();
        let right = values.next().flatten().unwrap_or_default();
        self.response(&left, &right)
    }

    fn response(self, left: &[u8], right: &[u8]) -> Result<Frame, Error> {
        if self.len_only {
            return Ok(Frame::Integer(lcs_length(left, right)? as i64));
        }
        let result = lcs_result(left, right)?;
        if !self.idx {
            return Ok(Frame::bulk_string(result.value));
        }

        let mut matches = Vec::new();
        for matched in result
            .matches
            .into_iter()
            .rev()
            .filter(|matched| matched.len >= self.min_match_len)
        {
            let mut item = vec![
                index_range_frame(matched.left_start, matched.len),
                index_range_frame(matched.right_start, matched.len),
            ];
            if self.with_match_len {
                item.push(Frame::Integer(matched.len as i64));
            }
            matches.push(Frame::Array(item));
        }
        Ok(Frame::Array(vec![
            Frame::bulk_string("matches"),
            Frame::Array(matches),
            Frame::bulk_string("len"),
            Frame::Integer(result.value.len() as i64),
        ]))
    }
}

struct LcsResult {
    value: Vec<u8>,
    matches: Vec<LcsMatch>,
}

struct LcsMatch {
    left_start: usize,
    right_start: usize,
    len: usize,
}

fn lcs_result(left: &[u8], right: &[u8]) -> Result<LcsResult, Error> {
    let rows = left.len();
    let cols = right.len();
    let stride = cols.checked_add(1).ok_or_else(lcs_limit_error)?;
    let cells = rows
        .checked_add(1)
        .and_then(|rows| rows.checked_mul(stride))
        .filter(|cells| *cells <= MAX_LCS_CELLS)
        .ok_or_else(lcs_limit_error)?;
    let mut dp = Vec::<u32>::new();
    dp.try_reserve_exact(cells).map_err(|_| lcs_limit_error())?;
    dp.resize(cells, 0);
    for i in (0..rows).rev() {
        for j in (0..cols).rev() {
            let offset = i * stride + j;
            dp[offset] = if left[i] == right[j] {
                dp[(i + 1) * stride + j + 1] + 1
            } else {
                dp[(i + 1) * stride + j].max(dp[i * stride + j + 1])
            };
        }
    }

    let mut i = 0;
    let mut j = 0;
    let mut value = Vec::with_capacity(dp[0] as usize);
    let mut matches = Vec::new();
    let mut active: Option<LcsMatch> = None;
    while i < rows && j < cols {
        if left[i] == right[j] {
            value.push(left[i]);
            match active.as_mut() {
                Some(matched)
                    if matched.left_start + matched.len == i
                        && matched.right_start + matched.len == j =>
                {
                    matched.len += 1;
                }
                _ => {
                    if let Some(matched) = active.take() {
                        matches.push(matched);
                    }
                    active = Some(LcsMatch {
                        left_start: i,
                        right_start: j,
                        len: 1,
                    });
                }
            }
            i += 1;
            j += 1;
        } else if dp[(i + 1) * stride + j] >= dp[i * stride + j + 1] {
            if let Some(matched) = active.take() {
                matches.push(matched);
            }
            i += 1;
        } else {
            if let Some(matched) = active.take() {
                matches.push(matched);
            }
            j += 1;
        }
    }
    if let Some(matched) = active {
        matches.push(matched);
    }
    Ok(LcsResult { value, matches })
}

fn lcs_length(left: &[u8], right: &[u8]) -> Result<usize, Error> {
    left.len()
        .checked_mul(right.len())
        .filter(|cells| *cells <= MAX_LCS_CELLS)
        .ok_or_else(lcs_limit_error)?;
    let (rows, cols) = if left.len() < right.len() {
        (right, left)
    } else {
        (left, right)
    };
    let len = cols.len().checked_add(1).ok_or_else(lcs_limit_error)?;
    let mut current = Vec::<u32>::new();
    current
        .try_reserve_exact(len)
        .map_err(|_| lcs_limit_error())?;
    current.resize(len, 0);
    for row in rows {
        let mut diagonal = 0u32;
        for (index, col) in cols.iter().enumerate() {
            let above = current[index + 1];
            current[index + 1] = if row == col {
                diagonal + 1
            } else {
                current[index + 1].max(current[index])
            };
            diagonal = above;
        }
    }
    Ok(current.last().copied().unwrap_or_default() as usize)
}

fn index_range_frame(start: usize, len: usize) -> Frame {
    Frame::Array(vec![
        Frame::Integer(start as i64),
        Frame::Integer(start.saturating_add(len).saturating_sub(1) as i64),
    ])
}

fn lcs_limit_error() -> Error {
    Error::msg("ERR LCS input exceeds configured computation limit")
}

#[cfg(test)]
mod tests {
    use super::{lcs_length, lcs_result};

    #[test]
    fn lcs_is_binary_safe_and_reports_length() {
        assert_eq!(
            lcs_result(b"ohmytext", b"mynewtext").unwrap().value,
            b"mytext"
        );
        assert_eq!(lcs_length(b"a\0bc", b"x\0bc").unwrap(), 3);
    }
}
