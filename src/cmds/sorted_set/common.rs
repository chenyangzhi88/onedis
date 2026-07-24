use anyhow::Error;

use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES},
    store::db::ZsetAggregate,
};

pub(crate) fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR invalid UTF-8 argument"))
}

pub(crate) fn validate_entry_count(count: u64, withscores: bool) -> Result<(), Error> {
    let multiplier = if withscores { 2 } else { 1 };
    if count > (MAX_ARRAY_ELEMENTS / multiplier) as u64 {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

pub(crate) fn entries_with_scores(entries: Vec<(String, f64)>) -> Result<Vec<Frame>, Error> {
    validate_entry_count(entries.len() as u64, true)?;
    let mut frames = Vec::with_capacity(entries.len().saturating_mul(2));
    let mut bytes = 32usize;
    for (member, score) in entries {
        let score = score.to_string();
        bytes = bytes
            .checked_add(member.len().saturating_add(score.len()).saturating_add(64))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(member));
        frames.push(Frame::bulk_string(score));
    }
    Ok(frames)
}

pub(crate) fn parse_numkeys_command(frame: &Frame, command: &str) -> Result<Vec<String>, Error> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    }
    let num_keys = text_arg(frame, 1)?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
    let keys_end = 2usize
        .checked_add(num_keys)
        .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
    if num_keys == 0 || frame.arg_len() < keys_end {
        return Err(Error::msg("ERR syntax error"));
    }
    (0..num_keys)
        .map(|idx| text_arg(frame, 2 + idx))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn parse_weights_and_aggregate(
    frame: &Frame,
    mut idx: usize,
    num_keys: usize,
) -> Result<(Vec<f64>, ZsetAggregate, bool), Error> {
    let mut weights = vec![1.0; num_keys];
    let mut aggregate = ZsetAggregate::Sum;
    let mut withscores = false;
    while idx < frame.arg_len() {
        match text_arg(frame, idx)?.to_ascii_uppercase().as_str() {
            "WEIGHTS"
                if idx
                    .checked_add(num_keys)
                    .is_some_and(|end| end < frame.arg_len()) =>
            {
                for (offset, weight) in weights.iter_mut().enumerate() {
                    *weight = text_arg(frame, idx + 1 + offset)?
                        .parse::<f64>()
                        .map_err(|_| Error::msg("ERR weight value is not a float"))?;
                    if weight.is_nan() {
                        return Err(Error::msg("ERR weight value is not a float"));
                    }
                }
                idx = idx
                    .checked_add(1 + num_keys)
                    .ok_or_else(|| Error::msg("ERR value is out of range"))?;
            }
            "AGGREGATE" if idx + 1 < frame.arg_len() => {
                aggregate = match text_arg(frame, idx + 1)?.to_ascii_uppercase().as_str() {
                    "SUM" => ZsetAggregate::Sum,
                    "MIN" => ZsetAggregate::Min,
                    "MAX" => ZsetAggregate::Max,
                    _ => return Err(Error::msg("ERR syntax error")),
                };
                idx += 2;
            }
            "WITHSCORES" => {
                withscores = true;
                idx += 1;
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
    }
    Ok((weights, aggregate, withscores))
}
