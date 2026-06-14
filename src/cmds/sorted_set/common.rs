use anyhow::Error;

use crate::{frame::Frame, store::db::ZsetAggregate};

pub(crate) fn entries_with_scores(entries: Vec<(String, f64)>) -> Vec<Frame> {
    let mut frames = Vec::with_capacity(entries.len() * 2);
    for (member, score) in entries {
        frames.push(Frame::bulk_string(member));
        frames.push(Frame::bulk_string(score.to_string()));
    }
    frames
}

pub(crate) fn parse_numkeys_command(frame: &Frame, command: &str) -> Result<Vec<String>, Error> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    }
    let num_keys = frame
        .get_arg(1)
        .unwrap()
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
    if num_keys == 0 || frame.arg_len() < 2 + num_keys {
        return Err(Error::msg("ERR syntax error"));
    }
    Ok((0..num_keys)
        .map(|idx| frame.get_arg(2 + idx).unwrap())
        .collect())
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
        match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
            "WEIGHTS" if idx + num_keys < frame.arg_len() => {
                for (offset, weight) in weights.iter_mut().enumerate() {
                    *weight = frame
                        .get_arg(idx + 1 + offset)
                        .unwrap()
                        .parse::<f64>()
                        .map_err(|_| Error::msg("ERR weight value is not a float"))?;
                }
                idx += 1 + num_keys;
            }
            "AGGREGATE" if idx + 1 < frame.arg_len() => {
                aggregate = match frame
                    .get_arg(idx + 1)
                    .unwrap()
                    .to_ascii_uppercase()
                    .as_str()
                {
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
