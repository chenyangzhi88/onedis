use anyhow::Error;

use crate::store::db::{ExpireCondition, StringExpireUpdate};

pub(crate) fn parse_hash_fields(args: &[String], start: usize) -> Result<Vec<String>, Error> {
    if start >= args.len() {
        return Err(Error::msg("ERR wrong number of arguments for hash command"));
    }
    if args[start].eq_ignore_ascii_case("FIELDS") {
        let count = args
            .get(start + 1)
            .ok_or_else(|| Error::msg("ERR syntax error"))?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let fields_start = start + 2;
        if args.len() != fields_start + count {
            return Err(Error::msg("ERR syntax error"));
        }
        return Ok(args[fields_start..].to_vec());
    }
    Ok(args[start..].to_vec())
}

pub(crate) fn parse_hash_field_values(
    args: &[String],
    start: usize,
) -> Result<Vec<(String, String)>, Error> {
    if start >= args.len() {
        return Err(Error::msg("ERR wrong number of arguments for hash command"));
    }
    let values = if args[start].eq_ignore_ascii_case("FIELDS") {
        let count = args
            .get(start + 1)
            .ok_or_else(|| Error::msg("ERR syntax error"))?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let fields_start = start + 2;
        if args.len() != fields_start + count.saturating_mul(2) {
            return Err(Error::msg("ERR syntax error"));
        }
        &args[fields_start..]
    } else {
        &args[start..]
    };
    if values.len() % 2 != 0 {
        return Err(Error::msg("ERR wrong number of arguments for hash command"));
    }
    Ok(values
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect())
}

pub(crate) fn parse_expire_condition(
    args: &[String],
    idx: &mut usize,
) -> Result<ExpireCondition, Error> {
    let mut condition = ExpireCondition::Always;
    while *idx < args.len() {
        let next = match args[*idx].to_ascii_uppercase().as_str() {
            "NX" => ExpireCondition::Nx,
            "XX" => ExpireCondition::Xx,
            "GT" => ExpireCondition::Gt,
            "LT" => ExpireCondition::Lt,
            _ => break,
        };
        if condition != ExpireCondition::Always {
            return Err(Error::msg(
                "ERR NX, XX, GT, and LT options are not compatible",
            ));
        }
        condition = next;
        *idx += 1;
    }
    Ok(condition)
}

pub(crate) fn parse_expire_update(
    args: &[String],
    idx: &mut usize,
) -> Result<Option<StringExpireUpdate>, Error> {
    if *idx >= args.len() {
        return Ok(None);
    }
    match args[*idx].to_ascii_uppercase().as_str() {
        "PERSIST" => {
            *idx += 1;
            Ok(Some(StringExpireUpdate::Persist))
        }
        "EX" | "PX" | "EXAT" | "PXAT" => {
            let option = args[*idx].to_ascii_uppercase();
            let value = args
                .get(*idx + 1)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid expire time in hash command"))?;
            if value == 0 && matches!(option.as_str(), "EX" | "PX") {
                return Err(Error::msg("ERR invalid expire time in hash command"));
            }
            *idx += 2;
            Ok(Some(match option.as_str() {
                "EX" => StringExpireUpdate::RelativeMs(value.saturating_mul(1000)),
                "PX" => StringExpireUpdate::RelativeMs(value),
                "EXAT" => StringExpireUpdate::AbsoluteMs(value.saturating_mul(1000)),
                "PXAT" => StringExpireUpdate::AbsoluteMs(value),
                _ => unreachable!(),
            }))
        }
        _ => Ok(None),
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
