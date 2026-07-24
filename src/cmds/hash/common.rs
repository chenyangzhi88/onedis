use anyhow::Error;

use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES},
    store::db::{ExpireCondition, HASH_FIELD_MAX_EXPIRE_MS, StringExpireUpdate},
};

pub(crate) fn parse_hash_fields(args: &[String], start: usize) -> Result<Vec<String>, Error> {
    if start >= args.len() || !args[start].eq_ignore_ascii_case("FIELDS") {
        return Err(Error::msg("ERR syntax error"));
    }
    let count = args
        .get(start + 1)
        .ok_or_else(|| Error::msg("ERR syntax error"))?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
    if count == 0 {
        return Err(Error::msg("ERR numfields should be greater than 0"));
    }
    let fields_start = start + 2;
    let fields_end = fields_start
        .checked_add(count)
        .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
    if args.len() != fields_end {
        return Err(Error::msg("ERR syntax error"));
    }
    Ok(args[fields_start..].to_vec())
}

pub(crate) fn parse_expire_condition(
    args: &[String],
    idx: &mut usize,
) -> Result<ExpireCondition, Error> {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    while *idx < args.len() {
        match args[*idx].to_ascii_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => break,
        }
        *idx += 1;
    }
    if nx && (xx || gt || lt) {
        return Err(Error::msg(
            "ERR NX and XX, GT or LT options are not compatible",
        ));
    }
    if gt && lt {
        return Err(Error::msg("ERR GT and LT options are not compatible"));
    }
    Ok(match (nx, xx, gt, lt) {
        (true, false, false, false) => ExpireCondition::Nx,
        (false, true, true, false) => ExpireCondition::XxGt,
        (false, true, false, true) => ExpireCondition::XxLt,
        (false, true, false, false) => ExpireCondition::Xx,
        (false, false, true, false) => ExpireCondition::Gt,
        (false, false, false, true) => ExpireCondition::Lt,
        (false, false, false, false) => ExpireCondition::Always,
        _ => unreachable!("incompatible expiration options were rejected"),
    })
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
                .ok_or_else(|| Error::msg("ERR syntax error"))?;
            *idx += 2;
            Ok(Some(parse_expire_update_value(&option, value)?))
        }
        _ => Ok(None),
    }
}

pub(crate) fn parse_expire_update_value(
    option: &str,
    value: &str,
) -> Result<StringExpireUpdate, Error> {
    let value = value
        .parse::<i64>()
        .map_err(|_| Error::msg("ERR invalid expire time in hash command"))?;
    if value < 0 {
        return Err(Error::msg("ERR invalid expire time in hash command"));
    }
    let expiration = match option {
        "EX" => StringExpireUpdate::RelativeMs(
            u64::try_from(value)
                .ok()
                .and_then(|value| value.checked_mul(1000))
                .ok_or_else(|| Error::msg("ERR invalid expire time in hash command"))?,
        ),
        "PX" => StringExpireUpdate::RelativeMs(
            u64::try_from(value)
                .map_err(|_| Error::msg("ERR invalid expire time in hash command"))?,
        ),
        "EXAT" => StringExpireUpdate::AbsoluteMs(
            u64::try_from(value)
                .ok()
                .and_then(|value| value.checked_mul(1000))
                .ok_or_else(|| Error::msg("ERR invalid expire time in hash command"))?,
        ),
        "PXAT" => StringExpireUpdate::AbsoluteMs(
            u64::try_from(value)
                .map_err(|_| Error::msg("ERR invalid expire time in hash command"))?,
        ),
        _ => return Err(Error::msg("ERR syntax error")),
    };
    let value_ms = match expiration {
        StringExpireUpdate::RelativeMs(value_ms) | StringExpireUpdate::AbsoluteMs(value_ms) => {
            value_ms
        }
        StringExpireUpdate::Persist => unreachable!(),
    };
    if value_ms > HASH_FIELD_MAX_EXPIRE_MS {
        return Err(Error::msg("ERR invalid expire time in hash command"));
    }
    Ok(expiration)
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn checked_bulk_array(values: Vec<Option<Vec<u8>>>) -> Result<Frame, Error> {
    if values.len() > MAX_ARRAY_ELEMENTS {
        return Err(response_limit_error());
    }
    let mut encoded_len = resp_array_header_len(values.len());
    for value in &values {
        let item_len = match value {
            Some(value) => resp_bulk_len(value.len()),
            None => Some(5),
        }
        .ok_or_else(response_limit_error)?;
        encoded_len = encoded_len
            .checked_add(item_len)
            .filter(|len| *len <= MAX_FRAME_BYTES)
            .ok_or_else(response_limit_error)?;
    }
    Ok(Frame::Array(
        values
            .into_iter()
            .map(|value| value.map(Frame::bulk_string).unwrap_or(Frame::Null))
            .collect(),
    ))
}

pub(crate) fn checked_hash_entries(
    entries: Vec<(String, Vec<u8>)>,
    with_values: bool,
) -> Result<Frame, Error> {
    let item_count = entries
        .len()
        .checked_mul(if with_values { 2 } else { 1 })
        .ok_or_else(response_limit_error)?;
    if item_count > MAX_ARRAY_ELEMENTS {
        return Err(response_limit_error());
    }
    let mut values = Vec::with_capacity(item_count);
    for (field, value) in entries {
        values.push(Some(field.into_bytes()));
        if with_values {
            values.push(Some(value));
        }
    }
    checked_bulk_array(values)
}

pub(crate) fn checked_random_entries(
    entries: Vec<(String, Option<Vec<u8>>)>,
) -> Result<Frame, Error> {
    let value_count = entries.iter().filter(|(_, value)| value.is_some()).count();
    let item_count = entries
        .len()
        .checked_add(value_count)
        .ok_or_else(response_limit_error)?;
    if item_count > MAX_ARRAY_ELEMENTS {
        return Err(response_limit_error());
    }
    let mut values = Vec::with_capacity(item_count);
    for (field, value) in entries {
        values.push(Some(field.into_bytes()));
        if let Some(value) = value {
            values.push(Some(value));
        }
    }
    checked_bulk_array(values)
}

fn resp_array_header_len(items: usize) -> usize {
    1 + decimal_digits(items) + 2
}

fn resp_bulk_len(bytes: usize) -> Option<usize> {
    1usize
        .checked_add(decimal_digits(bytes))?
        .checked_add(2)?
        .checked_add(bytes)?
        .checked_add(2)
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn response_limit_error() -> Error {
    Error::msg("ERR response exceeds configured limit")
}
