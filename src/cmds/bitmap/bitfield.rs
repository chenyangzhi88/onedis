use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration, read_bits_from, write_bits_into},
};
use anyhow::Error;

pub struct Bitfield {
    key: String,
    ops: Vec<BitfieldOp>,
    readonly: bool,
}
enum BitfieldOp {
    Get(BitType, usize),
    Set(BitType, usize, i64, Overflow),
    IncrBy(BitType, usize, i64, Overflow),
}
#[derive(Clone, Copy)]
struct BitType {
    signed: bool,
    width: usize,
}
#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

impl Bitfield {
    pub(crate) fn command_name(&self) -> &'static str {
        if self.readonly {
            "BITFIELD_RO"
        } else {
            "BITFIELD"
        }
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.readonly
    }

    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse(frame, false)
    }
    pub fn parse_ro_from_frame(frame: Frame) -> Result<Self, Error> {
        parse(frame, true)
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut bytes = match db.get_string_bytes(&self.key) {
            Ok(value) => value.unwrap_or_default(),
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        let (out, changed) = match execute_ops(&mut bytes, &self.ops) {
            Ok(result) => result,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        if changed
            && let Err(err) = db.set_string_bytes(
                self.key,
                bytes,
                SetExpiration::KeepTtl,
                SetCondition::Always,
                false,
            )
        {
            return Ok(Frame::Error(err.to_string()));
        }
        Ok(Frame::Array(out))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if self
            .ops
            .iter()
            .all(|op| matches!(op, BitfieldOp::Get(_, _)))
        {
            let mut bytes = match db.get_string_bytes_async(&self.key).await {
                Ok(value) => value.unwrap_or_default(),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            };
            return match execute_ops(&mut bytes, &self.ops) {
                Ok((out, _)) => Ok(Frame::Array(out)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            };
        }

        match db
            .mutate_string_bytes_if_changed_async(&self.key, |bytes, _| {
                execute_ops(bytes, &self.ops)
            })
            .await
        {
            Ok(out) => Ok(Frame::Array(out)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn execute_ops(bytes: &mut Vec<u8>, ops: &[BitfieldOp]) -> Result<(Vec<Frame>, bool), Error> {
    let mut out = Vec::with_capacity(ops.len());
    let mut changed = false;
    for op in ops {
        match *op {
            BitfieldOp::Get(ty, offset) => {
                out.push(Frame::Integer(read_bits_from(
                    bytes, offset, ty.width, ty.signed,
                )?));
            }
            BitfieldOp::Set(ty, offset, value, overflow) => {
                let old = read_bits_from(bytes, offset, ty.width, ty.signed)?;
                let Some(value) = apply_overflow(value as i128, ty, overflow) else {
                    out.push(Frame::Null);
                    continue;
                };
                write_bits_into(bytes, offset, ty.width, value)?;
                changed = true;
                out.push(Frame::Integer(old));
            }
            BitfieldOp::IncrBy(ty, offset, increment, overflow) => {
                let old = read_bits_from(bytes, offset, ty.width, ty.signed)?;
                let Some(next) = apply_overflow(old as i128 + increment as i128, ty, overflow)
                else {
                    out.push(Frame::Null);
                    continue;
                };
                write_bits_into(bytes, offset, ty.width, next)?;
                changed = true;
                out.push(Frame::Integer(next));
            }
        }
    }
    Ok((out, changed))
}

fn parse(frame: Frame, readonly: bool) -> Result<Bitfield, Error> {
    if frame.arg_len() < 2 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'bitfield' command",
        ));
    }
    let key = frame.get_arg(1).unwrap();
    let mut ops = Vec::new();
    let mut idx = 2;
    let mut overflow = Overflow::Wrap;
    while idx < frame.arg_len() {
        match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
            "GET" if idx + 2 < frame.arg_len() => {
                ops.push(BitfieldOp::Get(
                    parse_type(&frame.get_arg(idx + 1).unwrap())?,
                    parse_offset(
                        &frame.get_arg(idx + 2).unwrap(),
                        parse_type(&frame.get_arg(idx + 1).unwrap())?.width,
                    )?,
                ));
                idx += 3;
            }
            "SET" if idx + 3 < frame.arg_len() => {
                if readonly {
                    return Err(Error::msg("ERR BITFIELD_RO only supports GET"));
                }
                let ty = parse_type(&frame.get_arg(idx + 1).unwrap())?;
                ops.push(BitfieldOp::Set(
                    ty,
                    parse_offset(&frame.get_arg(idx + 2).unwrap(), ty.width)?,
                    frame
                        .get_arg(idx + 3)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                    overflow,
                ));
                idx += 4;
            }
            "INCRBY" if idx + 3 < frame.arg_len() => {
                if readonly {
                    return Err(Error::msg("ERR BITFIELD_RO only supports GET"));
                }
                let ty = parse_type(&frame.get_arg(idx + 1).unwrap())?;
                ops.push(BitfieldOp::IncrBy(
                    ty,
                    parse_offset(&frame.get_arg(idx + 2).unwrap(), ty.width)?,
                    frame
                        .get_arg(idx + 3)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                    overflow,
                ));
                idx += 4;
            }
            "OVERFLOW" if !readonly && idx + 1 < frame.arg_len() => {
                overflow = match frame
                    .get_arg(idx + 1)
                    .unwrap()
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return Err(Error::msg("ERR syntax error")),
                };
                idx += 2;
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
    }
    Ok(Bitfield { key, ops, readonly })
}

fn parse_type(text: &str) -> Result<BitType, Error> {
    let (signed, width) = match text.as_bytes().first().copied() {
        Some(b'i') | Some(b'I') => (true, &text[1..]),
        Some(b'u') | Some(b'U') => (false, &text[1..]),
        _ => return Err(Error::msg("ERR invalid bitfield type")),
    };
    let width = width
        .parse()
        .map_err(|_| Error::msg("ERR invalid bitfield type"))?;
    if width == 0 || (signed && width > 64) || (!signed && width > 63) {
        return Err(Error::msg("ERR invalid bitfield type"));
    }
    Ok(BitType { signed, width })
}

fn parse_offset(text: &str, width: usize) -> Result<usize, Error> {
    if let Some(multiplied) = text.strip_prefix('#') {
        multiplied
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR bit offset is not an integer or out of range"))?
            .checked_mul(width)
            .ok_or_else(|| Error::msg("ERR bit offset is not an integer or out of range"))
    } else {
        text.parse()
            .map_err(|_| Error::msg("ERR bit offset is not an integer or out of range"))
    }
}

fn apply_overflow(value: i128, ty: BitType, overflow: Overflow) -> Option<i64> {
    let modulus = 1i128 << ty.width;
    let (min, max) = if ty.signed {
        let sign = 1i128 << (ty.width - 1);
        (-sign, sign - 1)
    } else {
        (0, modulus - 1)
    };
    match overflow {
        Overflow::Fail if value < min || value > max => None,
        Overflow::Fail => Some(value as i64),
        Overflow::Sat => Some(value.clamp(min, max) as i64),
        Overflow::Wrap => {
            let wrapped = value.rem_euclid(modulus);
            if ty.signed && wrapped > max {
                Some((wrapped - modulus) as i64)
            } else {
                Some(wrapped as i64)
            }
        }
    }
}
