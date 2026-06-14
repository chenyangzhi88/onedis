use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Bitfield {
    key: String,
    ops: Vec<BitfieldOp>,
    readonly: bool,
}
enum BitfieldOp {
    Get(BitType, usize),
    Set(BitType, usize, i64),
    IncrBy(BitType, usize, i64),
}
#[derive(Clone, Copy)]
struct BitType {
    signed: bool,
    width: usize,
}

impl Bitfield {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse(frame, false)
    }
    pub fn parse_ro_from_frame(frame: Frame) -> Result<Self, Error> {
        parse(frame, true)
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut out = Vec::new();
        for op in self.ops {
            match op {
                BitfieldOp::Get(ty, offset) => {
                    match db.string_read_bits(&self.key, offset, ty.width, ty.signed) {
                        Ok(value) => out.push(Frame::Integer(value)),
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    }
                }
                BitfieldOp::Set(ty, offset, value) if !self.readonly => {
                    let old = match db.string_read_bits(&self.key, offset, ty.width, ty.signed) {
                        Ok(value) => value,
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    };
                    if let Err(err) = db.string_write_bits(&self.key, offset, ty.width, value) {
                        return Ok(Frame::Error(err.to_string()));
                    }
                    out.push(Frame::Integer(old));
                }
                BitfieldOp::IncrBy(ty, offset, increment) if !self.readonly => {
                    let old = match db.string_read_bits(&self.key, offset, ty.width, ty.signed) {
                        Ok(value) => value,
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    };
                    let next = old.wrapping_add(increment);
                    if let Err(err) = db.string_write_bits(&self.key, offset, ty.width, next) {
                        return Ok(Frame::Error(err.to_string()));
                    }
                    out.push(Frame::Integer(next));
                }
                _ => {
                    return Ok(Frame::Error(
                        "ERR BITFIELD_RO only supports GET".to_string(),
                    ));
                }
            }
        }
        Ok(Frame::Array(out))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut out = Vec::new();
        for op in self.ops {
            match op {
                BitfieldOp::Get(ty, offset) => {
                    match db
                        .string_read_bits_async(&self.key, offset, ty.width, ty.signed)
                        .await
                    {
                        Ok(value) => out.push(Frame::Integer(value)),
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    }
                }
                BitfieldOp::Set(ty, offset, value) if !self.readonly => {
                    let old = match db
                        .string_read_bits_async(&self.key, offset, ty.width, ty.signed)
                        .await
                    {
                        Ok(value) => value,
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    };
                    if let Err(err) = db
                        .string_write_bits_async(&self.key, offset, ty.width, value)
                        .await
                    {
                        return Ok(Frame::Error(err.to_string()));
                    }
                    out.push(Frame::Integer(old));
                }
                BitfieldOp::IncrBy(ty, offset, increment) if !self.readonly => {
                    let old = match db
                        .string_read_bits_async(&self.key, offset, ty.width, ty.signed)
                        .await
                    {
                        Ok(value) => value,
                        Err(err) => return Ok(Frame::Error(err.to_string())),
                    };
                    let next = old.wrapping_add(increment);
                    if let Err(err) = db
                        .string_write_bits_async(&self.key, offset, ty.width, next)
                        .await
                    {
                        return Ok(Frame::Error(err.to_string()));
                    }
                    out.push(Frame::Integer(next));
                }
                _ => {
                    return Ok(Frame::Error(
                        "ERR BITFIELD_RO only supports GET".to_string(),
                    ));
                }
            }
        }
        Ok(Frame::Array(out))
    }
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
                let ty = parse_type(&frame.get_arg(idx + 1).unwrap())?;
                ops.push(BitfieldOp::Set(
                    ty,
                    parse_offset(&frame.get_arg(idx + 2).unwrap(), ty.width)?,
                    frame
                        .get_arg(idx + 3)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                ));
                idx += 4;
            }
            "INCRBY" if idx + 3 < frame.arg_len() => {
                let ty = parse_type(&frame.get_arg(idx + 1).unwrap())?;
                ops.push(BitfieldOp::IncrBy(
                    ty,
                    parse_offset(&frame.get_arg(idx + 2).unwrap(), ty.width)?,
                    frame
                        .get_arg(idx + 3)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                ));
                idx += 4;
            }
            "OVERFLOW" if idx + 1 < frame.arg_len() => idx += 2,
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
    Ok(BitType {
        signed,
        width: width
            .parse()
            .map_err(|_| Error::msg("ERR invalid bitfield type"))?,
    })
}

fn parse_offset(text: &str, width: usize) -> Result<usize, Error> {
    if let Some(multiplied) = text.strip_prefix('#') {
        Ok(multiplied
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR bit offset is not an integer or out of range"))?
            * width)
    } else {
        text.parse()
            .map_err(|_| Error::msg("ERR bit offset is not an integer or out of range"))
    }
}
