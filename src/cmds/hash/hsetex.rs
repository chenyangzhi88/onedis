use anyhow::Error;

use crate::{
    cmds::hash::common::{parse_expire_update, parse_hash_field_values},
    frame::Frame,
    store::{db::Db, db::StringExpireUpdate},
};

pub struct Hsetex {
    key: String,
    fields: Vec<(String, String)>,
    expiration: Option<StringExpireUpdate>,
    keep_ttl: bool,
    fnx: bool,
    fxx: bool,
}

impl Hsetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hsetex' command",
            ));
        }
        let mut idx = 2;
        let mut fnx = false;
        let mut fxx = false;
        let mut keep_ttl = false;
        let mut expiration = None;
        while idx < args.len() {
            match args[idx].to_ascii_uppercase().as_str() {
                "FNX" => {
                    if fxx {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    fnx = true;
                    idx += 1;
                }
                "FXX" => {
                    if fnx {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    fxx = true;
                    idx += 1;
                }
                "KEEPTTL" => {
                    keep_ttl = true;
                    idx += 1;
                }
                _ => {
                    expiration = parse_expire_update(&args, &mut idx)?;
                    break;
                }
            }
        }
        let fields = parse_hash_field_values(&args, idx)?;
        Ok(Self {
            key: args[1].clone(),
            fields,
            expiration,
            keep_ttl,
            fnx,
            fxx,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_ex(
            &self.key,
            &self.fields,
            self.expiration,
            self.keep_ttl,
            self.fnx,
            self.fxx,
        ) {
            Ok(changed) => Ok(Frame::Integer(if changed { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_set_ex_async(
                &self.key,
                &self.fields,
                self.expiration,
                self.keep_ttl,
                self.fnx,
                self.fxx,
            )
            .await
        {
            Ok(changed) => Ok(Frame::Integer(if changed { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
