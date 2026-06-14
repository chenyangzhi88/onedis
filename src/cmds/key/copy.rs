use anyhow::Error;

use crate::{frame::Frame, server::Handler};

pub struct Copy {
    source: String,
    destination: String,
    db_index: Option<usize>,
    replace: bool,
}

impl Copy {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'copy' command",
            ));
        }
        let source = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let destination = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let mut db_index = None;
        let mut replace = false;
        let mut idx = 3;
        while idx < frame.arg_len() {
            let option = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR syntax error"))?;
            match option.to_ascii_uppercase().as_str() {
                "DB" => {
                    if db_index.is_some() || idx + 1 >= frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    db_index = Some(
                        frame
                            .get_arg(idx + 1)
                            .ok_or_else(|| Error::msg("ERR syntax error"))?
                            .parse::<usize>()
                            .map_err(|_| Error::msg("ERR DB index is out of range"))?,
                    );
                    idx += 2;
                }
                "REPLACE" => {
                    if replace {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    replace = true;
                    idx += 1;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }

        Ok(Copy {
            source,
            destination,
            db_index,
            replace,
        })
    }

    pub fn apply_sync(self, handler: &Handler) -> Result<Frame, Error> {
        let source_db = handler.get_session().get_current_db() as u16;
        let target_db = self.db_index.unwrap_or(source_db as usize);
        if handler.get_args().databases <= target_db {
            return Ok(Frame::Error("ERR DB index is out of range".to_string()));
        }
        let dm = handler.get_db_manager();
        let copied = crate::store::db::Db::copy_key_between_dbs(
            dm.store(),
            source_db,
            &self.source,
            target_db as u16,
            &self.destination,
            self.replace,
            dm.version_counter(),
            Some(dm.ttl_manager()),
        )?;
        Ok(Frame::Integer(if copied { 1 } else { 0 }))
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn destination(&self) -> &str {
        &self.destination
    }

    pub fn db_index(&self) -> Option<usize> {
        self.db_index
    }

    pub fn replace(&self) -> bool {
        self.replace
    }
}
