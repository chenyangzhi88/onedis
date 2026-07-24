use crate::{frame::Frame, server::Handler};
use anyhow::Error;

pub struct Move {
    key: String,
    db_index: usize,
}

impl Move {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'move' command",
            ));
        }

        let key = args[1].to_string();
        let db_index = match args[2].parse::<usize>() {
            Ok(num) => num,
            Err(_) => {
                return Err(Error::msg("ERR index is not an integer"));
            }
        };

        Ok(Move { key, db_index })
    }

    pub fn get_key(&self) -> &String {
        &self.key
    }

    pub fn get_db_index(&self) -> usize {
        self.db_index
    }

    pub fn apply_sync(self, handler: &Handler) -> Result<Frame, Error> {
        if handler.get_args().databases <= self.db_index {
            return Ok(Frame::Error("ERR DB index is out of range".to_string()));
        }

        let source_db = handler.get_session().get_current_db() as u16;
        let target_db = self.db_index as u16;
        if source_db == target_db {
            return Ok(Frame::Integer(0));
        }

        let dm = handler.get_db_manager();
        let moved = dm
            .get_db(source_db as usize)
            .move_key_to_db(target_db, &self.key)?;
        Ok(Frame::Integer(if moved { 1 } else { 0 }))
    }
}
