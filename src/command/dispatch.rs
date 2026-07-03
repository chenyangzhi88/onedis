use anyhow::Error;

use crate::{command::Command, frame::Frame, store::db::Db};

mod async_dispatch;
mod autocommit;
mod sync_dispatch;

pub use async_dispatch::handle_command_async;
pub use autocommit::{handle_command_autocommit, handle_command_autocommit_async};
pub use sync_dispatch::handle_command;
