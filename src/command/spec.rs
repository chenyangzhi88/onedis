use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandAccess {
    Read,
    Write,
    Control,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockingKind {
    None,
    List,
    SortedSet,
    Stream,
}

#[derive(Clone, Copy, Debug)]
pub struct CommandSpec<'a> {
    pub name: &'a str,
    pub access: CommandAccess,
    pub blocking: bool,
    pub blocking_kind: BlockingKind,
    pub transaction_allowed: bool,
    pub lua_allowed: bool,
}

impl Command {
    pub fn spec(&self) -> CommandSpec<'_> {
        let name = self.effective_name();
        let capability = command_capability(name);
        let access = match self.dynamic_access() {
            Some(access) => access,
            None => match capability.map(|capability| capability.access.as_str()) {
                Some("read") => CommandAccess::Read,
                Some("write") => CommandAccess::Write,
                Some("control") => CommandAccess::Control,
                Some(_) | None
                    if matches!(self, Command::Unknown(_) | Command::FtUnsupported(_)) =>
                {
                    CommandAccess::Unsupported
                }
                Some(_) | None => CommandAccess::Control,
            },
        };
        let blocking_kind = self.blocking_kind();
        CommandSpec {
            name,
            access,
            blocking: blocking_kind != BlockingKind::None,
            blocking_kind,
            transaction_allowed: capability
                .is_some_and(|capability| capability.transaction_allowed),
            lua_allowed: capability.is_some_and(|capability| capability.lua_allowed),
        }
    }

    pub fn is_mutating(&self) -> bool {
        self.spec().access == CommandAccess::Write
    }

    fn dynamic_access(&self) -> Option<CommandAccess> {
        match self {
            Command::Unknown(_) | Command::FtUnsupported(_) => Some(CommandAccess::Unsupported),
            Command::Xcfgset(_) => Some(CommandAccess::Control),
            Command::Flushall(_) | Command::Flushdb(_) => Some(CommandAccess::Write),
            Command::Bitfield(bitfield) => Some(if bitfield.is_read_only() {
                CommandAccess::Read
            } else {
                CommandAccess::Write
            }),
            Command::Georadius(search) | Command::Georadiusbymember(search) => {
                Some(if search.stores_result() {
                    CommandAccess::Write
                } else {
                    CommandAccess::Read
                })
            }
            Command::FtConfig(config) => Some(if config.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            Command::FtDict(dict) => Some(if dict.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            Command::FtSug(suggestion) => Some(if suggestion.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            Command::FtSyn(synonym) => Some(if synonym.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            Command::Lua(lua) => Some(if lua.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            Command::Wasm(wasm) => Some(if wasm.may_write_data() {
                CommandAccess::Write
            } else {
                CommandAccess::Read
            }),
            _ => None,
        }
    }

    fn blocking_kind(&self) -> BlockingKind {
        match self {
            Command::Blmove(_)
            | Command::Blmpop(_)
            | Command::Blpop(_)
            | Command::Brpop(_)
            | Command::Brpoplpush(_) => BlockingKind::List,
            Command::Bzmpop(_) | Command::Bzpopmax(_) | Command::Bzpopmin(_) => {
                BlockingKind::SortedSet
            }
            Command::Xread(command) if command.block_ms.is_some() => BlockingKind::Stream,
            Command::Xreadgroup(command) if command.block_ms.is_some() => BlockingKind::Stream,
            _ => BlockingKind::None,
        }
    }
}
