use std::collections::HashSet;

use anyhow::Error;

use crate::frame::{Frame, MAX_FRAME_BYTES};
use crate::network::session_manager::{
    ClientKillFilter, ClientListFilter, ClientTypeFilter, ClientUnblockMode,
};
use crate::server::Handler;

pub struct Client {
    subcommand: String,
    args: Vec<String>,
}

enum ParsedKill {
    LegacyAddress(String),
    Filter(ClientKillFilter),
}

impl Client {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'client' command",
            ));
        }

        let mut args = frame.get_args_from_index(1).into_iter();
        let subcommand = args
            .next()
            .ok_or_else(|| Error::msg("ERR CLIENT subcommand must be a valid UTF-8 string"))?
            .to_ascii_uppercase();
        let args = args.collect();
        Ok(Client { subcommand, args })
    }

    pub fn command_name(&self) -> &'static str {
        match self.subcommand.as_str() {
            "CACHING" => "CLIENT|CACHING",
            "GETNAME" => "CLIENT|GETNAME",
            "GETREDIR" => "CLIENT|GETREDIR",
            "HELP" => "CLIENT|HELP",
            "ID" => "CLIENT|ID",
            "INFO" => "CLIENT|INFO",
            "KILL" => "CLIENT|KILL",
            "LIST" => "CLIENT|LIST",
            "NO-EVICT" => "CLIENT|NO-EVICT",
            "NO-TOUCH" => "CLIENT|NO-TOUCH",
            "PAUSE" => "CLIENT|PAUSE",
            "REPLY" => "CLIENT|REPLY",
            "SETINFO" => "CLIENT|SETINFO",
            "SETNAME" => "CLIENT|SETNAME",
            "TRACKING" => "CLIENT|TRACKING",
            "TRACKINGINFO" => "CLIENT|TRACKINGINFO",
            "UNBLOCK" => "CLIENT|UNBLOCK",
            "UNPAUSE" => "CLIENT|UNPAUSE",
            _ => "CLIENT",
        }
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(self
            .apply_inner()
            .unwrap_or_else(|error| Frame::Error(error.to_string())))
    }

    fn apply_inner(self) -> Result<Frame, Error> {
        match self.subcommand.as_str() {
            "HELP" => {
                self.require_no_args()?;
                Ok(Frame::Array(
                    [
                        "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "CACHING (not implemented)",
                        "GETNAME",
                        "GETREDIR",
                        "HELP",
                        "ID",
                        "INFO",
                        "KILL <ip:port | filter value [filter value ...]>",
                        "LIST [TYPE NORMAL|MASTER|REPLICA|PUBSUB] [ID client-id ...]",
                        "NO-EVICT ON|OFF",
                        "NO-TOUCH ON|OFF",
                        "PAUSE (not implemented)",
                        "REPLY (not implemented)",
                        "SETINFO LIB-NAME|LIB-VER <value>",
                        "SETNAME <name>",
                        "TRACKING (not implemented)",
                        "TRACKINGINFO",
                        "UNBLOCK <client-id> [TIMEOUT|ERROR]",
                        "UNPAUSE",
                    ]
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
                ))
            }
            "INFO" => {
                self.require_no_args()?;
                Ok(Frame::bulk_string(Self::placeholder_client_info()))
            }
            "LIST" => {
                self.parse_list_filter()?;
                Ok(Frame::bulk_string(Self::placeholder_client_info()))
            }
            "SETINFO" => {
                self.parse_setinfo()?;
                Ok(Frame::Ok)
            }
            "SETNAME" => {
                self.parse_setname()?;
                Ok(Frame::Ok)
            }
            "GETNAME" => {
                self.require_no_args()?;
                Ok(Frame::Null)
            }
            "ID" => {
                self.require_no_args()?;
                Ok(Frame::Integer(0))
            }
            "GETREDIR" => {
                self.require_no_args()?;
                Ok(Frame::Integer(-1))
            }
            "NO-EVICT" | "NO-TOUCH" => {
                self.parse_on_off()?;
                Ok(Frame::Ok)
            }
            "TRACKINGINFO" => {
                self.require_no_args()?;
                Ok(Self::tracking_info())
            }
            "UNBLOCK" => {
                self.parse_unblock()?;
                Ok(Frame::Integer(0))
            }
            "KILL" => match self.parse_kill()? {
                ParsedKill::LegacyAddress(_) => Ok(Frame::Error("ERR No such client".to_string())),
                ParsedKill::Filter(_) => Ok(Frame::Integer(0)),
            },
            "TRACKING" if self.args.len() == 1 && self.args[0].eq_ignore_ascii_case("OFF") => {
                Ok(Frame::Ok)
            }
            "UNPAUSE" => {
                self.require_no_args()?;
                Ok(Frame::Ok)
            }
            "CACHING" | "PAUSE" | "REPLY" | "TRACKING" => {
                self.validate_unsupported_syntax()?;
                Ok(self.unsupported())
            }
            _ => Ok(Frame::Error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                self.subcommand
            ))),
        }
    }

    pub fn apply_with_handler(self, handler: &mut Handler) -> Result<Frame, Error> {
        Ok(self
            .apply_with_handler_inner(handler)
            .unwrap_or_else(|error| Frame::Error(error.to_string())))
    }

    fn apply_with_handler_inner(self, handler: &mut Handler) -> Result<Frame, Error> {
        match self.subcommand.as_str() {
            "LIST" => {
                let filter = self.parse_list_filter()?;
                Ok(Frame::bulk_string(
                    handler
                        .get_session_manager()
                        .try_client_list_filtered(&filter, MAX_FRAME_BYTES)
                        .map_err(Error::msg)?,
                ))
            }
            "INFO" => {
                self.require_no_args()?;
                Ok(Frame::bulk_string(
                    handler
                        .get_session_manager()
                        .try_client_info(handler.get_session().get_id(), MAX_FRAME_BYTES)
                        .map_err(Error::msg)?
                        .unwrap_or_default(),
                ))
            }
            "SETINFO" => {
                let (attribute, value) = self.parse_setinfo()?;
                let value = (!value.is_empty()).then(|| value.to_string());
                match attribute {
                    "LIB-NAME" => handler.set_client_library_name(value),
                    "LIB-VER" => handler.set_client_library_version(value),
                    _ => unreachable!("SETINFO attribute was validated"),
                }
                Ok(Frame::Ok)
            }
            "SETNAME" => {
                let name = self.parse_setname()?;
                handler.set_client_name((!name.is_empty()).then(|| name.to_string()));
                Ok(Frame::Ok)
            }
            "GETNAME" => {
                self.require_no_args()?;
                Ok(handler
                    .client_name()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null))
            }
            "ID" => {
                self.require_no_args()?;
                Ok(Frame::Integer(handler.get_session().get_id() as i64))
            }
            "NO-EVICT" => {
                let enabled = self.parse_on_off()?;
                handler.set_client_no_evict(enabled);
                Ok(Frame::Ok)
            }
            "NO-TOUCH" => {
                let enabled = self.parse_on_off()?;
                handler.set_client_no_touch(enabled);
                Ok(Frame::Ok)
            }
            "KILL" => {
                let current_id = handler.get_session().get_id();
                let manager = handler.get_session_manager();
                match self.parse_kill()? {
                    ParsedKill::LegacyAddress(address) => {
                        let filter = ClientKillFilter {
                            addr: Some(address),
                            skip_current: false,
                            ..ClientKillFilter::default()
                        };
                        if manager.kill_clients(current_id, &filter) == 0 {
                            Ok(Frame::Error("ERR No such client".to_string()))
                        } else {
                            Ok(Frame::Ok)
                        }
                    }
                    ParsedKill::Filter(filter) => Ok(Frame::Integer(
                        manager.kill_clients(current_id, &filter) as i64,
                    )),
                }
            }
            "UNBLOCK" => {
                let (session_id, mode) = self.parse_unblock()?;
                Ok(Frame::Integer(i64::from(
                    handler
                        .get_session_manager()
                        .unblock_client(session_id, mode),
                )))
            }
            _ => self.apply(),
        }
    }

    fn require_no_args(&self) -> Result<(), Error> {
        if self.args.is_empty() {
            Ok(())
        } else {
            Err(self.wrong_arity())
        }
    }

    fn parse_setname(&self) -> Result<&str, Error> {
        if self.args.len() != 1 {
            return Err(self.wrong_arity());
        }
        let name = self.args[0].as_str();
        if !Self::valid_client_info_value(name) {
            return Err(Error::msg(
                "ERR Client names cannot contain spaces, newlines or special characters.",
            ));
        }
        Ok(name)
    }

    fn parse_setinfo(&self) -> Result<(&str, &str), Error> {
        if self.args.len() != 2 {
            return Err(self.wrong_arity());
        }
        let attribute = self.args[0].to_ascii_uppercase();
        if !matches!(attribute.as_str(), "LIB-NAME" | "LIB-VER") {
            return Err(Error::msg(format!(
                "ERR Unrecognized option or bad number of args for: '{}'",
                self.args[0]
            )));
        }
        let value = self.args[1].as_str();
        if !Self::valid_client_info_value(value) {
            return Err(Error::msg(
                "ERR Client library information cannot contain spaces, newlines or special characters.",
            ));
        }
        Ok((
            if attribute == "LIB-NAME" {
                "LIB-NAME"
            } else {
                "LIB-VER"
            },
            value,
        ))
    }

    fn valid_client_info_value(value: &str) -> bool {
        value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
    }

    fn parse_on_off(&self) -> Result<bool, Error> {
        if self.args.len() != 1 {
            return Err(self.wrong_arity());
        }
        match self.args[0].to_ascii_uppercase().as_str() {
            "ON" => Ok(true),
            "OFF" => Ok(false),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    fn parse_list_filter(&self) -> Result<ClientListFilter, Error> {
        let mut filter = ClientListFilter::default();
        let mut index = 0;

        if self
            .args
            .get(index)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("TYPE"))
        {
            let client_type = self.args.get(index + 1).ok_or_else(|| self.wrong_arity())?;
            filter.client_type = Some(Self::parse_client_type(client_type)?);
            index += 2;
        }

        if index < self.args.len() {
            if !self.args[index].eq_ignore_ascii_case("ID") || index + 1 >= self.args.len() {
                return Err(Error::msg("ERR syntax error"));
            }
            let ids = self.args[index + 1..]
                .iter()
                .map(|value| Self::parse_client_id(value))
                .collect::<Result<HashSet<_>, _>>()?;
            filter.ids = Some(ids);
        }

        Ok(filter)
    }

    fn parse_client_type(value: &str) -> Result<ClientTypeFilter, Error> {
        match value.to_ascii_uppercase().as_str() {
            "NORMAL" => Ok(ClientTypeFilter::Normal),
            "MASTER" => Ok(ClientTypeFilter::Master),
            "SLAVE" | "REPLICA" => Ok(ClientTypeFilter::Replica),
            "PUBSUB" => Ok(ClientTypeFilter::Pubsub),
            _ => Err(Error::msg("ERR Unknown client type")),
        }
    }

    fn parse_kill(&self) -> Result<ParsedKill, Error> {
        if self.args.is_empty() {
            return Err(self.wrong_arity());
        }
        if self.args.len() == 1
            && !matches!(
                self.args[0].to_ascii_uppercase().as_str(),
                "ID" | "TYPE" | "USER" | "ADDR" | "LADDR" | "SKIPME" | "MAXAGE"
            )
        {
            return Ok(ParsedKill::LegacyAddress(self.args[0].clone()));
        }
        if !self.args.len().is_multiple_of(2) {
            return Err(Error::msg("ERR syntax error"));
        }

        let mut filter = ClientKillFilter::default();
        for pair in self.args.chunks_exact(2) {
            match pair[0].to_ascii_uppercase().as_str() {
                "ID" => filter.id = Some(Self::parse_client_id(&pair[1])?),
                "TYPE" => filter.client_type = Some(Self::parse_client_type(&pair[1])?),
                "USER" => filter.user = Some(pair[1].clone()),
                "ADDR" => filter.addr = Some(pair[1].clone()),
                "LADDR" => filter.local_addr = Some(pair[1].clone()),
                "SKIPME" => {
                    filter.skip_current = match pair[1].to_ascii_uppercase().as_str() {
                        "YES" => true,
                        "NO" => false,
                        _ => return Err(Error::msg("ERR syntax error")),
                    };
                }
                "MAXAGE" => {
                    filter.min_age_secs =
                        Some(pair[1].parse::<u64>().map_err(|_| {
                            Error::msg("ERR value is not an integer or out of range")
                        })?);
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(ParsedKill::Filter(filter))
    }

    fn parse_unblock(&self) -> Result<(usize, ClientUnblockMode), Error> {
        if !(1..=2).contains(&self.args.len()) {
            return Err(self.wrong_arity());
        }
        let session_id = Self::parse_client_id(&self.args[0])?;
        let mode_arg = self.args.get(1).map(|value| value.to_ascii_uppercase());
        let mode = match mode_arg.as_deref() {
            None | Some("TIMEOUT") => ClientUnblockMode::Timeout,
            Some("ERROR") => ClientUnblockMode::Error,
            Some(_) => return Err(Error::msg("ERR syntax error")),
        };
        Ok((session_id, mode))
    }

    fn validate_unsupported_syntax(&self) -> Result<(), Error> {
        match self.subcommand.as_str() {
            "CACHING" => {
                if self.args.len() != 1 {
                    return Err(self.wrong_arity());
                }
                if !matches!(self.args[0].to_ascii_uppercase().as_str(), "YES" | "NO") {
                    return Err(Error::msg("ERR syntax error"));
                }
            }
            "PAUSE" => {
                if !(1..=2).contains(&self.args.len()) {
                    return Err(self.wrong_arity());
                }
                self.args[0]
                    .parse::<u64>()
                    .map_err(|_| Error::msg("ERR timeout is not an integer or out of range"))?;
                if self.args.get(1).is_some_and(|mode| {
                    !matches!(mode.to_ascii_uppercase().as_str(), "WRITE" | "ALL")
                }) {
                    return Err(Error::msg("ERR syntax error"));
                }
            }
            "REPLY" => {
                if self.args.len() != 1 {
                    return Err(self.wrong_arity());
                }
                if !matches!(
                    self.args[0].to_ascii_uppercase().as_str(),
                    "ON" | "OFF" | "SKIP"
                ) {
                    return Err(Error::msg("ERR syntax error"));
                }
            }
            "TRACKING" if self.args.is_empty() => return Err(self.wrong_arity()),
            "TRACKING" => {}
            _ => unreachable!("only unsupported CLIENT subcommands are validated"),
        }
        Ok(())
    }

    fn parse_client_id(value: &str) -> Result<usize, Error> {
        value
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))
    }

    fn tracking_info() -> Frame {
        Frame::Array(vec![
            Frame::bulk_string("flags"),
            Frame::Array(Vec::new()),
            Frame::bulk_string("redirect"),
            Frame::Integer(-1),
            Frame::bulk_string("prefixes"),
            Frame::Array(Vec::new()),
        ])
    }

    fn placeholder_client_info() -> &'static str {
        "id=0 addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=-1 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2 lib-name= lib-ver=\r\n"
    }

    fn wrong_arity(&self) -> Error {
        Error::msg(format!(
            "ERR wrong number of arguments for 'client|{}' command",
            self.subcommand.to_ascii_lowercase()
        ))
    }

    fn unsupported(&self) -> Frame {
        Frame::Error(format!(
            "ERR CLIENT {} is not supported by onedis",
            self.subcommand
        ))
    }
}
