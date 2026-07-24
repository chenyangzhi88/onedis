use anyhow::Error;

use crate::frame::Frame;
use crate::server::Handler;

pub struct Client {
    subcommand: String,
    args: Vec<String>,
}

impl Client {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'client' command",
            ));
        }

        let subcommand = args[1].to_ascii_uppercase();
        let args: Vec<String> = args.iter().skip(2).map(|s| s.to_string()).collect();
        Ok(Client { subcommand, args })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        match self.subcommand.as_str() {
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk_string("CLIENT LIST"),
                Frame::bulk_string("CLIENT ID"),
                Frame::bulk_string("CLIENT SETNAME <name>"),
                Frame::bulk_string("CLIENT GETNAME"),
                Frame::bulk_string("CLIENT SETINFO <attr> <value>"),
            ])),
            "INFO" => Ok(Frame::bulk_string(
                "id=0 addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=-1 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default resp=2\r\n",
            )),
            "LIST" => Ok(Frame::bulk_string(
                "id=0 addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=-1 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default resp=2\r\n",
            )),
            "SETINFO" => {
                if self.args.len() != 2 {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|setinfo' command".to_string(),
                    ));
                }
                Ok(Frame::Ok)
            }
            "SETNAME" => {
                if self.args.len() != 1 {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|setname' command".to_string(),
                    ));
                }
                Ok(Frame::Ok)
            }
            "GETNAME" => {
                if !self.args.is_empty() {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|getname' command".to_string(),
                    ));
                }
                Ok(Frame::Null)
            }
            "ID" => {
                if !self.args.is_empty() {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|id' command".to_string(),
                    ));
                }
                Ok(Frame::Integer(0))
            }
            "GETREDIR" => Ok(Frame::Integer(-1)),
            "KILL" | "PAUSE" | "UNPAUSE" | "NO-EVICT" | "NO-TOUCH" | "CACHING" | "REPLY"
            | "TRACKING" => Ok(Frame::Ok),
            "TRACKINGINFO" => Ok(Frame::Array(vec![
                Frame::bulk_string("flags"),
                Frame::Array(Vec::new()),
                Frame::bulk_string("redirect"),
                Frame::Integer(-1),
                Frame::bulk_string("prefixes"),
                Frame::Array(Vec::new()),
            ])),
            "UNBLOCK" => Ok(Frame::Integer(0)),
            _ => Ok(Frame::Error(format!(
                "ERR unknown subcommand '{}'",
                self.subcommand
            ))),
        }
    }

    pub fn apply_with_handler(self, handler: &mut Handler) -> Result<Frame, Error> {
        match self.subcommand.as_str() {
            "LIST" => Ok(Frame::bulk_string(
                handler.get_session_manager().client_list(),
            )),
            "INFO" => Ok(Frame::bulk_string(
                handler
                    .get_session_manager()
                    .client_info(handler.get_session().get_id())
                    .unwrap_or_default(),
            )),
            "SETNAME" => {
                if self.args.len() != 1 {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|setname' command".to_string(),
                    ));
                }
                let name = &self.args[0];
                if !name.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
                    return Ok(Frame::Error(
                        "ERR Client names cannot contain spaces, newlines or special characters."
                            .to_string(),
                    ));
                }
                handler.set_client_name((!name.is_empty()).then(|| name.clone()));
                Ok(Frame::Ok)
            }
            "GETNAME" => {
                if !self.args.is_empty() {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client|getname' command".to_string(),
                    ));
                }
                Ok(handler
                    .client_name()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null))
            }
            "ID" => Ok(Frame::Integer(handler.get_session().get_id() as i64)),
            _ => self.apply(),
        }
    }
}
