use anyhow::Error;

use crate::frame::Frame;

pub struct Unknown {
    command_name: String,
    args: Vec<String>,
}

impl Unknown {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let command_name = match frame.get_arg(0) {
            Some(name) => name.to_string(),
            None => return Err(Error::msg("Failed to get command name")),
        };

        let args = frame.get_args().into_iter().skip(1).collect();

        Ok(Unknown { command_name, args })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        match self.command_name.to_ascii_uppercase().as_str() {
            "HELLO" => {
                return Ok(Frame::Array(vec![
                    Frame::bulk_string("server"),
                    Frame::bulk_string("onedis"),
                    Frame::bulk_string("version"),
                    Frame::bulk_string("0.1.0"),
                    Frame::bulk_string("proto"),
                    Frame::Integer(2),
                    Frame::bulk_string("id"),
                    Frame::Integer(0),
                    Frame::bulk_string("mode"),
                    Frame::bulk_string("standalone"),
                    Frame::bulk_string("role"),
                    Frame::bulk_string("master"),
                    Frame::bulk_string("modules"),
                    Frame::Array(Vec::new()),
                ]));
            }
            "QUIT" | "RESET" | "ASKING" | "READONLY" | "READWRITE" => return Ok(Frame::Ok),
            "TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                return Ok(Frame::Array(vec![
                    Frame::bulk_string(now.as_secs().to_string()),
                    Frame::bulk_string(now.subsec_micros().to_string()),
                ]));
            }
            "COMMAND" => return Ok(command_response(&self.args)),
            "MEMORY" => return Ok(memory_response(&self.args)),
            "ACL" => return Ok(acl_response(&self.args)),
            "CLUSTER" => return Ok(cluster_response(&self.args)),
            "LATENCY" | "SLOWLOG" | "MODULE" => return Ok(simple_subcommand_response(&self.args)),
            "PUBSUB" => return Ok(pubsub_response(&self.args)),
            "PUBLISH" | "SPUBLISH" => return Ok(Frame::Integer(0)),
            "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                return Ok(subscription_response(
                    self.command_name.to_ascii_lowercase(),
                    &self.args,
                    true,
                ));
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" => {
                return Ok(subscription_response(
                    self.command_name.to_ascii_lowercase(),
                    &self.args,
                    false,
                ));
            }
            _ => {}
        }
        Ok(Frame::Error(format!(
            "ERR unknown command `{}`, with args beginning with: `{}`",
            self.command_name,
            self.args.join(" ")
        )))
    }

    pub fn command_name(&self) -> &str {
        &self.command_name
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

fn command_response(args: &[String]) -> Frame {
    if args
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("DOCS") || arg.eq_ignore_ascii_case("INFO"))
    {
        return Frame::Array(Vec::new());
    }
    if args
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("COUNT"))
    {
        return Frame::Integer(0);
    }
    Frame::Array(Vec::new())
}

fn memory_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("USAGE") => Frame::Integer(0),
        Some("STATS") => Frame::Array(Vec::new()),
        Some("HELP") => Frame::Array(vec![Frame::bulk_string("MEMORY USAGE <key>")]),
        _ => Frame::Ok,
    }
}

fn acl_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("WHOAMI") => Frame::bulk_string("default"),
        Some("LIST") => Frame::Array(vec![Frame::bulk_string(
            "user default on nopass ~* &* +@all",
        )]),
        Some("USERS") => Frame::Array(vec![Frame::bulk_string("default")]),
        Some("CAT") => Frame::Array(Vec::new()),
        Some("HELP") => Frame::Array(vec![Frame::bulk_string("ACL compatibility surface")]),
        _ => Frame::Ok,
    }
}

fn cluster_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("INFO") => Frame::bulk_string("cluster_enabled:0\r\n"),
        Some("NODES") => Frame::bulk_string(""),
        Some("SLOTS") | Some("SHARDS") => Frame::Array(Vec::new()),
        Some("KEYSLOT") => Frame::Integer(0),
        Some("HELP") => Frame::Array(vec![Frame::bulk_string("CLUSTER compatibility surface")]),
        _ => Frame::Error("ERR cluster support disabled".to_string()),
    }
}

fn simple_subcommand_response(args: &[String]) -> Frame {
    if args
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("HELP"))
    {
        Frame::Array(Vec::new())
    } else {
        Frame::Array(Vec::new())
    }
}

fn pubsub_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("NUMSUB") => {
            let mut frames = Vec::new();
            for channel in args.iter().skip(1) {
                frames.push(Frame::bulk_string(channel.clone()));
                frames.push(Frame::Integer(0));
            }
            Frame::Array(frames)
        }
        Some("NUMPAT") => Frame::Integer(0),
        Some("CHANNELS") | Some("SHARDCHANNELS") | Some("SHARDNUMSUB") | Some("HELP") | None => {
            Frame::Array(Vec::new())
        }
        _ => Frame::Array(Vec::new()),
    }
}

fn subscription_response(command: String, args: &[String], subscribing: bool) -> Frame {
    let channels = if args.is_empty() {
        vec![String::new()]
    } else {
        args.to_vec()
    };
    Frame::Array(
        channels
            .into_iter()
            .enumerate()
            .map(|(idx, channel)| {
                Frame::Array(vec![
                    Frame::bulk_string(command.clone()),
                    Frame::bulk_string(channel),
                    Frame::Integer(if subscribing { idx + 1 } else { 0 } as i64),
                ])
            })
            .collect(),
    )
}
