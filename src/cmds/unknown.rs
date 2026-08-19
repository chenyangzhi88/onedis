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
            "HELLO" | "RESET" | "QUIT" => {
                return Ok(connection_context_error(&self.command_name));
            }
            "ASKING" | "READONLY" | "READWRITE" => {
                return Ok(unsupported_response(&self.command_name));
            }
            "TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                return Ok(Frame::Array(vec![
                    Frame::bulk_string(now.as_secs().to_string()),
                    Frame::bulk_string(now.subsec_micros().to_string()),
                ]));
            }
            "COMMAND" => {
                return Ok(crate::command::command_introspection_response(&self.args));
            }
            "MEMORY" => return Ok(memory_response(&self.args)),
            "ACL" => return Ok(connection_context_error("ACL")),
            "CLUSTER" => return Ok(cluster_response(&self.args)),
            "LATENCY" | "SLOWLOG" | "MODULE" => {
                return Ok(unsupported_response(&self.command_name));
            }
            "PUBSUB" | "PUBLISH" | "SPUBLISH" | "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE"
            | "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" | "MONITOR" => {
                return Ok(connection_context_error(&self.command_name));
            }
            _ => {}
        }
        if crate::command::command_capability(&self.command_name)
            .is_some_and(|capability| capability.compatibility == "Unsupported")
        {
            return Ok(unsupported_response(&self.command_name));
        }
        Ok(Frame::Error(format!(
            "ERR unknown command `{}`, with args beginning with: `{}`",
            self.command_name,
            format_args_preview(&self.args)
        )))
    }

    pub fn command_name(&self) -> &str {
        &self.command_name
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

fn format_args_preview(args: &[String]) -> String {
    const MAX_PREVIEW_ARGS: usize = 3;
    const MAX_PREVIEW_CHARS: usize = 128;

    let mut preview = args
        .iter()
        .take(MAX_PREVIEW_ARGS)
        .map(|arg| arg.chars().take(MAX_PREVIEW_CHARS).collect::<String>())
        .collect::<Vec<_>>()
        .join(" ");
    if args.len() > MAX_PREVIEW_ARGS
        || args
            .iter()
            .take(MAX_PREVIEW_ARGS)
            .any(|arg| arg.chars().count() > MAX_PREVIEW_CHARS)
    {
        preview.push_str("...");
    }
    preview
}

fn memory_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("HELP") => Frame::Array(vec![Frame::bulk_string("MEMORY USAGE <key>")]),
        _ => unsupported_response("MEMORY"),
    }
}

fn cluster_response(args: &[String]) -> Frame {
    match args.first().map(|arg| arg.to_ascii_uppercase()).as_deref() {
        Some("INFO") => Frame::bulk_string("cluster_enabled:0\r\n"),
        Some("HELP") => Frame::Array(vec![Frame::bulk_string("CLUSTER compatibility surface")]),
        _ => Frame::Error("ERR cluster support disabled".to_string()),
    }
}

fn unsupported_response(command: &str) -> Frame {
    Frame::Error(format!(
        "ERR command '{}' is unsupported by OneDis",
        command.to_ascii_lowercase()
    ))
}

fn connection_context_error(command: &str) -> Frame {
    Frame::Error(format!(
        "ERR command '{}' requires a live client connection",
        command.to_ascii_lowercase()
    ))
}
