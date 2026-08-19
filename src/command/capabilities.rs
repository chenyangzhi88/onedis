use std::sync::OnceLock;

use crate::frame::Frame;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandCapability {
    pub name: String,
    pub compatibility: String,
    pub access: String,
    pub blocking: bool,
    pub key_spec: String,
    pub execution_role: String,
    pub acl_category: String,
    pub transaction_allowed: bool,
    pub lua_allowed: bool,
}

const CAPABILITY_MANIFEST: &str = include_str!("../../docs/redis_user_command_compat.json");

pub fn command_capabilities() -> &'static [CommandCapability] {
    static CAPABILITIES: OnceLock<Vec<CommandCapability>> = OnceLock::new();
    CAPABILITIES.get_or_init(|| {
        let manifest: serde_json::Value = serde_json::from_str(CAPABILITY_MANIFEST)
            .expect("embedded command capability manifest must be valid JSON");
        let mut commands = manifest["commands"]
            .as_array()
            .expect("command capability manifest must contain commands")
            .iter()
            .map(|command| {
                let name = command["name"].as_str().unwrap_or_default().to_string();
                let compatibility = command["compatibility"]
                    .as_str()
                    .unwrap_or("Unsupported")
                    .to_string();
                let access = command["access"]
                    .as_str()
                    .unwrap_or("unsupported")
                    .to_string();
                let blocking = command["blocking"].as_bool().unwrap_or(false);
                let supported = compatibility != "Unsupported";
                let data_command = matches!(access.as_str(), "read" | "write");
                let transaction_control = matches!(
                    name.as_str(),
                    "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH"
                );
                let script_control = name.starts_with("SCRIPT")
                    || matches!(name.as_str(), "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO");
                CommandCapability {
                    name,
                    compatibility,
                    execution_role: if !supported {
                        "unsupported"
                    } else if data_command {
                        "data"
                    } else {
                        "control"
                    }
                    .to_string(),
                    acl_category: format!("@{access}"),
                    transaction_allowed: supported
                        && data_command
                        && !blocking
                        && !transaction_control,
                    lua_allowed: supported && data_command && !blocking && !script_control,
                    access,
                    blocking,
                    key_spec: command["key_spec"].as_str().unwrap_or("none").to_string(),
                }
            })
            .collect::<Vec<_>>();
        commands.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        commands
    })
}

pub fn command_capability(name: &str) -> Option<&'static CommandCapability> {
    let name = name.to_ascii_uppercase();
    let commands = command_capabilities();
    commands
        .binary_search_by(|command| command.name.as_str().cmp(name.as_str()))
        .ok()
        .map(|index| &commands[index])
}

pub(crate) fn command_introspection_response(args: &[String]) -> Frame {
    let Some(subcommand) = args.first().map(|value| value.to_ascii_uppercase()) else {
        return Frame::Array(
            command_capabilities()
                .iter()
                .filter(|command| command.compatibility != "Unsupported")
                .map(command_info_frame)
                .collect(),
        );
    };
    match subcommand.as_str() {
        "COUNT" if args.len() == 1 => Frame::Integer(
            command_capabilities()
                .iter()
                .filter(|command| command.compatibility != "Unsupported")
                .count() as i64,
        ),
        "LIST" => command_list_response(&args[1..]),
        "INFO" => {
            let selected = if args.len() == 1 {
                command_capabilities()
                    .iter()
                    .filter(|command| command.compatibility != "Unsupported")
                    .map(command_info_frame)
                    .collect()
            } else {
                args[1..]
                    .iter()
                    .map(|name| {
                        command_capability(name)
                            .filter(|command| command.compatibility != "Unsupported")
                            .map_or(Frame::Null, command_info_frame)
                    })
                    .collect()
            };
            Frame::Array(selected)
        }
        "DOCS" => command_docs_response(&args[1..]),
        "GETKEYS" => command_getkeys_response(&args[1..], false),
        "GETKEYSANDFLAGS" => command_getkeys_response(&args[1..], true),
        "HELP" if args.len() == 1 => Frame::Array(vec![
            Frame::bulk_string("COMMAND COUNT"),
            Frame::bulk_string("COMMAND LIST [FILTERBY PATTERN <glob>]"),
            Frame::bulk_string("COMMAND INFO [command-name ...]"),
            Frame::bulk_string("COMMAND DOCS [command-name ...]"),
            Frame::bulk_string("COMMAND GETKEYS <full-command>"),
            Frame::bulk_string("COMMAND GETKEYSANDFLAGS <full-command>"),
        ]),
        _ => Frame::Error("ERR syntax error".to_string()),
    }
}

fn command_list_response(args: &[String]) -> Frame {
    let pattern = match args {
        [] => None,
        [filter, kind, pattern]
            if filter.eq_ignore_ascii_case("FILTERBY") && kind.eq_ignore_ascii_case("PATTERN") =>
        {
            Some(pattern.to_ascii_lowercase())
        }
        _ => return Frame::Error("ERR syntax error".to_string()),
    };
    Frame::Array(
        command_capabilities()
            .iter()
            .filter(|command| command.compatibility != "Unsupported")
            .filter(|command| {
                pattern.as_ref().is_none_or(|pattern| {
                    simple_glob_matches(pattern, &command.name.to_ascii_lowercase())
                })
            })
            .map(|command| Frame::bulk_string(command.name.to_ascii_lowercase()))
            .collect(),
    )
}

fn command_info_frame(command: &CommandCapability) -> Frame {
    let mut flags = Vec::new();
    match command.access.as_str() {
        "read" => flags.push(Frame::bulk_string("readonly")),
        "write" => flags.push(Frame::bulk_string("write")),
        "admin" => flags.push(Frame::bulk_string("admin")),
        _ => {}
    }
    if command.blocking {
        flags.push(Frame::bulk_string("blocking"));
    }
    if !command.lua_allowed {
        flags.push(Frame::bulk_string("noscript"));
    }
    if command.compatibility == "Unsupported" {
        flags.push(Frame::bulk_string("unsupported"));
    }
    let (first, last, step) = match command.key_spec.as_str() {
        "first" => (1, 1, 1),
        "all" => (1, -1, 1),
        _ => (0, 0, 0),
    };
    Frame::Array(vec![
        Frame::bulk_string(command.name.to_ascii_lowercase()),
        Frame::Integer(-1),
        Frame::Array(flags),
        Frame::Integer(first),
        Frame::Integer(last),
        Frame::Integer(step),
        Frame::Array(Vec::new()),
        Frame::Array(Vec::new()),
        Frame::Array(Vec::new()),
        Frame::Array(Vec::new()),
    ])
}

fn command_docs_response(names: &[String]) -> Frame {
    let selected = if names.is_empty() {
        command_capabilities()
            .iter()
            .filter(|command| command.compatibility != "Unsupported")
            .collect::<Vec<_>>()
    } else {
        names
            .iter()
            .filter_map(|name| command_capability(name))
            .filter(|command| command.compatibility != "Unsupported")
            .collect()
    };
    let mut docs = Vec::with_capacity(selected.len() * 2);
    for command in selected {
        docs.push(Frame::bulk_string(command.name.to_ascii_lowercase()));
        docs.push(Frame::Array(vec![
            Frame::bulk_string("summary"),
            Frame::bulk_string(format!(
                "OneDis {} command ({})",
                command.compatibility, command.access
            )),
            Frame::bulk_string("since"),
            Frame::bulk_string(env!("CARGO_PKG_VERSION")),
            Frame::bulk_string("group"),
            Frame::bulk_string(command_group(command)),
            Frame::bulk_string("onedis_compatibility"),
            Frame::bulk_string(command.compatibility.clone()),
            Frame::bulk_string("onedis_execution_role"),
            Frame::bulk_string(command.execution_role.clone()),
            Frame::bulk_string("onedis_acl_category"),
            Frame::bulk_string(command.acl_category.clone()),
            Frame::bulk_string("onedis_transaction_allowed"),
            Frame::Integer(i64::from(command.transaction_allowed)),
            Frame::bulk_string("onedis_lua_allowed"),
            Frame::Integer(i64::from(command.lua_allowed)),
        ]));
    }
    Frame::Array(docs)
}

fn command_group(command: &CommandCapability) -> &'static str {
    if command.name.starts_with("FT.") {
        "search"
    } else if command.name.starts_with("JSON.") {
        "json"
    } else if command.name.starts_with('V') {
        "vector"
    } else {
        match command.access.as_str() {
            "connection" => "connection",
            "admin" => "server",
            _ => "generic",
        }
    }
}

fn command_getkeys_response(args: &[String], with_flags: bool) -> Frame {
    let Some(name) = args.first() else {
        return Frame::Error("ERR wrong number of arguments for 'command|getkeys' command".into());
    };
    let Some(command) = command_capability(name) else {
        return Frame::Error("ERR Invalid command specified".to_string());
    };
    let keys = match extract_keys(command, args) {
        Ok(keys) => keys,
        Err(error) => return Frame::Error(error.to_string()),
    };
    Frame::Array(
        keys.into_iter()
            .map(|key| {
                if with_flags {
                    let flag = if command.access == "read" { "RO" } else { "RW" };
                    Frame::Array(vec![
                        Frame::bulk_string(key),
                        Frame::Array(vec![Frame::bulk_string(flag)]),
                    ])
                } else {
                    Frame::bulk_string(key)
                }
            })
            .collect(),
    )
}

fn extract_keys(command: &CommandCapability, args: &[String]) -> Result<Vec<String>, &'static str> {
    match command.name.as_str() {
        "MSET" | "MSETNX" | "MSETEX" => Ok(args.iter().skip(1).step_by(2).cloned().collect()),
        "JSON.MSET" => Ok(args.iter().skip(1).step_by(3).cloned().collect()),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            let count = args
                .get(2)
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or("ERR invalid number of keys")?;
            if args.len() < 3 + count {
                return Err("ERR invalid number of keys");
            }
            Ok(args[3..3 + count].to_vec())
        }
        _ if command.key_spec == "none" => Ok(Vec::new()),
        _ if command.key_spec == "first" => args
            .get(1)
            .cloned()
            .map(|key| vec![key])
            .ok_or("ERR invalid number of arguments specified for command"),
        _ if command.key_spec == "all" => Ok(args.iter().skip(1).cloned().collect()),
        _ => Err("ERR command has dynamic key specifications that require explicit support"),
    }
}

fn simple_glob_matches(pattern: &str, value: &str) -> bool {
    let (mut pattern_index, mut value_index) = (0usize, 0usize);
    let (mut star, mut retry_value) = (None, 0usize);
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star = Some(pattern_index);
            pattern_index += 1;
            retry_value = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            retry_value += 1;
            value_index = retry_value;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_manifest_is_sorted_unique_and_introspection_is_real() {
        let commands = command_capabilities();
        assert!(commands.len() > 200);
        assert!(commands.windows(2).all(|pair| pair[0].name < pair[1].name));
        assert_eq!(command_capability("GET").unwrap().access, "read");
        assert_eq!(command_capability("GET").unwrap().execution_role, "data");
        assert_eq!(command_capability("GET").unwrap().acl_category, "@read");
        assert!(command_capability("GET").unwrap().transaction_allowed);
        assert!(command_capability("GET").unwrap().lua_allowed);
        assert!(!command_capability("BLPOP").unwrap().transaction_allowed);
        assert!(!command_capability("BLPOP").unwrap().lua_allowed);
        assert_eq!(
            command_capability("SAVE").unwrap().compatibility,
            "Unsupported"
        );
        assert!(matches!(
            command_introspection_response(&["COUNT".to_string()]),
            Frame::Integer(count)
                if count == commands.iter().filter(|command| command.compatibility != "Unsupported").count() as i64
        ));
    }

    #[test]
    fn every_parser_route_has_a_capability_entry() {
        let parser = include_str!("parse.rs");
        for line in parser.lines().filter(|line| line.contains("=>")) {
            let Some((route, _)) = line.split_once("=>") else {
                continue;
            };
            for token in route.split('"').skip(1).step_by(2) {
                if !token.is_empty()
                    && token
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && token.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || b"._".contains(&byte)
                    })
                {
                    assert!(
                        command_capability(token).is_some(),
                        "parser route {token} is missing from redis_user_command_compat.json"
                    );
                }
            }
        }
    }
}
