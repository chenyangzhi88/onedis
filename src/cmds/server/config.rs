use anyhow::Error;
use std::collections::HashSet;

use crate::{args::ResolvedArgs, frame::Frame};

pub struct Config {
    subcommand: ConfigSubcommand,
}

enum ConfigSubcommand {
    Get(Vec<String>),
    Help,
}

impl Config {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'config' command",
            ));
        }

        let subcommand = match args[1].to_ascii_uppercase().as_str() {
            "GET" => {
                if args.len() < 3 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'config|get' command",
                    ));
                }
                ConfigSubcommand::Get(args.iter().skip(2).map(|s| s.to_lowercase()).collect())
            }
            "HELP" => {
                if args.len() != 2 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'config|help' command",
                    ));
                }
                ConfigSubcommand::Help
            }
            other => {
                return Err(Error::msg(format!(
                    "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                    other.to_lowercase()
                )));
            }
        };

        Ok(Self { subcommand })
    }

    pub fn apply(self, args: &ResolvedArgs) -> Result<Frame, Error> {
        match self.subcommand {
            ConfigSubcommand::Get(patterns) => Ok(Frame::Array(config_get(args, &patterns))),
            ConfigSubcommand::Help => Ok(Frame::Array(config_help())),
        }
    }
}

fn config_help() -> Vec<Frame> {
    [
        "GET <pattern> [<pattern> ...] -- Return parameters matching glob-like patterns.",
        "HELP -- Print this help.",
    ]
    .into_iter()
    .map(|line| Frame::bulk_string(line.to_string()))
    .collect()
}

fn config_get(args: &ResolvedArgs, patterns: &[String]) -> Vec<Frame> {
    let entries = config_entries(args);
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for (name, value) in entries {
        if patterns.iter().any(|pattern| glob_match(pattern, &name)) && seen.insert(name.clone()) {
            result.push(Frame::bulk_string(name));
            result.push(Frame::bulk_string(value));
        }
    }

    result
}

fn config_entries(args: &ResolvedArgs) -> Vec<(String, String)> {
    let requirepass = args.requirepass.clone().unwrap_or_default();

    vec![
        ("appendonly".to_string(), "no".to_string()),
        ("bind".to_string(), args.bind.clone()),
        ("databases".to_string(), args.databases.to_string()),
        ("dbfilename".to_string(), "".to_string()),
        ("hz".to_string(), trim_trailing_zero(args.hz)),
        ("loglevel".to_string(), args.loglevel.clone()),
        ("maxclients".to_string(), args.maxclients.to_string()),
        ("port".to_string(), args.port.to_string()),
        ("replicaof".to_string(), String::new()),
        ("requirepass".to_string(), requirepass),
        ("save".to_string(), "".to_string()),
        ("slaveof".to_string(), String::new()),
    ]
}

fn trim_trailing_zero(value: f64) -> String {
    let mut text = value.to_string();
    if let Some(dot) = text.find('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.len() == dot + 1 {
            text.pop();
        }
    }
    text
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    glob_match_impl(&pattern_chars, 0, &text_chars, 0)
}

fn glob_match_impl(pattern: &[char], pi: usize, text: &[char], ti: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }

    match pattern[pi] {
        '*' => {
            let mut next_ti = ti;
            while next_ti <= text.len() {
                if glob_match_impl(pattern, pi + 1, text, next_ti) {
                    return true;
                }
                next_ti += 1;
            }
            false
        }
        '?' => {
            if ti == text.len() {
                false
            } else {
                glob_match_impl(pattern, pi + 1, text, ti + 1)
            }
        }
        ch => {
            if ti < text.len() && ch == text[ti] {
                glob_match_impl(pattern, pi + 1, text, ti + 1)
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use crate::{args::ResolvedArgs, frame::Frame};

    fn sample_args() -> ResolvedArgs {
        ResolvedArgs {
            config: "config/onedis.toml".to_string(),
            requirepass: None,
            bind: "127.0.0.1".to_string(),
            databases: 16,
            hz: 10.0,
            port: 6379,
            loglevel: "info".to_string(),
            maxclients: 0,
            observability_enabled: false,
            metrics_bind: "127.0.0.1".to_string(),
            metrics_port: 0,
            slow_command_threshold_ms: 10,
        }
    }

    #[test]
    fn config_get_returns_exact_match() {
        let command = Config::parse_from_frame(Frame::Array(vec![
            Frame::bulk_string("CONFIG"),
            Frame::bulk_string("GET"),
            Frame::bulk_string("port"),
        ]))
        .unwrap();

        let Frame::Array(values) = command.apply(&sample_args()).unwrap() else {
            panic!("expected array reply");
        };

        assert_eq!(values.len(), 2);
        assert!(matches!(&values[0], Frame::BulkString(name) if name.as_slice() == b"port"));
        assert!(matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"6379"));
    }

    #[test]
    fn config_get_supports_multiple_patterns_without_duplicates() {
        let command = Config::parse_from_frame(Frame::Array(vec![
            Frame::bulk_string("CONFIG"),
            Frame::bulk_string("GET"),
            Frame::bulk_string("*pass"),
            Frame::bulk_string("require*"),
        ]))
        .unwrap();

        let Frame::Array(values) = command.apply(&sample_args()).unwrap() else {
            panic!("expected array reply");
        };

        assert_eq!(values.len(), 2);
        assert!(matches!(&values[0], Frame::BulkString(name) if name.as_slice() == b"requirepass"));
    }

    #[test]
    fn config_get_returns_empty_array_for_unknown_pattern() {
        let command = Config::parse_from_frame(Frame::Array(vec![
            Frame::bulk_string("CONFIG"),
            Frame::bulk_string("GET"),
            Frame::bulk_string("unknown-setting"),
        ]))
        .unwrap();

        let Frame::Array(values) = command.apply(&sample_args()).unwrap() else {
            panic!("expected array reply");
        };

        assert!(values.is_empty());
    }

    #[test]
    fn config_get_returns_standard_redis_persistence_fields() {
        let command = Config::parse_from_frame(Frame::Array(vec![
            Frame::bulk_string("CONFIG"),
            Frame::bulk_string("GET"),
            Frame::bulk_string("save"),
            Frame::bulk_string("appendonly"),
        ]))
        .unwrap();

        let Frame::Array(values) = command.apply(&sample_args()).unwrap() else {
            panic!("expected array reply");
        };

        assert_eq!(values.len(), 4);
        assert!(matches!(&values[0], Frame::BulkString(name) if name.as_slice() == b"appendonly"));
        assert!(matches!(&values[1], Frame::BulkString(value) if value.as_slice() == b"no"));
        assert!(matches!(&values[2], Frame::BulkString(name) if name.as_slice() == b"save"));
        assert!(matches!(&values[3], Frame::BulkString(value) if value.is_empty()));
    }
}
