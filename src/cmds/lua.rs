use anyhow::{Error, Result};

use crate::{
    frame::Frame,
    lua::{LuaEval, lua_registry},
    store::db::Db,
};

pub enum LuaCommand {
    Eval(LuaEval),
    EvalSha {
        sha: String,
        keys: Vec<String>,
        args: Vec<String>,
        read_only: bool,
    },
    ScriptLoad(String),
    ScriptExists(Vec<String>),
    ScriptFlush,
    ScriptKill,
    ScriptDebug,
    ScriptHelp,
}

impl LuaCommand {
    pub fn parse_from_frame(frame: Frame) -> Result<Self> {
        let command = frame
            .get_arg(0)
            .ok_or_else(|| Error::msg("ERR empty command"))?
            .to_ascii_uppercase();
        match command.as_str() {
            "EVAL" | "EVAL_RO" => {
                let (script, keys, args) = parse_eval_args(&frame, "eval")?;
                Ok(Self::Eval(LuaEval {
                    script,
                    keys,
                    args,
                    read_only: command == "EVAL_RO",
                }))
            }
            "EVALSHA" | "EVALSHA_RO" => {
                let (sha, keys, args) = parse_eval_args(&frame, "evalsha")?;
                Ok(Self::EvalSha {
                    sha,
                    keys,
                    args,
                    read_only: command == "EVALSHA_RO",
                })
            }
            "SCRIPT" => parse_script_command(frame),
            _ => Err(Error::msg("ERR unknown lua command")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame> {
        match self {
            Self::Eval(eval) => lua_registry().eval(db, eval),
            Self::EvalSha {
                sha,
                keys,
                args,
                read_only,
            } => {
                let script = lua_registry()
                    .get(&sha)?
                    .ok_or_else(|| Error::msg("NOSCRIPT No matching script. Please use EVAL."))?;
                lua_registry().eval(
                    db,
                    LuaEval {
                        script,
                        keys,
                        args,
                        read_only,
                    },
                )
            }
            Self::ScriptLoad(script) => Ok(Frame::bulk_string(lua_registry().load(&script)?)),
            Self::ScriptExists(shas) => Ok(Frame::Array(
                lua_registry()
                    .exists(&shas)?
                    .into_iter()
                    .map(|exists| Frame::Integer(i64::from(exists)))
                    .collect(),
            )),
            Self::ScriptFlush => {
                lua_registry().flush()?;
                Ok(Frame::Ok)
            }
            Self::ScriptKill => {
                lua_registry().kill()?;
                Ok(Frame::Ok)
            }
            Self::ScriptDebug => Ok(Frame::Ok),
            Self::ScriptHelp => Ok(Frame::Array(vec![
                Frame::bulk_string("SCRIPT LOAD script"),
                Frame::bulk_string("SCRIPT EXISTS sha [sha ...]"),
                Frame::bulk_string("SCRIPT FLUSH [ASYNC|SYNC]"),
                Frame::bulk_string("SCRIPT KILL"),
                Frame::bulk_string("SCRIPT DEBUG YES|SYNC|NO"),
            ])),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame> {
        self.apply(db)
    }
}

fn parse_eval_args(
    frame: &Frame,
    command: &'static str,
) -> Result<(String, Vec<String>, Vec<String>)> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    let script = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid script"))?;
    let numkeys = frame
        .get_arg(2)
        .ok_or_else(|| Error::msg("ERR invalid numkeys"))?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
    if frame.arg_len() < 3 + numkeys {
        return Err(Error::msg(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut keys = Vec::with_capacity(numkeys);
    for idx in 0..numkeys {
        keys.push(
            frame
                .get_arg(3 + idx)
                .ok_or_else(|| Error::msg("ERR invalid key"))?,
        );
    }
    let args = (3 + numkeys..frame.arg_len())
        .map(|idx| {
            frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR invalid argument"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((script, keys, args))
}

fn parse_script_command(frame: Frame) -> Result<LuaCommand> {
    if frame.arg_len() < 2 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'script' command",
        ));
    }
    let subcommand = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid script subcommand"))?
        .to_ascii_uppercase();
    match subcommand.as_str() {
        "LOAD" => {
            if frame.arg_len() != 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script load' command",
                ));
            }
            Ok(LuaCommand::ScriptLoad(
                frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR invalid script"))?,
            ))
        }
        "EXISTS" => {
            if frame.arg_len() < 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script exists' command",
                ));
            }
            Ok(LuaCommand::ScriptExists(
                (2..frame.arg_len())
                    .map(|idx| {
                        frame
                            .get_arg(idx)
                            .ok_or_else(|| Error::msg("ERR invalid script sha"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
        "FLUSH" => {
            if frame.arg_len() > 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script flush' command",
                ));
            }
            if frame.arg_len() == 3 {
                let mode = frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR syntax error"))?
                    .to_ascii_uppercase();
                if !matches!(mode.as_str(), "SYNC" | "ASYNC") {
                    return Err(Error::msg("ERR syntax error"));
                }
            }
            Ok(LuaCommand::ScriptFlush)
        }
        "KILL" => {
            if frame.arg_len() != 2 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script kill' command",
                ));
            }
            Ok(LuaCommand::ScriptKill)
        }
        "DEBUG" => Ok(LuaCommand::ScriptDebug),
        "HELP" => Ok(LuaCommand::ScriptHelp),
        _ => Err(Error::msg("ERR unknown script subcommand")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        db::Db,
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    };
    use std::{path::PathBuf, sync::Arc, time::SystemTime};

    fn frame(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn test_db(prefix: &str) -> Db {
        let unique = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"))
            .join(format!("{prefix}-{unique}"));
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    #[test]
    fn lua_parser_covers_eval_evalsha_script_and_errors() {
        match LuaCommand::parse_from_frame(frame(&[
            "EVAL",
            "return KEYS[1] .. ARGV[1]",
            "1",
            "k",
            "a",
        ]))
        .unwrap()
        {
            LuaCommand::Eval(eval) => {
                assert_eq!(eval.script, "return KEYS[1] .. ARGV[1]");
                assert_eq!(eval.keys, vec!["k".to_string()]);
                assert_eq!(eval.args, vec!["a".to_string()]);
                assert!(!eval.read_only);
            }
            _ => panic!("expected EVAL"),
        }
        match LuaCommand::parse_from_frame(frame(&["EVAL_RO", "return KEYS[1]", "1", "k"])).unwrap()
        {
            LuaCommand::Eval(eval) => assert!(eval.read_only),
            _ => panic!("expected EVAL_RO"),
        }
        match LuaCommand::parse_from_frame(frame(&["EVALSHA_RO", "abc", "0", "arg"])).unwrap() {
            LuaCommand::EvalSha {
                sha,
                keys,
                args,
                read_only,
            } => {
                assert_eq!(sha, "abc");
                assert!(keys.is_empty());
                assert_eq!(args, vec!["arg".to_string()]);
                assert!(read_only);
            }
            _ => panic!("expected EVALSHA_RO"),
        }

        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "LOAD", "return 1"])).unwrap(),
            LuaCommand::ScriptLoad(_)
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "EXISTS", "a", "b"])).unwrap(),
            LuaCommand::ScriptExists(shas) if shas == vec!["a".to_string(), "b".to_string()]
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH"])).unwrap(),
            LuaCommand::ScriptFlush
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH", "ASYNC"])).unwrap(),
            LuaCommand::ScriptFlush
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "KILL"])).unwrap(),
            LuaCommand::ScriptKill
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "DEBUG", "YES"])).unwrap(),
            LuaCommand::ScriptDebug
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "HELP"])).unwrap(),
            LuaCommand::ScriptHelp
        ));

        for args in [
            vec!["EVAL", "return 1"],
            vec!["EVAL", "return 1", "bad"],
            vec!["EVAL", "return 1", "2", "only-one-key"],
            vec!["EVALSHA"],
            vec!["SCRIPT"],
            vec!["SCRIPT", "LOAD"],
            vec!["SCRIPT", "EXISTS"],
            vec!["SCRIPT", "FLUSH", "BAD"],
            vec!["SCRIPT", "FLUSH", "SYNC", "extra"],
            vec!["SCRIPT", "KILL", "extra"],
            vec!["SCRIPT", "NOPE"],
            vec!["UNKNOWN"],
        ] {
            assert!(
                LuaCommand::parse_from_frame(frame(&args)).is_err(),
                "{args:?} should fail"
            );
        }
    }

    #[test]
    fn lua_command_apply_covers_script_cache_and_help_paths() {
        let db = test_db("onedis-lua-cmd");
        let sha = match LuaCommand::parse_from_frame(frame(&["SCRIPT", "LOAD", "return 'cached'"]))
            .unwrap()
            .apply(&db)
            .unwrap()
        {
            Frame::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected sha bulk, got {}", other.to_string()),
        };
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "EXISTS", &sha, "missing"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Array(values) if matches!(values.as_slice(), [Frame::Integer(1), Frame::Integer(0)])
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["EVALSHA", &sha, "0"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::BulkString(bytes) if bytes == b"cached"
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "HELP"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Array(values) if !values.is_empty()
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "DEBUG", "NO"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Ok
        ));
        assert!(matches!(
            LuaCommand::parse_from_frame(frame(&["SCRIPT", "FLUSH", "SYNC"]))
                .unwrap()
                .apply(&db)
                .unwrap(),
            Frame::Ok
        ));
        assert!(
            LuaCommand::parse_from_frame(frame(&["EVALSHA", &sha, "0"]))
                .unwrap()
                .apply(&db)
                .is_err()
        );
    }
}
