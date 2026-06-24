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
        "DEBUG" => {
            if frame.arg_len() != 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script debug' command",
                ));
            }
            let mode = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .to_ascii_uppercase();
            if !matches!(mode.as_str(), "YES" | "SYNC" | "NO") {
                return Err(Error::msg("ERR syntax error"));
            }
            Ok(LuaCommand::ScriptDebug)
        }
        "HELP" => {
            if frame.arg_len() != 2 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'script help' command",
                ));
            }
            Ok(LuaCommand::ScriptHelp)
        }
        _ => Err(Error::msg("ERR unknown script subcommand")),
    }
}
