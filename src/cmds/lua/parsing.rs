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
