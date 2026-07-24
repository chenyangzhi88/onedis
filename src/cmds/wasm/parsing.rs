use super::*;

impl WasmCommand {
    pub fn parse_from_frame(frame: Frame) -> Result<Self> {
        let command = frame
            .get_arg(0)
            .ok_or_else(|| Error::msg("ERR empty command"))?
            .to_ascii_uppercase();
        match command.as_str() {
            "WASM.LOAD" => {
                if frame.arg_len() != 3 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.load' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let bytes = frame
                    .get_arg_bytes(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module bytes"))?;
                Ok(Self::Load { name, bytes })
            }
            "WASM.CALL" | "WASM.CALL_RO" => {
                if frame.arg_len() < 3 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.call' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let function = frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm function name"))?;
                let args = frame.get_args_from_index(3);
                Ok(Self::Call {
                    name,
                    function,
                    args,
                    read_only: command == "WASM.CALL_RO",
                    command_name: if command == "WASM.CALL_RO" {
                        "WASM.CALL_RO"
                    } else {
                        "WASM.CALL"
                    },
                })
            }
            "WASM.DEL" => {
                if frame.arg_len() != 2 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.del' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                Ok(Self::Delete { name })
            }
            "WASM.SCAN" => {
                if frame.arg_len() < 4 || frame.arg_len() > 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.scan' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let function = frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm function name"))?;
                let prefix = frame
                    .get_arg(3)
                    .ok_or_else(|| Error::msg("ERR invalid wasm scan prefix"))?;
                let limit = match frame.get_arg(4) {
                    Some(value) => value
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR invalid wasm scan limit"))?,
                    None => 1000,
                };
                Ok(Self::Scan {
                    name,
                    function,
                    prefix,
                    limit,
                })
            }
            "WASM.LIST" => {
                if frame.arg_len() != 1 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.list' command",
                    ));
                }
                Ok(Self::List)
            }
            "FUNCTION" => parse_function_command(frame),
            "FCALL" | "FCALL_RO" => parse_fcall_command(frame, command == "FCALL_RO"),
            _ => Err(Error::msg("ERR unknown wasm command")),
        }
    }
}

fn parse_function_command(frame: Frame) -> Result<WasmCommand> {
    if frame.arg_len() < 2 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'function' command",
        ));
    }
    let subcommand = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid function subcommand"))?
        .to_ascii_uppercase();
    match subcommand.as_str() {
        "LOAD" => {
            if frame.arg_len() != 4 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function load' command",
                ));
            }
            let name = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR invalid function name"))?;
            let bytes = frame
                .get_arg_bytes(3)
                .ok_or_else(|| Error::msg("ERR invalid function payload"))?;
            Ok(WasmCommand::FunctionLoad { name, bytes })
        }
        "DELETE" | "DEL" => {
            if frame.arg_len() != 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function delete' command",
                ));
            }
            let name = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR invalid function name"))?;
            Ok(WasmCommand::FunctionDelete { name })
        }
        "LIST" => {
            if frame.arg_len() != 2 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function list' command",
                ));
            }
            Ok(WasmCommand::FunctionList)
        }
        _ => Err(Error::msg("ERR unsupported function subcommand")),
    }
}

fn parse_fcall_command(frame: Frame, read_only: bool) -> Result<WasmCommand> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'fcall' command",
        ));
    }
    let function_ref = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid function name"))?;
    let numkeys = frame
        .get_arg(2)
        .ok_or_else(|| Error::msg("ERR invalid numkeys"))?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR invalid numkeys"))?;
    if frame.arg_len() < 3 + numkeys {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'fcall' command",
        ));
    }
    let (name, function) = function_ref
        .split_once('.')
        .ok_or_else(|| Error::msg("ERR function name must be module.function"))?;
    let args = frame.get_args_from_index(3);
    Ok(WasmCommand::Call {
        name: name.to_string(),
        function: function.to_string(),
        args,
        read_only,
        command_name: if read_only { "FCALL_RO" } else { "FCALL" },
    })
}
