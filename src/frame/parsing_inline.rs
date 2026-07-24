impl Frame {
    fn inline_frame_boundary(bytes: &[u8]) -> FrameBoundary {
        let Some(end) = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|idx| idx + 2)
        else {
            return if bytes.len() >= MAX_FRAME_BYTES {
                FrameBoundary::Invalid("inline frame exceeds configured limit".to_string())
            } else {
                FrameBoundary::Incomplete
            };
        };
        if end > MAX_FRAME_BYTES {
            return FrameBoundary::Invalid("inline frame exceeds configured limit".to_string());
        }
        let line = &bytes[..end - 2];
        if line.contains(&b'\r') || line.contains(&b'\n') {
            return FrameBoundary::Invalid(
                "invalid control character in inline command".to_string(),
            );
        }
        if line.iter().all(|byte| inline_ascii_whitespace(*byte)) {
            return FrameBoundary::Invalid("empty inline command".to_string());
        }
        FrameBoundary::Complete(end)
    }

    fn parse_inline_command(bytes: &[u8]) -> Result<Frame, Error> {
        let end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid inline command: missing terminator"))?;
        let parts = parse_inline_arguments(&bytes[..end])?;

        if parts.is_empty() {
            return Err(Error::msg("Empty inline command"));
        }

        Ok(Frame::Array(
            parts.into_iter().map(Frame::BulkString).collect(),
        ))
    }
}

fn inline_ascii_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | 0x0b | 0x0c)
}

fn inline_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_inline_arguments(line: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
    let mut arguments = Vec::new();
    let mut index = 0usize;
    while index < line.len() {
        while index < line.len() && inline_ascii_whitespace(line[index]) {
            index += 1;
        }
        if index == line.len() {
            break;
        }

        let mut argument = Vec::new();
        let mut double_quoted = false;
        let mut single_quoted = false;
        loop {
            if index == line.len() {
                if double_quoted || single_quoted {
                    return Err(Error::msg("ERR unbalanced quotes in inline request"));
                }
                break;
            }
            let byte = line[index];
            if double_quoted {
                match byte {
                    b'\\' if index + 1 < line.len() => {
                        if line[index + 1] == b'x'
                            && index + 3 < line.len()
                            && let (Some(high), Some(low)) = (
                                inline_hex_value(line[index + 2]),
                                inline_hex_value(line[index + 3]),
                            )
                        {
                            argument.push((high << 4) | low);
                            index += 4;
                            continue;
                        }
                        let escaped = match line[index + 1] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => other,
                        };
                        argument.push(escaped);
                        index += 2;
                    }
                    b'\\' => {
                        return Err(Error::msg("ERR invalid escape in inline request"));
                    }
                    b'"' => {
                        index += 1;
                        if index < line.len() && !inline_ascii_whitespace(line[index]) {
                            return Err(Error::msg(
                                "ERR quoted argument must be followed by whitespace",
                            ));
                        }
                        break;
                    }
                    other => {
                        argument.push(other);
                        index += 1;
                    }
                }
            } else if single_quoted {
                match byte {
                    b'\\' if index + 1 < line.len() && line[index + 1] == b'\'' => {
                        argument.push(b'\'');
                        index += 2;
                    }
                    b'\'' => {
                        index += 1;
                        if index < line.len() && !inline_ascii_whitespace(line[index]) {
                            return Err(Error::msg(
                                "ERR quoted argument must be followed by whitespace",
                            ));
                        }
                        break;
                    }
                    other => {
                        argument.push(other);
                        index += 1;
                    }
                }
            } else {
                match byte {
                    b'"' => {
                        double_quoted = true;
                        index += 1;
                    }
                    b'\'' => {
                        single_quoted = true;
                        index += 1;
                    }
                    byte if inline_ascii_whitespace(byte) => break,
                    b'\r' | b'\n' => {
                        return Err(Error::msg(
                            "ERR invalid control character in inline request",
                        ));
                    }
                    other => {
                        argument.push(other);
                        index += 1;
                    }
                }
            }
        }
        arguments.push(argument);
        if arguments.len() > MAX_ARRAY_ELEMENTS {
            return Err(Error::msg(
                "ERR inline argument count exceeds configured limit",
            ));
        }
        while index < line.len() && inline_ascii_whitespace(line[index]) {
            index += 1;
        }
    }
    Ok(arguments)
}
