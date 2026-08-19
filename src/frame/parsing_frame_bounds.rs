#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FrameScanResult {
    Ready(usize),
    Incomplete,
    Invalid(String),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FrameBatchScan {
    pub(crate) complete_len: usize,
    pub(crate) command_count: usize,
    pub(crate) limit_reached: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum FrameBoundary {
    Complete(usize),
    Incomplete,
    Invalid(String),
}

fn parse_protocol_usize(value: &str) -> Option<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<usize>().ok()
}

fn parse_protocol_i64(value: &str) -> Option<i64> {
    let digits = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<i64>().ok()
}

fn prefixed_length_line(bytes: &[u8]) -> Result<Option<(usize, &str)>, String> {
    const MAX_LENGTH_HEADER_BYTES: usize = 20;
    let search_end = bytes.len().min(MAX_LENGTH_HEADER_BYTES);
    if let Some(offset) = bytes[1..search_end]
        .windows(2)
        .position(|window| window == b"\r\n")
    {
        let line_end = 1 + offset + 2;
        let value = std::str::from_utf8(&bytes[1..line_end - 2])
            .map_err(|_| "invalid UTF-8 in length header".to_string())?;
        return Ok(Some((line_end, value)));
    }
    if bytes.len() >= MAX_LENGTH_HEADER_BYTES {
        return Err("length header exceeds protocol limit".to_string());
    }
    Ok(None)
}

fn line_frame_boundary(bytes: &[u8]) -> FrameBoundary {
    let Some(line_end) = bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|idx| idx + 2)
    else {
        return FrameBoundary::Incomplete;
    };
    if line_end > MAX_FRAME_BYTES {
        return FrameBoundary::Invalid("line frame exceeds configured limit".to_string());
    }
    let payload = &bytes[1..line_end - 2];
    match bytes[0] {
        b'+' | b'-' | b',' | b'(' if payload.contains(&b'\r') || payload.contains(&b'\n') => {
            FrameBoundary::Invalid("invalid control character in line frame".to_string())
        }
        b'+' | b'-' | b',' | b'(' if std::str::from_utf8(payload).is_err() => {
            FrameBoundary::Invalid("invalid UTF-8 in line frame".to_string())
        }
        b':' if std::str::from_utf8(payload)
            .ok()
            .and_then(parse_protocol_i64)
            .is_none() =>
        {
            FrameBoundary::Invalid("invalid integer frame".to_string())
        }
        b'#' if !matches!(payload, b"t" | b"f") => {
            FrameBoundary::Invalid("invalid boolean frame".to_string())
        }
        b',' if std::str::from_utf8(payload)
            .ok()
            .filter(|value| matches!(*value, "inf" | "+inf" | "-inf" | "nan") || value.parse::<f64>().is_ok())
            .is_none() =>
        {
            FrameBoundary::Invalid("invalid double frame".to_string())
        }
        b'(' if {
            let digits = payload
                .strip_prefix(b"-")
                .or_else(|| payload.strip_prefix(b"+"))
                .unwrap_or(payload);
            digits.is_empty() || !digits.iter().all(u8::is_ascii_digit)
        } => FrameBoundary::Invalid("invalid big number frame".to_string()),
        b'_' if !payload.is_empty() => FrameBoundary::Invalid("invalid null frame".to_string()),
        _ => FrameBoundary::Complete(line_end),
    }
}

fn payload_frame_boundary(bytes: &[u8], null_allowed: bool, frame_name: &str) -> FrameBoundary {
    let (line_end, line) = match prefixed_length_line(bytes) {
        Ok(Some(header)) => header,
        Ok(None) => return FrameBoundary::Incomplete,
        Err(message) => return FrameBoundary::Invalid(message),
    };
    if null_allowed && line == "-1" {
        return FrameBoundary::Complete(line_end);
    }
    let payload_len = match parse_protocol_usize(line) {
        Some(len) => len,
        None => {
            return FrameBoundary::Invalid(format!("invalid {frame_name} length"));
        }
    };
    if payload_len > MAX_BULK_STRING_BYTES {
        return FrameBoundary::Invalid(format!("{frame_name} exceeds configured limit"));
    }
    let Some(frame_end) = line_end
        .checked_add(payload_len)
        .and_then(|end| end.checked_add(2))
    else {
        return FrameBoundary::Invalid(format!("{frame_name} length overflow"));
    };
    if frame_end > MAX_FRAME_BYTES {
        return FrameBoundary::Invalid(format!("{frame_name} exceeds configured limit"));
    }
    if frame_end > bytes.len() {
        return FrameBoundary::Incomplete;
    }
    if &bytes[frame_end - 2..frame_end] != b"\r\n" {
        return FrameBoundary::Invalid(format!("invalid {frame_name} terminator"));
    }
    FrameBoundary::Complete(frame_end)
}

fn frame_boundary(bytes: &[u8], top_level: bool) -> FrameBoundary {
    let mut remaining_nodes = MAX_FRAME_NODES;
    frame_boundary_with_budget(bytes, top_level, 0, &mut remaining_nodes)
}

fn frame_boundary_with_budget(
    bytes: &[u8],
    top_level: bool,
    depth: usize,
    remaining_nodes: &mut usize,
) -> FrameBoundary {
    if bytes.is_empty() {
        return FrameBoundary::Incomplete;
    }
    let Some(next_remaining) = remaining_nodes.checked_sub(1) else {
        return FrameBoundary::Invalid("frame element count exceeds configured limit".to_string());
    };
    *remaining_nodes = next_remaining;
    match bytes[0] {
        b'*' | b'~' | b'>' | b'%' | b'|' => aggregate_frame_boundary(
            bytes,
            depth,
            remaining_nodes,
            bytes[0],
        ),
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => line_frame_boundary(bytes),
        b'$' => payload_frame_boundary(bytes, true, "bulk string"),
        b'!' => payload_frame_boundary(bytes, false, "blob error"),
        b'=' => payload_frame_boundary(bytes, false, "verbatim string"),
        _ if top_level => Frame::inline_frame_boundary(bytes),
        _ => FrameBoundary::Invalid("invalid array element type".to_string()),
    }
}

fn aggregate_frame_boundary(
    bytes: &[u8],
    depth: usize,
    remaining_nodes: &mut usize,
    prefix: u8,
) -> FrameBoundary {
    if depth >= MAX_ARRAY_NESTING_DEPTH {
        return FrameBoundary::Invalid("aggregate nesting exceeds configured limit".to_string());
    }
    let (line_end, line) = match prefixed_length_line(bytes) {
        Ok(Some(header)) => header,
        Ok(None) => return FrameBoundary::Incomplete,
        Err(message) => return FrameBoundary::Invalid(message),
    };
    if prefix == b'*' && line == "-1" {
        return FrameBoundary::Complete(line_end);
    }
    let logical_len = match parse_protocol_usize(line) {
        Some(len) => len,
        None => return FrameBoundary::Invalid("invalid aggregate length".to_string()),
    };
    if logical_len > MAX_ARRAY_ELEMENTS {
        return FrameBoundary::Invalid("aggregate exceeds configured limit".to_string());
    }
    let multiplier = if matches!(prefix, b'%' | b'|') { 2 } else { 1 };
    let trailing = usize::from(prefix == b'|');
    let Some(element_count) = logical_len
        .checked_mul(multiplier)
        .and_then(|count| count.checked_add(trailing))
    else {
        return FrameBoundary::Invalid("aggregate length overflow".to_string());
    };
    if element_count > MAX_FRAME_NODES {
        return FrameBoundary::Invalid("aggregate element count exceeds configured limit".to_string());
    }
    let mut current_pos = line_end;
    for _ in 0..element_count {
        if current_pos >= bytes.len() {
            return FrameBoundary::Incomplete;
        }
        match frame_boundary_with_budget(
            &bytes[current_pos..],
            false,
            depth + 1,
            remaining_nodes,
        ) {
            FrameBoundary::Complete(element_end) => {
                current_pos += element_end;
                if current_pos > MAX_FRAME_BYTES {
                    return FrameBoundary::Invalid(
                        "aggregate frame exceeds configured limit".to_string(),
                    );
                }
            }
            FrameBoundary::Incomplete => return FrameBoundary::Incomplete,
            FrameBoundary::Invalid(message) => return FrameBoundary::Invalid(message),
        }
    }
    FrameBoundary::Complete(current_pos)
}

impl Frame {
    /**
     * 查找单个命令帧的结束位置
     *
     * @param bytes 二进制数据
     */
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn find_frame_end(bytes: &[u8]) -> Option<usize> {
        match frame_boundary(bytes, true) {
            FrameBoundary::Complete(end) => Some(end),
            FrameBoundary::Incomplete | FrameBoundary::Invalid(_) => None,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn complete_frames_len(bytes: &[u8]) -> usize {
        match Self::scan_complete_frames(bytes) {
            FrameScanResult::Ready(len) => len,
            FrameScanResult::Incomplete | FrameScanResult::Invalid(_) => 0,
        }
    }

    pub(crate) fn scan_complete_frames(bytes: &[u8]) -> FrameScanResult {
        let mut position = 0;
        while position < bytes.len() {
            if bytes[position..].starts_with(b"\r\n") {
                position += 2;
                continue;
            }
            match frame_boundary(&bytes[position..], true) {
                FrameBoundary::Complete(frame_end) => position += frame_end,
                FrameBoundary::Incomplete => {
                    return if position > 0 {
                        FrameScanResult::Ready(position)
                    } else {
                        FrameScanResult::Incomplete
                    };
                }
                FrameBoundary::Invalid(message) => {
                    return if position > 0 {
                        FrameScanResult::Ready(position)
                    } else {
                        FrameScanResult::Invalid(message)
                    };
                }
            }
        }
        if position > 0 {
            FrameScanResult::Ready(position)
        } else {
            FrameScanResult::Incomplete
        }
    }

    pub(crate) fn scan_complete_frames_bounded(
        bytes: &[u8],
        max_commands: usize,
        max_bytes: usize,
    ) -> Result<FrameBatchScan, FrameScanResult> {
        let mut position = 0usize;
        let mut command_count = 0usize;
        while position < bytes.len() {
            if bytes[position..].starts_with(b"\r\n") {
                position += 2;
                continue;
            }
            if command_count >= max_commands || (position >= max_bytes && command_count > 0) {
                return Ok(FrameBatchScan {
                    complete_len: position,
                    command_count,
                    limit_reached: true,
                });
            }
            match frame_boundary(&bytes[position..], true) {
                FrameBoundary::Complete(frame_end) => {
                    if command_count > 0 && position.saturating_add(frame_end) > max_bytes {
                        return Ok(FrameBatchScan {
                            complete_len: position,
                            command_count,
                            limit_reached: true,
                        });
                    }
                    position += frame_end;
                    command_count += 1;
                }
                FrameBoundary::Incomplete => {
                    return if command_count > 0 {
                        Ok(FrameBatchScan {
                            complete_len: position,
                            command_count,
                            limit_reached: false,
                        })
                    } else {
                        Err(FrameScanResult::Incomplete)
                    };
                }
                FrameBoundary::Invalid(message) => {
                    return if command_count > 0 {
                        Ok(FrameBatchScan {
                            complete_len: position,
                            command_count,
                            limit_reached: false,
                        })
                    } else {
                        Err(FrameScanResult::Invalid(message))
                    };
                }
            }
        }
        if command_count > 0 {
            Ok(FrameBatchScan {
                complete_len: position,
                command_count,
                limit_reached: command_count >= max_commands || position >= max_bytes,
            })
        } else {
            Err(FrameScanResult::Incomplete)
        }
    }
}
