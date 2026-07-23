#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FrameScanResult {
    Ready(usize),
    Incomplete,
    Invalid(String),
}

#[derive(Debug, Eq, PartialEq)]
enum FrameBoundary {
    Complete(usize),
    Incomplete,
    Invalid(String),
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
    let payload = &bytes[1..line_end - 2];
    match bytes[0] {
        b'+' | b'-' if std::str::from_utf8(payload).is_err() => {
            FrameBoundary::Invalid("invalid UTF-8 in line frame".to_string())
        }
        b':' if std::str::from_utf8(payload)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .is_none() =>
        {
            FrameBoundary::Invalid("invalid integer frame".to_string())
        }
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
    let payload_len = match line.parse::<usize>() {
        Ok(len) => len,
        Err(_) => {
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
    if frame_end > bytes.len() {
        return FrameBoundary::Incomplete;
    }
    if &bytes[frame_end - 2..frame_end] != b"\r\n" {
        return FrameBoundary::Invalid(format!("invalid {frame_name} terminator"));
    }
    FrameBoundary::Complete(frame_end)
}

fn frame_boundary(bytes: &[u8], top_level: bool) -> FrameBoundary {
    if bytes.is_empty() {
        return FrameBoundary::Incomplete;
    }
    match bytes[0] {
        b'*' => {
            let (line_end, line) = match prefixed_length_line(bytes) {
                Ok(Some(header)) => header,
                Ok(None) => return FrameBoundary::Incomplete,
                Err(message) => return FrameBoundary::Invalid(message),
            };
            let array_len = match line.parse::<usize>() {
                Ok(len) => len,
                Err(_) => return FrameBoundary::Invalid("invalid array length".to_string()),
            };
            if array_len > MAX_ARRAY_ELEMENTS {
                return FrameBoundary::Invalid("array exceeds configured limit".to_string());
            }
            let mut current_pos = line_end;
            for _ in 0..array_len {
                if current_pos >= bytes.len() {
                    return FrameBoundary::Incomplete;
                }
                match frame_boundary(&bytes[current_pos..], false) {
                    FrameBoundary::Complete(element_end) => current_pos += element_end,
                    FrameBoundary::Incomplete => return FrameBoundary::Incomplete,
                    FrameBoundary::Invalid(message) => return FrameBoundary::Invalid(message),
                }
            }
            FrameBoundary::Complete(current_pos)
        }
        b'+' | b'-' | b':' => line_frame_boundary(bytes),
        b'$' => payload_frame_boundary(bytes, true, "bulk string"),
        b'~' => payload_frame_boundary(bytes, false, "RDB frame"),
        _ if top_level => match Frame::find_inline_frame_end(bytes) {
            Some(end) => FrameBoundary::Complete(end),
            None => FrameBoundary::Incomplete,
        },
        _ => FrameBoundary::Invalid("invalid array element type".to_string()),
    }
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

    /**
     * 查找元素的结束位置
     *
     * @param bytes 二进制数据
     */
    fn find_element_end(bytes: &[u8]) -> Option<usize> {
        match frame_boundary(bytes, false) {
            FrameBoundary::Complete(end) => Some(end),
            FrameBoundary::Incomplete | FrameBoundary::Invalid(_) => None,
        }
    }
}
