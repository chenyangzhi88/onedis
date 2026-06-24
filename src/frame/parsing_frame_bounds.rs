fn prefixed_length_line_end(bytes: &[u8]) -> Option<usize> {
    for i in 1..bytes.len().min(20) {
        if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
            return Some(i + 2);
        }
    }
    None
}

fn line_frame_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|idx| idx + 2)
}

fn checked_payload_frame_end(header_end: usize, payload_len: usize, bytes_len: usize) -> Option<usize> {
    let frame_end = header_end.checked_add(payload_len)?.checked_add(2)?;
    if frame_end <= bytes_len {
        Some(frame_end)
    } else {
        None
    }
}

fn bulk_frame_end(bytes: &[u8]) -> Option<usize> {
    let line_end = prefixed_length_line_end(bytes)?;
    let line = std::str::from_utf8(&bytes[1..line_end - 2]).ok()?;

    if line == "-1" {
        return Some(line_end);
    }

    let str_len: usize = line.parse().ok()?;
    if str_len > MAX_BULK_STRING_BYTES {
        return None;
    }

    checked_payload_frame_end(line_end, str_len, bytes.len())
}

fn rdb_frame_end(bytes: &[u8]) -> Option<usize> {
    let len_end = prefixed_length_line_end(bytes)?;
    if len_end >= bytes.len() {
        return None;
    }

    let len_str = std::str::from_utf8(&bytes[1..len_end - 2]).ok()?;
    let data_len: usize = len_str.parse().ok()?;
    if data_len > MAX_BULK_STRING_BYTES {
        return None;
    }

    checked_payload_frame_end(len_end, data_len, bytes.len())
}

impl Frame {
    /**
     * 查找单个命令帧的结束位置
     *
     * @param bytes 二进制数据
     */
    pub(crate) fn find_frame_end(bytes: &[u8]) -> Option<usize> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            b'*' => {
                let line_end = prefixed_length_line_end(bytes)?;
                if line_end >= bytes.len() {
                    return None;
                }

                let line = std::str::from_utf8(&bytes[1..line_end - 2]).ok()?;
                let array_len: usize = line.parse().ok()?;
                if array_len > MAX_ARRAY_ELEMENTS {
                    return None;
                }

                let mut current_pos = line_end;
                for _ in 0..array_len {
                    if current_pos >= bytes.len() {
                        return None;
                    }

                    if let Some(element_end) = Frame::find_element_end(&bytes[current_pos..]) {
                        current_pos += element_end;
                    } else {
                        return None;
                    }
                }

                Some(current_pos)
            }
            b'+' | b'-' | b':' => line_frame_end(bytes),
            b'$' => bulk_frame_end(bytes),
            b'~' => rdb_frame_end(bytes),
            _ => Frame::find_inline_frame_end(bytes),
        }
    }

    pub(crate) fn complete_frames_len(bytes: &[u8]) -> usize {
        let mut position = 0;

        while position < bytes.len() {
            if let Some(frame_end) = Frame::find_frame_end(&bytes[position..]) {
                position += frame_end;
            } else {
                break;
            }
        }

        position
    }

    /**
     * 查找元素的结束位置
     *
     * @param bytes 二进制数据
     */
    fn find_element_end(bytes: &[u8]) -> Option<usize> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            b'*' => Frame::find_frame_end(bytes),
            b'+' | b'-' | b':' => line_frame_end(bytes),
            b'$' => bulk_frame_end(bytes),
            _ => None,
        }
    }
}
