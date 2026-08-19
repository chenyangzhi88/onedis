impl Frame {
    pub fn parse_from_bytes(bytes: &[u8]) -> Result<Frame, Error> {
        if bytes.is_empty() {
            return Err(Error::msg("Empty frame"));
        }

        match frame_boundary(bytes, true) {
            FrameBoundary::Complete(frame_end) if frame_end == bytes.len() => {}
            FrameBoundary::Complete(_) => {
                return Err(Error::msg("ERR trailing data after protocol frame"));
            }
            FrameBoundary::Incomplete => {
                return Err(Error::msg("ERR incomplete protocol frame"));
            }
            FrameBoundary::Invalid(message) => return Err(Error::msg(message)),
        }
        Frame::parse_validated_frame(bytes, 0)
    }

    fn parse_validated_frame(bytes: &[u8], depth: usize) -> Result<Frame, Error> {
        let (frame, consumed) = Frame::parse_validated_frame_prefix(bytes, depth)?;
        if consumed != bytes.len() {
            return Err(Error::msg("ERR trailing data after protocol frame"));
        }
        Ok(frame)
    }

    fn parse_validated_frame_prefix(bytes: &[u8], depth: usize) -> Result<(Frame, usize), Error> {
        if bytes.is_empty() {
            return Err(Error::msg("Empty frame"));
        }
        match bytes[0] {
            b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => {
                let frame_end = bytes
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .map(|position| position + 2)
                    .ok_or_else(|| Error::msg("ERR incomplete line frame"))?;
                let frame = match bytes[0] {
                    b'+' => Frame::parse_simple_string(&bytes[..frame_end])?,
                    b'-' => Frame::parse_error(&bytes[..frame_end])?,
                    b':' => Frame::parse_integer(&bytes[..frame_end])?,
                    b'_' => Frame::Null,
                    b'#' => Frame::Boolean(bytes.get(1) == Some(&b't')),
                    b',' => Frame::Double(parse_resp3_double(&bytes[1..frame_end - 2])?),
                    b'(' => Frame::BigNumber(
                        std::str::from_utf8(&bytes[1..frame_end - 2])?.to_string(),
                    ),
                    _ => unreachable!(),
                };
                Ok((frame, frame_end))
            }
            b'$' => {
                let frame_end = match payload_frame_boundary(bytes, true, "bulk string") {
                    FrameBoundary::Complete(frame_end) => frame_end,
                    FrameBoundary::Incomplete => {
                        return Err(Error::msg("ERR incomplete bulk string"));
                    }
                    FrameBoundary::Invalid(message) => return Err(Error::msg(message)),
                };
                Ok((Frame::parse_bulk_string(&bytes[..frame_end])?, frame_end))
            }
            b'!' => parse_resp3_blob(bytes, false),
            b'=' => parse_resp3_blob(bytes, true),
            b'*' | b'%' | b'~' | b'|' | b'>' => Frame::parse_aggregate(bytes, depth),
            _ => {
                let frame_end = bytes
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .map(|position| position + 2)
                    .ok_or_else(|| Error::msg("ERR incomplete inline frame"))?;
                Ok((Frame::parse_inline_command(&bytes[..frame_end])?, frame_end))
            }
        }
    }

    /**
     * 解析粘连的多个命令帧
     *
     * @param bytes 二进制数据
     */
    pub fn parse_multiple_frames(bytes: &[u8]) -> Result<Vec<Frame>, Error> {
        let mut frames = Vec::new();
        let mut position = 0;

        while position < bytes.len() {
            if bytes[position..].starts_with(b"\r\n") {
                position += 2;
                continue;
            }
            match frame_boundary(&bytes[position..], true) {
                FrameBoundary::Complete(frame_end) => {
                    let frame_bytes = &bytes[position..position + frame_end];
                    let frame = Frame::parse_validated_frame(frame_bytes, 0)?;
                    frames.push(frame);
                    position += frame_end;
                }
                FrameBoundary::Incomplete => break,
                FrameBoundary::Invalid(message) => return Err(Error::msg(message)),
            }
        }

        Ok(frames)
    }
}

fn parse_resp3_double(bytes: &[u8]) -> Result<f64, Error> {
    match std::str::from_utf8(bytes)? {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        value => value
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR invalid RESP3 double")),
    }
}

fn parse_resp3_blob(bytes: &[u8], verbatim: bool) -> Result<(Frame, usize), Error> {
    let header_end = bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| Error::msg("ERR incomplete RESP3 blob header"))?;
    let payload_len = parse_protocol_usize(std::str::from_utf8(&bytes[1..header_end])?)
        .ok_or_else(|| Error::msg("ERR invalid RESP3 blob length"))?;
    let data_start = header_end + 2;
    let data_end = data_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::msg("ERR RESP3 blob length overflow"))?;
    let frame_end = data_end
        .checked_add(2)
        .ok_or_else(|| Error::msg("ERR RESP3 blob length overflow"))?;
    let payload = bytes
        .get(data_start..data_end)
        .ok_or_else(|| Error::msg("ERR incomplete RESP3 blob"))?;
    if verbatim {
        if payload.len() < 4 || payload[3] != b':' {
            return Err(Error::msg("ERR invalid RESP3 verbatim string"));
        }
        let mut format = [0_u8; 3];
        format.copy_from_slice(&payload[..3]);
        Ok((
            Frame::VerbatimString {
                format,
                data: payload[4..].to_vec(),
            },
            frame_end,
        ))
    } else {
        Ok((Frame::BlobError(payload.to_vec()), frame_end))
    }
}
