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
            b'+' | b'-' | b':' => {
                let frame_end = bytes
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .map(|position| position + 2)
                    .ok_or_else(|| Error::msg("ERR incomplete line frame"))?;
                let frame = match bytes[0] {
                    b'+' => Frame::parse_simple_string(&bytes[..frame_end])?,
                    b'-' => Frame::parse_error(&bytes[..frame_end])?,
                    b':' => Frame::parse_integer(&bytes[..frame_end])?,
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
            b'*' => Frame::parse_array(bytes, depth),
            b'_' | b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~' | b'>' => {
                Err(Error::msg("ERR unsupported RESP3 frame type"))
            }
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
