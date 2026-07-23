impl Frame {
    /**
     * 通过解析 bytes 创建命令帧
     *
     * @param bytes 二进制
     */
    pub fn parse_from_bytes(bytes: &[u8]) -> Result<Frame, Error> {
        if bytes.is_empty() {
            return Err(Error::msg("Empty frame"));
        }

        match bytes[0] {
            b'+' => Frame::parse_simple_string(bytes),
            b'-' => Frame::parse_error(bytes),
            b':' => Frame::parse_integer(bytes),
            b'$' => Frame::parse_bulk_string(bytes),
            b'~' => Frame::parse_rdb_file(bytes),
            b'*' => Frame::parse_array(bytes),
            _ => Frame::parse_inline_command(bytes),
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
                    let frame = Frame::parse_from_bytes(frame_bytes)?;
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
