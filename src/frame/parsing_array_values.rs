impl Frame {
    fn parse_array(bytes: &[u8], depth: usize) -> Result<(Frame, usize), Error> {
        if depth >= MAX_ARRAY_NESTING_DEPTH {
            return Err(Error::msg("ERR array nesting exceeds configured limit"));
        }
        let header_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid array: missing length terminator"))?;
        let len_str = std::str::from_utf8(&bytes[1..header_end])?;
        let header_len = header_end
            .checked_add(2)
            .ok_or_else(|| Error::msg("Array header length overflow"))?;
        if len_str == "-1" {
            return Ok((Frame::Null, header_len));
        }
        let array_len =
            parse_protocol_usize(len_str).ok_or_else(|| Error::msg("Invalid array length"))?;
        if array_len > MAX_ARRAY_ELEMENTS {
            return Err(Error::msg("ERR array length exceeds configured limit"));
        }

        let mut frames = Vec::with_capacity(array_len);
        let mut current_pos = header_len;

        for _ in 0..array_len {
            let remaining = bytes
                .get(current_pos..)
                .ok_or_else(|| Error::msg("Incomplete array element"))?;
            let (frame, element_end) = Frame::parse_validated_frame_prefix(remaining, depth + 1)?;
            frames.push(frame);
            current_pos = current_pos
                .checked_add(element_end)
                .ok_or_else(|| Error::msg("Array frame length overflow"))?;
        }

        Ok((Frame::Array(frames), current_pos))
    }
}
