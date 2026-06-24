impl Frame {
    /**
     * 数组字符串
     *
     * @param bytes 二进制
     */
    fn parse_array(bytes: &[u8]) -> Result<Frame, Error> {
        let header_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid array: missing length terminator"))?;
        let len_str = std::str::from_utf8(&bytes[1..header_end])?;
        let array_len = len_str.parse::<usize>()?;
        if array_len > MAX_ARRAY_ELEMENTS {
            return Err(Error::msg("ERR array length exceeds configured limit"));
        }

        let mut frames = Vec::with_capacity(array_len);
        let mut current_pos = header_end + 2;

        for _ in 0..array_len {
            let element_end = Frame::find_element_end(&bytes[current_pos..])
                .ok_or_else(|| Error::msg("Incomplete array element"))?;
            let frame = Frame::parse_from_bytes(&bytes[current_pos..current_pos + element_end])?;
            frames.push(frame);
            current_pos += element_end;
        }

        Ok(Frame::Array(frames))
    }
}
