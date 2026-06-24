impl Frame {
    fn find_inline_frame_end(bytes: &[u8]) -> Option<usize> {
        bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|idx| idx + 2)
    }

    fn parse_inline_command(bytes: &[u8]) -> Result<Frame, Error> {
        let end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid inline command: missing terminator"))?;
        let line = std::str::from_utf8(&bytes[..end])?;
        let parts = line.split_whitespace().collect::<Vec<_>>();

        if parts.is_empty() {
            return Err(Error::msg("Empty inline command"));
        }

        Ok(Frame::Array(
            parts
                .into_iter()
                .map(|part| Frame::BulkString(part.as_bytes().to_vec()))
                .collect(),
        ))
    }
}
