impl Frame {
    fn parse_bulk_string(bytes: &[u8]) -> Result<Frame, Error> {
        let len_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid bulk string: missing length terminator"))?;
        let len_str = std::str::from_utf8(&bytes[1..len_end])?;

        if len_str == "-1" {
            return Ok(Frame::Null);
        }

        let data_len = parse_protocol_usize(len_str)
            .ok_or_else(|| Error::msg("Invalid bulk string length"))?;
        if data_len > MAX_BULK_STRING_BYTES {
            return Err(Error::msg(
                "ERR bulk string length exceeds configured limit",
            ));
        }
        let data_start = len_end
            .checked_add(2)
            .ok_or_else(|| Error::msg("Bulk string header length overflow"))?;
        let data_end = data_start
            .checked_add(data_len)
            .ok_or_else(|| Error::msg("Bulk string data length overflow"))?;
        let frame_end = data_end
            .checked_add(2)
            .ok_or_else(|| Error::msg("Bulk string frame length overflow"))?;

        if bytes.len() < frame_end {
            return Err(Error::msg("Bulk string incomplete"));
        }

        if bytes[data_end] != b'\r' || bytes[data_end + 1] != b'\n' {
            return Err(Error::msg("Invalid bulk string terminator"));
        }

        Ok(Frame::BulkString(bytes[data_start..data_end].to_vec()))
    }
}
