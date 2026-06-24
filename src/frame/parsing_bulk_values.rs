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

        let data_len = len_str.parse::<usize>()?;
        if data_len > MAX_BULK_STRING_BYTES {
            return Err(Error::msg(
                "ERR bulk string length exceeds configured limit",
            ));
        }
        let data_start = len_end + 2;
        let data_end = data_start + data_len;

        if bytes.len() < data_end + 2 {
            return Err(Error::msg("Bulk string incomplete"));
        }

        if bytes[data_end] != b'\r' || bytes[data_end + 1] != b'\n' {
            return Err(Error::msg("Invalid bulk string terminator"));
        }

        Ok(Frame::BulkString(bytes[data_start..data_end].to_vec()))
    }

    /**
     * 正确解析 RDB 文件帧
     *
     * @param bytes 二进制
     */
    fn parse_rdb_file(bytes: &[u8]) -> Result<Frame, Error> {
        let len_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid RDB format: missing length terminator"))?;

        let len_str = std::str::from_utf8(&bytes[1..len_end])
            .map_err(|e| Error::msg(format!("Invalid UTF-8: {}", e)))?;

        let data_len = len_str.parse::<usize>().map_err(|e| {
            Error::msg(format!("Invalid RDB length: {} ({})", len_str, e))
        })?;
        if data_len > MAX_BULK_STRING_BYTES {
            return Err(Error::msg("ERR RDB length exceeds configured limit"));
        }

        let data_start = len_end + 2;
        let data_end = data_start
            .checked_add(data_len)
            .ok_or_else(|| Error::msg("RDB data length overflow"))?;
        let frame_end = data_end
            .checked_add(2)
            .ok_or_else(|| Error::msg("RDB frame length overflow"))?;

        if bytes.len() < frame_end {
            return Err(Error::msg(format!(
                "RDB data incomplete: expected {} bytes, got {}",
                frame_end,
                bytes.len()
            )));
        }

        if bytes[data_end] != b'\r' || bytes[data_end + 1] != b'\n' {
            return Err(Error::msg("Invalid RDB terminator"));
        }

        Ok(Frame::RDBFile(bytes[data_start..data_end].to_vec()))
    }
}
