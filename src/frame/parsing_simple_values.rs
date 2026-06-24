fn scalar_payload<'a>(bytes: &'a [u8], frame_type: &str) -> Result<&'a [u8], Error> {
    let end = bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| Error::msg(format!("Invalid {}: missing terminator", frame_type)))?;

    Ok(&bytes[1..end])
}

impl Frame {
    /**
     * 简单字符串
     *
     * @param bytes 二进制
     */
    fn parse_simple_string(bytes: &[u8]) -> Result<Frame, Error> {
        let content = String::from_utf8(scalar_payload(bytes, "simple string")?.to_vec())?;
        Ok(Frame::SimpleString(content))
    }

    fn parse_error(bytes: &[u8]) -> Result<Frame, Error> {
        let content = String::from_utf8(scalar_payload(bytes, "error")?.to_vec())?;
        Ok(Frame::Error(content))
    }

    fn parse_integer(bytes: &[u8]) -> Result<Frame, Error> {
        let content = std::str::from_utf8(scalar_payload(bytes, "integer")?)?;
        let value = content.parse::<i64>()?;
        Ok(Frame::Integer(value))
    }
}
