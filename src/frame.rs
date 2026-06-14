use anyhow::Error;

pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_BULK_STRING_BYTES: usize = MAX_FRAME_BYTES;
pub(crate) const MAX_ARRAY_ELEMENTS: usize = 1_000_000;

/*
 * 命令帧枚举
 */
#[derive(Clone)]
pub enum Frame {
    Ok,
    Integer(i64),
    RDBFile(Vec<u8>),
    SimpleString(String),
    Array(Vec<Frame>),
    BulkString(Vec<u8>),
    Error(String),
    Null,
}

impl Frame {
    /**
     * 将 frame 转化为字符串
     *
     * @param self 本身
     */
    pub fn to_string(&self) -> String {
        match self {
            Frame::Ok => String::from("OK"),
            Frame::Integer(i) => i.to_string(),
            Frame::RDBFile(data) => format!("[RDBFile {} bytes]", data.len()),
            Frame::SimpleString(s) => s.clone(),
            Frame::BulkString(s) => String::from_utf8_lossy(s).into_owned(),
            Frame::Error(e) => e.clone(),
            Frame::Null => String::new(),
            Frame::Array(arr) => {
                let mut result = String::new();
                for item in arr {
                    result.push_str(&item.to_string());
                    result.push(' ');
                }
                result.trim_end().to_string()
            }
        }
    }

    /**
     * 将 frame 转换为 bytes
     *
     * @param self 本身
     */
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Frame::Ok => b"+OK\r\n".to_vec(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::Error(e) => format!("-{}\r\n", e).into_bytes(),
            Frame::Null => b"$-1\r\n".to_vec(),
            Frame::RDBFile(data) => {
                let mut bytes = format!("~{}\r\n", data.len()).into_bytes();
                bytes.extend(data);
                bytes.extend(b"\r\n");
                bytes
            }
            Frame::Array(arr) => {
                let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
                for item in arr {
                    bytes.extend(item.as_bytes());
                }
                bytes
            }
            Frame::BulkString(s) => {
                let mut bytes = format!("${}\r\n", s.len()).into_bytes();
                bytes.extend(s);
                bytes.extend(b"\r\n");
                bytes
            }
        }
    }

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
            // 查找下一个完整的命令帧
            if let Some(frame_end) = Frame::find_frame_end(&bytes[position..]) {
                let frame_bytes = &bytes[position..position + frame_end];
                let frame = Frame::parse_from_bytes(frame_bytes)?;
                frames.push(frame);
                position += frame_end;
            } else {
                break;
            }
        }

        Ok(frames)
    }

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
                // 数组类型的帧
                // 首先找到数组长度行的结束位置
                let mut line_end = None;
                for i in 1..bytes.len().min(20) {
                    // 限制搜索范围，防止过长的第一行
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        line_end = Some(i + 2);
                        break;
                    }
                }

                let line_end = line_end?;
                if line_end >= bytes.len() {
                    return None;
                }

                // 解析数组长度
                let line = std::str::from_utf8(&bytes[1..line_end - 2]).ok()?;
                let array_len: usize = line.parse().ok()?;
                if array_len > MAX_ARRAY_ELEMENTS {
                    return None;
                }

                // 计算数组元素的结束位置
                let mut current_pos = line_end;
                for _ in 0..array_len {
                    if current_pos >= bytes.len() {
                        return None;
                    }

                    // 查找当前元素的结束位置
                    if let Some(element_end) = Frame::find_element_end(&bytes[current_pos..]) {
                        current_pos += element_end;
                    } else {
                        return None;
                    }
                }

                Some(current_pos)
            }
            b'+' | b'-' | b':' => {
                // 简单字符串、错误、整数类型
                for i in 1..bytes.len() {
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        return Some(i + 2);
                    }
                }
                None
            }
            b'$' => {
                // 批量字符串类型
                // 找到长度行的结束
                let mut line_end = None;
                for i in 1..bytes.len().min(20) {
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        line_end = Some(i + 2);
                        break;
                    }
                }

                let line_end = line_end?;

                // 解析字符串长度
                let line = std::str::from_utf8(&bytes[1..line_end - 2]).ok()?;
                if line == "-1" {
                    // NULL批量字符串
                    return Some(line_end);
                }

                let str_len: usize = line.parse().ok()?;
                if str_len > MAX_BULK_STRING_BYTES {
                    return None;
                }

                // 字符串内容 + \r\n
                if line_end + str_len + 2 <= bytes.len() {
                    Some(line_end + str_len + 2)
                } else {
                    None
                }
            }
            b'~' => {
                // RDB文件类型
                let mut len_end = None;
                for i in 1..bytes.len().min(20) {
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        len_end = Some(i + 2);
                        break;
                    }
                }

                let len_end = len_end?;
                if len_end >= bytes.len() {
                    return None;
                }

                let len_str = std::str::from_utf8(&bytes[1..len_end - 2]).ok()?;
                let data_len: usize = len_str.parse().ok()?;
                if data_len > MAX_BULK_STRING_BYTES {
                    return None;
                }

                if len_end + data_len + 2 <= bytes.len() {
                    Some(len_end + data_len + 2)
                } else {
                    None
                }
            }
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
            b'+' | b'-' | b':' => {
                for i in 1..bytes.len() {
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        return Some(i + 2);
                    }
                }
                None
            }
            b'$' => {
                // 找到长度行的结束
                let mut line_end = None;
                for i in 1..bytes.len().min(20) {
                    if bytes[i] == b'\r' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                        line_end = Some(i + 2);
                        break;
                    }
                }

                let line_end = line_end?;

                // 解析字符串长度
                let line = std::str::from_utf8(&bytes[1..line_end - 2]).ok()?;
                if line == "-1" {
                    // NULL批量字符串
                    return Some(line_end);
                }

                let str_len: usize = line.parse().ok()?;
                if str_len > MAX_BULK_STRING_BYTES {
                    return None;
                }

                // 字符串内容 + \r\n
                if line_end + str_len + 2 <= bytes.len() {
                    Some(line_end + str_len + 2)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn find_inline_frame_end(bytes: &[u8]) -> Option<usize> {
        bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|idx| idx + 2)
    }

    /**
     * 简单字符串
     *
     * @param bytes 二进制
     */
    fn parse_simple_string(bytes: &[u8]) -> Result<Frame, Error> {
        let end = bytes.iter().position(|&x| x == b'\r').unwrap();
        let content = String::from_utf8(bytes[1..end].to_vec())?;
        Ok(Frame::SimpleString(content))
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

    fn parse_error(bytes: &[u8]) -> Result<Frame, Error> {
        let end = bytes.iter().position(|&x| x == b'\r').unwrap();
        let content = String::from_utf8(bytes[1..end].to_vec())?;
        Ok(Frame::Error(content))
    }

    fn parse_integer(bytes: &[u8]) -> Result<Frame, Error> {
        let end = bytes.iter().position(|&x| x == b'\r').unwrap();
        let content = std::str::from_utf8(&bytes[1..end])?;
        let value = content.parse::<i64>()?;
        Ok(Frame::Integer(value))
    }

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
        let mut len_end = None;
        for (i, &byte) in bytes.iter().enumerate() {
            if byte == b'\r' {
                len_end = Some(i);
                break;
            }
        }

        let len_end = match len_end {
            Some(pos) => pos,
            None => return Err(Error::msg("Invalid RDB format: missing CR")),
        };

        let len_bytes = &bytes[1..len_end];
        let len_str = match std::str::from_utf8(len_bytes) {
            Ok(s) => s,
            Err(e) => return Err(Error::msg(format!("Invalid UTF-8: {}", e))),
        };

        let data_len = match len_str.parse::<usize>() {
            Ok(n) => n,
            Err(e) => {
                return Err(Error::msg(format!(
                    "Invalid RDB length: {} ({})",
                    len_str, e
                )));
            }
        };

        let data_start = len_end + 2;
        let data_end = data_start + data_len;

        if bytes.len() < data_end + 2 {
            return Err(Error::msg(format!(
                "RDB data incomplete: expected {} bytes, got {}",
                data_end + 2,
                bytes.len()
            )));
        }

        if bytes[data_end] != b'\r' || bytes[data_end + 1] != b'\n' {
            return Err(Error::msg("Invalid RDB terminator"));
        }

        let mut data = Vec::with_capacity(data_len);
        for &byte in &bytes[data_start..data_end] {
            data.push(byte);
        }

        Ok(Frame::RDBFile(data))
    }

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

    /**
     * 获取指定索引的内容
     *
     * @param index 索引
     */
    pub fn get_arg(&self, index: usize) -> Option<String> {
        match self {
            Frame::Array(array) => {
                if index < array.len() {
                    array[index].as_text()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn arg_len(&self) -> usize {
        match self {
            Frame::Array(array) => array.len(),
            _ => 0,
        }
    }

    /**
     * 获取命令帧中的所有参数
     *
     * @param self 本身
     *
     * @return 一个包含所有参数的字符串向量，如果不是 Array 类型则返回空向量
     */
    pub fn get_args(&self) -> Vec<String> {
        match self {
            Frame::Array(array) => array.iter().filter_map(Frame::as_text).collect(),
            _ => Vec::new(),
        }
    }

    /**
     * 获取从指定索引开始的内容集合
     *
     * @param self 本身
     * @param start_index 开始索引
     *
     * @return 一个包含从指定索引开始的所有参数的字符串向量，如果不是 Array 类型或索引超出范围则返回空向量
     */
    pub fn get_args_from_index(&self, start_index: usize) -> Vec<String> {
        match self {
            Frame::Array(array) => {
                if start_index < array.len() {
                    array[start_index..]
                        .iter()
                        .filter_map(Frame::as_text)
                        .collect()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    pub fn get_arg_bytes(&self, index: usize) -> Option<Vec<u8>> {
        match self {
            Frame::Array(array) => array.get(index).and_then(Frame::as_bytes_arg),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<String> {
        match self {
            Frame::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
            Frame::SimpleString(text) | Frame::Error(text) => Some(text.clone()),
            Frame::Integer(value) => Some(value.to_string()),
            Frame::Ok => Some("OK".to_string()),
            Frame::Null | Frame::Array(_) | Frame::RDBFile(_) => None,
        }
    }

    pub fn as_bytes_arg(&self) -> Option<Vec<u8>> {
        match self {
            Frame::BulkString(bytes) => Some(bytes.clone()),
            Frame::SimpleString(text) | Frame::Error(text) => Some(text.as_bytes().to_vec()),
            Frame::Integer(value) => Some(value.to_string().into_bytes()),
            Frame::Ok => Some(b"OK".to_vec()),
            Frame::Null | Frame::Array(_) | Frame::RDBFile(_) => None,
        }
    }

    pub fn bulk_string<T: Into<Vec<u8>>>(value: T) -> Self {
        Frame::BulkString(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{Frame, MAX_ARRAY_ELEMENTS, MAX_BULK_STRING_BYTES};

    #[test]
    fn parse_multiple_frames_handles_client_setinfo_with_values() {
        let bytes = b"*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$8\r\nredis-rs\r\n*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$7\r\nLIB-VER\r\n$10\r\n1.0.0-rc.4\r\n";
        let frames = Frame::parse_multiple_frames(bytes).unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[0].get_args(),
            vec!["CLIENT", "SETINFO", "LIB-NAME", "redis-rs"]
        );
        assert_eq!(
            frames[1].get_args(),
            vec!["CLIENT", "SETINFO", "LIB-VER", "1.0.0-rc.4"]
        );
    }

    #[test]
    fn complete_frames_len_stops_before_partial_frame() {
        let complete = b"*1\r\n$4\r\nPING\r\n";
        let partial = b"*2\r\n$3\r\nGET\r\n$3\r\nke";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(complete);
        bytes.extend_from_slice(partial);

        assert_eq!(Frame::complete_frames_len(&bytes), complete.len());
    }

    #[test]
    fn parse_inline_ping_command() {
        let frame = Frame::parse_from_bytes(b"PING\r\n").unwrap();

        assert_eq!(frame.get_args(), vec!["PING"]);
    }

    #[test]
    fn parse_multiple_frames_ignores_incomplete_tail_frame() {
        let complete = b"*1\r\n$4\r\nPING\r\n";
        let partial = b"*2\r\n$3\r\nGET\r\n$3\r\nke";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(complete);
        bytes.extend_from_slice(partial);

        let frames = Frame::parse_multiple_frames(&bytes).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].get_args(), vec!["PING"]);
    }

    #[test]
    fn oversized_bulk_string_is_rejected() {
        let frame = format!("${}\r\n", super::MAX_BULK_STRING_BYTES.saturating_add(1));

        assert!(Frame::parse_from_bytes(frame.as_bytes()).is_err());
        assert_eq!(Frame::complete_frames_len(frame.as_bytes()), 0);
    }

    #[test]
    fn oversized_array_is_rejected() {
        let frame = format!("*{}\r\n", MAX_ARRAY_ELEMENTS.saturating_add(1));

        assert!(Frame::parse_from_bytes(frame.as_bytes()).is_err());
        assert_eq!(Frame::complete_frames_len(frame.as_bytes()), 0);
    }

    #[test]
    fn frame_to_string_and_bytes_cover_all_variants() {
        assert_eq!(Frame::Ok.to_string(), "OK");
        assert_eq!(Frame::Integer(-7).to_string(), "-7");
        assert_eq!(
            Frame::RDBFile(vec![1, 2, 3]).to_string(),
            "[RDBFile 3 bytes]"
        );
        assert_eq!(Frame::SimpleString("PONG".to_string()).to_string(), "PONG");
        assert_eq!(Frame::BulkString(b"hello".to_vec()).to_string(), "hello");
        assert_eq!(Frame::Error("ERR bad".to_string()).to_string(), "ERR bad");
        assert_eq!(Frame::Null.to_string(), "");
        assert_eq!(
            Frame::Array(vec![
                Frame::SimpleString("A".to_string()),
                Frame::Integer(2),
                Frame::Null,
            ])
            .to_string(),
            "A 2"
        );

        assert_eq!(Frame::Ok.as_bytes(), b"+OK\r\n");
        assert_eq!(Frame::Integer(-7).as_bytes(), b":-7\r\n");
        assert_eq!(
            Frame::SimpleString("PONG".to_string()).as_bytes(),
            b"+PONG\r\n"
        );
        assert_eq!(
            Frame::Error("ERR bad".to_string()).as_bytes(),
            b"-ERR bad\r\n"
        );
        assert_eq!(Frame::Null.as_bytes(), b"$-1\r\n");
        assert_eq!(
            Frame::BulkString(b"hi".to_vec()).as_bytes(),
            b"$2\r\nhi\r\n"
        );
        assert_eq!(
            Frame::RDBFile(vec![1, 2, 3]).as_bytes(),
            b"~3\r\n\x01\x02\x03\r\n"
        );
        assert_eq!(
            Frame::Array(vec![Frame::Integer(1), Frame::BulkString(b"x".to_vec())]).as_bytes(),
            b"*2\r\n:1\r\n$1\r\nx\r\n"
        );
    }

    #[test]
    fn parse_simple_error_integer_rdb_null_and_nested_arrays() {
        assert!(matches!(
            Frame::parse_from_bytes(b"+OK\r\n").unwrap(),
            Frame::SimpleString(value) if value == "OK"
        ));
        assert!(Frame::parse_from_bytes(b"+\xff\r\n").is_err());
        assert!(matches!(
            Frame::parse_from_bytes(b"-ERR bad\r\n").unwrap(),
            Frame::Error(value) if value == "ERR bad"
        ));
        assert!(Frame::parse_from_bytes(b"-\xff\r\n").is_err());
        assert!(matches!(
            Frame::parse_from_bytes(b":-42\r\n").unwrap(),
            Frame::Integer(-42)
        ));
        assert!(Frame::parse_from_bytes(b":nope\r\n").is_err());
        assert!(matches!(
            Frame::parse_from_bytes(b"$-1\r\n").unwrap(),
            Frame::Null
        ));
        assert!(matches!(
            Frame::parse_from_bytes(b"~3\r\nabc\r\n").unwrap(),
            Frame::RDBFile(value) if value == b"abc"
        ));
        assert!(matches!(
            Frame::parse_from_bytes(b"*3\r\n+OK\r\n:5\r\n-ERR no\r\n").unwrap(),
            Frame::Array(values)
                if matches!(&values[0], Frame::SimpleString(value) if value == "OK")
                    && matches!(&values[1], Frame::Integer(5))
                    && matches!(&values[2], Frame::Error(value) if value == "ERR no")
        ));
        assert!(matches!(
            Frame::parse_from_bytes(b"*1\r\n*1\r\n$4\r\nPING\r\n").unwrap(),
            Frame::Array(values)
                if matches!(&values[0], Frame::Array(inner) if inner[0].to_string() == "PING")
        ));
    }

    #[test]
    fn invalid_frames_report_errors_or_incomplete_lengths() {
        assert!(Frame::parse_from_bytes(b"").is_err());
        assert!(Frame::parse_from_bytes(b"   \r\n").is_err());
        assert!(Frame::parse_from_bytes(b"PING").is_err());
        assert!(Frame::parse_from_bytes(b"$3\nabc\r\n").is_err());
        assert!(Frame::parse_from_bytes(b"$-2\r\n").is_err());
        assert!(Frame::parse_from_bytes(b"$3\r\nab").is_err());
        assert!(Frame::parse_from_bytes(b"$3\r\nabcXX").is_err());
        assert!(Frame::parse_from_bytes(b"~bad\r\nabc\r\n").is_err());
        assert!(Frame::parse_from_bytes(b"~3\r\nab").is_err());
        assert!(Frame::parse_from_bytes(b"~3\r\nabcXX").is_err());
        assert!(Frame::parse_from_bytes(b"~\xff\r\nabc\r\n").is_err());
        assert!(Frame::parse_from_bytes(b"*x\r\n").is_err());
        assert!(Frame::parse_from_bytes(b"*2\r\n$4\r\nPING\r\n").is_err());

        assert_eq!(Frame::find_frame_end(b""), None);
        assert_eq!(Frame::find_frame_end(b"+OK\r\ntrailing"), Some(5));
        assert_eq!(Frame::find_frame_end(b"-ERR\r\n"), Some(6));
        assert_eq!(Frame::find_frame_end(b":1\r\n"), Some(4));
        assert_eq!(Frame::find_frame_end(b"$-1\r\n"), Some(5));
        assert_eq!(Frame::find_frame_end(b"$3\r\nabc\r\n"), Some(9));
        assert_eq!(Frame::find_frame_end(b"$3\r\nab"), None);
        assert_eq!(
            Frame::find_frame_end(
                format!("${}\r\n", MAX_BULK_STRING_BYTES.saturating_add(1)).as_bytes()
            ),
            None
        );
        assert_eq!(Frame::find_frame_end(b"~3\r\nabc\r\n"), Some(9));
        assert_eq!(Frame::find_frame_end(b"~3\r\nab"), None);
        assert_eq!(Frame::find_frame_end(b"PING\r\n"), Some(6));
        assert_eq!(Frame::find_frame_end(b"PING"), None);
        assert_eq!(Frame::complete_frames_len(b"+OK\r\n:1\r\npartial"), 9);
    }

    #[test]
    fn argument_accessors_cover_text_bytes_and_non_array_inputs() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"cmd".to_vec()),
            Frame::SimpleString("simple".to_string()),
            Frame::Error("err".to_string()),
            Frame::Integer(42),
            Frame::Ok,
            Frame::Null,
            Frame::Array(vec![]),
            Frame::RDBFile(vec![1]),
        ]);

        assert_eq!(frame.arg_len(), 8);
        assert_eq!(frame.get_arg(0), Some("cmd".to_string()));
        assert_eq!(frame.get_arg(1), Some("simple".to_string()));
        assert_eq!(frame.get_arg(2), Some("err".to_string()));
        assert_eq!(frame.get_arg(3), Some("42".to_string()));
        assert_eq!(frame.get_arg(4), Some("OK".to_string()));
        assert_eq!(frame.get_arg(5), None);
        assert_eq!(frame.get_arg(99), None);
        assert_eq!(frame.get_args(), vec!["cmd", "simple", "err", "42", "OK"]);
        assert_eq!(
            frame.get_args_from_index(2),
            vec!["err".to_string(), "42".to_string(), "OK".to_string()]
        );
        assert!(frame.get_args_from_index(99).is_empty());
        assert_eq!(frame.get_arg_bytes(0), Some(b"cmd".to_vec()));
        assert_eq!(frame.get_arg_bytes(1), Some(b"simple".to_vec()));
        assert_eq!(frame.get_arg_bytes(2), Some(b"err".to_vec()));
        assert_eq!(frame.get_arg_bytes(3), Some(b"42".to_vec()));
        assert_eq!(frame.get_arg_bytes(4), Some(b"OK".to_vec()));
        assert_eq!(frame.get_arg_bytes(5), None);

        let non_array = Frame::BulkString(b"value".to_vec());
        assert_eq!(non_array.arg_len(), 0);
        assert_eq!(non_array.get_arg(0), None);
        assert!(non_array.get_args().is_empty());
        assert!(non_array.get_args_from_index(0).is_empty());
        assert_eq!(non_array.get_arg_bytes(0), None);
        assert_eq!(Frame::RDBFile(vec![1]).as_text(), None);
        assert_eq!(Frame::RDBFile(vec![1]).as_bytes_arg(), None);
    }
}
