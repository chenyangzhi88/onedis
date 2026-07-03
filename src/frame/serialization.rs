use super::*;

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
}
