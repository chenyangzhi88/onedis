use super::*;

impl std::fmt::Display for Frame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Ok => formatter.write_str("OK"),
            Frame::Integer(value) => write!(formatter, "{value}"),
            Frame::SimpleString(value) | Frame::Error(value) => formatter.write_str(value),
            Frame::BulkString(value) => formatter.write_str(&String::from_utf8_lossy(value)),
            Frame::Null => Ok(()),
            Frame::Array(arr) => {
                let mut result = String::new();
                for item in arr {
                    use std::fmt::Write;
                    write!(&mut result, "{item}")?;
                    result.push(' ');
                }
                formatter.write_str(result.trim_end())
            }
        }
    }
}

impl Frame {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut pending = vec![self];
        while let Some(frame) = pending.pop() {
            match frame {
                Frame::Ok => bytes.extend_from_slice(b"+OK\r\n"),
                Frame::Integer(value) => {
                    bytes.push(b':');
                    bytes.extend_from_slice(value.to_string().as_bytes());
                    bytes.extend_from_slice(b"\r\n");
                }
                Frame::SimpleString(value) => {
                    append_sanitized_line(&mut bytes, b'+', value);
                }
                Frame::Error(value) => append_sanitized_line(&mut bytes, b'-', value),
                Frame::Null => bytes.extend_from_slice(b"$-1\r\n"),
                Frame::Array(array) => {
                    bytes.push(b'*');
                    bytes.extend_from_slice(array.len().to_string().as_bytes());
                    bytes.extend_from_slice(b"\r\n");
                    pending.extend(array.iter().rev());
                }
                Frame::BulkString(value) => {
                    bytes.push(b'$');
                    bytes.extend_from_slice(value.len().to_string().as_bytes());
                    bytes.extend_from_slice(b"\r\n");
                    bytes.extend_from_slice(value);
                    bytes.extend_from_slice(b"\r\n");
                }
            }
        }
        bytes
    }
}

fn append_sanitized_line(output: &mut Vec<u8>, prefix: u8, value: &str) {
    output.push(prefix);
    output.extend(value.as_bytes().iter().map(|byte| {
        if matches!(byte, b'\r' | b'\n') {
            b' '
        } else {
            *byte
        }
    }));
    output.extend_from_slice(b"\r\n");
}
