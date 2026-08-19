use super::*;

impl std::fmt::Display for Frame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Ok => formatter.write_str("OK"),
            Frame::Integer(value) => write!(formatter, "{value}"),
            Frame::SimpleString(value) | Frame::Error(value) | Frame::BigNumber(value) => {
                formatter.write_str(value)
            }
            Frame::BulkString(value) | Frame::BlobError(value) => {
                formatter.write_str(&String::from_utf8_lossy(value))
            }
            Frame::VerbatimString { data, .. } => {
                formatter.write_str(&String::from_utf8_lossy(data))
            }
            Frame::Null => Ok(()),
            Frame::Boolean(value) => formatter.write_str(if *value { "true" } else { "false" }),
            Frame::Double(value) => write!(formatter, "{value}"),
            Frame::Array(values) | Frame::Set(values) | Frame::Push(values) => {
                display_sequence(formatter, values)
            }
            Frame::Map(values) => {
                let mut flattened = Vec::with_capacity(values.len() * 2);
                for (key, value) in values {
                    flattened.push(key.clone());
                    flattened.push(value.clone());
                }
                display_sequence(formatter, &flattened)
            }
            Frame::Attribute { data, .. } => std::fmt::Display::fmt(data, formatter),
        }
    }
}

fn display_sequence(formatter: &mut std::fmt::Formatter<'_>, values: &[Frame]) -> std::fmt::Result {
    let mut wrote_item = false;
    for item in values {
        if matches!(item, Frame::Null) {
            continue;
        }
        if wrote_item {
            formatter.write_str(" ")?;
        }
        write!(formatter, "{item}")?;
        wrote_item = true;
    }
    Ok(())
}

impl Frame {
    pub fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes_for_protocol(RespVersion::Resp2)
    }

    pub fn as_bytes_for_protocol(&self, protocol: RespVersion) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_frame(&mut bytes, self, protocol);
        bytes
    }
}

fn append_frame(output: &mut Vec<u8>, frame: &Frame, protocol: RespVersion) {
    match (protocol, frame) {
        (_, Frame::Ok) => output.extend_from_slice(b"+OK\r\n"),
        (_, Frame::Integer(value)) => append_line_number(output, b':', value),
        (_, Frame::SimpleString(value)) => append_sanitized_line(output, b'+', value),
        (_, Frame::Error(value)) => append_sanitized_line(output, b'-', value),
        (RespVersion::Resp2, Frame::Null) => output.extend_from_slice(b"$-1\r\n"),
        (RespVersion::Resp3, Frame::Null) => output.extend_from_slice(b"_\r\n"),
        (_, Frame::BulkString(value)) => append_blob(output, b'$', value),
        (RespVersion::Resp2, Frame::Boolean(value)) => {
            append_line_number(output, b':', &i64::from(*value));
        }
        (RespVersion::Resp3, Frame::Boolean(value)) => {
            output.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
        }
        (RespVersion::Resp2, Frame::Double(value)) => {
            append_blob(output, b'$', resp3_double(*value).as_bytes());
        }
        (RespVersion::Resp3, Frame::Double(value)) => {
            append_sanitized_line(output, b',', &resp3_double(*value));
        }
        (RespVersion::Resp2, Frame::BigNumber(value)) => {
            append_blob(output, b'$', value.as_bytes());
        }
        (RespVersion::Resp3, Frame::BigNumber(value)) => append_sanitized_line(output, b'(', value),
        (RespVersion::Resp2, Frame::BlobError(value)) => {
            append_sanitized_line(output, b'-', &String::from_utf8_lossy(value));
        }
        (RespVersion::Resp3, Frame::BlobError(value)) => append_blob(output, b'!', value),
        (RespVersion::Resp2, Frame::VerbatimString { data, .. }) => {
            append_blob(output, b'$', data);
        }
        (RespVersion::Resp3, Frame::VerbatimString { format, data }) => {
            let mut payload = Vec::with_capacity(4 + data.len());
            payload.extend_from_slice(format);
            payload.push(b':');
            payload.extend_from_slice(data);
            append_blob(output, b'=', &payload);
        }
        (_, Frame::Array(values)) => append_sequence(output, b'*', values, protocol),
        (RespVersion::Resp2, Frame::Set(values) | Frame::Push(values)) => {
            append_sequence(output, b'*', values, protocol);
        }
        (RespVersion::Resp3, Frame::Set(values)) => append_sequence(output, b'~', values, protocol),
        (RespVersion::Resp3, Frame::Push(values)) => {
            append_sequence(output, b'>', values, protocol);
        }
        (RespVersion::Resp2, Frame::Map(entries)) => append_map_as_array(output, entries, protocol),
        (RespVersion::Resp3, Frame::Map(entries)) => append_map(output, b'%', entries, protocol),
        (RespVersion::Resp2, Frame::Attribute { data, .. }) => append_frame(output, data, protocol),
        (RespVersion::Resp3, Frame::Attribute { attributes, data }) => {
            append_map(output, b'|', attributes, protocol);
            append_frame(output, data, protocol);
        }
    }
}

fn append_line_number<T: std::fmt::Display>(output: &mut Vec<u8>, prefix: u8, value: &T) {
    output.push(prefix);
    output.extend_from_slice(value.to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
}

fn append_blob(output: &mut Vec<u8>, prefix: u8, value: &[u8]) {
    output.push(prefix);
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_sequence(output: &mut Vec<u8>, prefix: u8, values: &[Frame], protocol: RespVersion) {
    output.push(prefix);
    output.extend_from_slice(values.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    for value in values {
        append_frame(output, value, protocol);
    }
}

fn append_map(output: &mut Vec<u8>, prefix: u8, entries: &[(Frame, Frame)], protocol: RespVersion) {
    output.push(prefix);
    output.extend_from_slice(entries.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    for (key, value) in entries {
        append_frame(output, key, protocol);
        append_frame(output, value, protocol);
    }
}

fn append_map_as_array(output: &mut Vec<u8>, entries: &[(Frame, Frame)], protocol: RespVersion) {
    output.push(b'*');
    output.extend_from_slice(entries.len().saturating_mul(2).to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    for (key, value) in entries {
        append_frame(output, key, protocol);
        append_frame(output, value, protocol);
    }
}

fn resp3_double(value: f64) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else if value == f64::INFINITY {
        "inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        value.to_string()
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
