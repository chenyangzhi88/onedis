use super::*;

impl Frame {
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

    pub fn get_args(&self) -> Vec<String> {
        match self {
            // Never drop an unrepresentable item: doing so shifts every later
            // argument to a different command position. Returning no textual
            // arguments makes legacy text-only parsers reject the request.
            Frame::Array(array) => array
                .iter()
                .map(Frame::as_text)
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    pub fn get_args_from_index(&self, start_index: usize) -> Vec<String> {
        match self {
            Frame::Array(array) => {
                if start_index < array.len() {
                    array[start_index..]
                        .iter()
                        .map(Frame::as_text)
                        .collect::<Option<Vec<_>>>()
                        .unwrap_or_default()
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
            Frame::Null | Frame::Array(_) => None,
        }
    }

    pub fn as_bytes_arg(&self) -> Option<Vec<u8>> {
        match self {
            Frame::BulkString(bytes) => Some(bytes.clone()),
            Frame::SimpleString(text) | Frame::Error(text) => Some(text.as_bytes().to_vec()),
            Frame::Integer(value) => Some(value.to_string().into_bytes()),
            Frame::Ok => Some(b"OK".to_vec()),
            Frame::Null | Frame::Array(_) => None,
        }
    }

    pub fn bulk_string<T: Into<Vec<u8>>>(value: T) -> Self {
        Frame::BulkString(value.into())
    }
}
