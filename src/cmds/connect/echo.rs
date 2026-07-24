use anyhow::Error;

use crate::frame::{Frame, MAX_BULK_STRING_BYTES};

pub struct Echo {
    value: Vec<u8>,
}

impl Echo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let value = match frame {
            Frame::Array(mut arguments) if arguments.len() == 2 => {
                let value = arguments.pop().expect("ECHO argument length was validated");
                Self::frame_argument_into_bytes(value)
                    .ok_or_else(|| Error::msg("ERR ECHO argument must be a string or an integer"))?
            }
            _ => {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'echo' command",
                ));
            }
        };
        Self::validate_payload_len(value.len())?;
        Ok(Echo { value })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(Frame::bulk_string(self.value))
    }

    pub fn into_response_bytes(mut self) -> Vec<u8> {
        let payload_len = self.value.len();
        let header = format!("${payload_len}\r\n");
        let header_len = header.len();
        let response_len = payload_len
            .checked_add(header_len + 2)
            .expect("validated ECHO payload cannot overflow response length");

        self.value.reserve(header_len + 2);
        self.value.resize(response_len, 0);
        self.value.copy_within(0..payload_len, header_len);
        self.value[..header_len].copy_from_slice(header.as_bytes());
        self.value[header_len + payload_len..].copy_from_slice(b"\r\n");
        self.value
    }

    fn frame_argument_into_bytes(frame: Frame) -> Option<Vec<u8>> {
        match frame {
            Frame::BulkString(bytes) => Some(bytes),
            Frame::SimpleString(text) | Frame::Error(text) => Some(text.into_bytes()),
            Frame::Integer(value) => Some(value.to_string().into_bytes()),
            Frame::Ok => Some(b"OK".to_vec()),
            Frame::Null | Frame::Array(_) => None,
        }
    }

    fn validate_payload_len(payload_len: usize) -> Result<(), Error> {
        if payload_len > MAX_BULK_STRING_BYTES {
            return Err(Error::msg("ERR ECHO payload exceeds configured limit"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Echo;
    use crate::frame::{Frame, MAX_BULK_STRING_BYTES};

    fn echo_frame(payload: Vec<u8>) -> Frame {
        Frame::Array(vec![Frame::bulk_string("ECHO"), Frame::BulkString(payload)])
    }

    #[test]
    fn echo_is_binary_safe_and_resp_injection_stays_inside_one_bulk_string() {
        let payload = b"\xff\x00\r\n+PWNED\r\n*1\r\n$4\r\nPING\r\n".to_vec();
        let response = Echo::parse_from_frame(echo_frame(payload.clone()))
            .unwrap()
            .into_response_bytes();
        let expected_header = format!("${}\r\n", payload.len());

        assert!(response.starts_with(expected_header.as_bytes()));
        assert_eq!(
            &response[expected_header.len()..response.len() - 2],
            payload
        );
        let parsed = Frame::parse_multiple_frames(&response).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].get_arg_bytes(0), None);
        assert!(matches!(&parsed[0], Frame::BulkString(value) if value == &payload));
    }

    #[test]
    fn echo_rejects_bad_shapes_and_defends_its_payload_limit() {
        assert!(Echo::parse_from_frame(Frame::Array(vec![])).is_err());
        assert!(
            Echo::parse_from_frame(Frame::Array(vec![
                Frame::bulk_string("ECHO"),
                Frame::Array(Vec::new()),
            ]))
            .is_err()
        );
        assert!(Echo::validate_payload_len(MAX_BULK_STRING_BYTES).is_ok());
        assert!(Echo::validate_payload_len(MAX_BULK_STRING_BYTES + 1).is_err());
    }

    #[test]
    fn direct_response_encoding_matches_normal_frame_encoding() {
        let payload = b"hello\r\nworld".to_vec();
        let direct = Echo::parse_from_frame(echo_frame(payload.clone()))
            .unwrap()
            .into_response_bytes();
        let framed = Echo::parse_from_frame(echo_frame(payload))
            .unwrap()
            .apply()
            .unwrap()
            .as_bytes();

        assert_eq!(direct, framed);
    }
}
