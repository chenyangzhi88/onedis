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

include!("frame/serialization.rs");
include!("frame/parsing.rs");
include!("frame/accessors.rs");
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
        assert!(Frame::parse_from_bytes(b"+OK").is_err());
        assert!(Frame::parse_from_bytes(b"+OK\rx").is_err());
        assert!(Frame::parse_from_bytes(b"-ERR").is_err());
        assert!(Frame::parse_from_bytes(b":1").is_err());
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
