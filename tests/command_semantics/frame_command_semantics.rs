#[cfg(test)]
mod tests {
    use anyhow::Error;
    use onedis_server::frame::{Frame, RespVersion};

    #[test]
    fn test_parse_multiple_frames() -> Result<(), Error> {
        // 模拟redis-rust客户端发送的粘连命令
        // CLIENT SETINFO LIB-NAME redis-rs
        // CLIENT SETINFO LIB-VER 1.0.0-rc.4
        let bytes = b"*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$8\r\nredis-rs\r\n*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$7\r\nLIB-VER\r\n$10\r\n1.0.0-rc.4\r\n";

        let frames = Frame::parse_multiple_frames(bytes)?;

        assert_eq!(frames.len(), 2);

        // 验证第一个命令
        let first_frame = &frames[0];
        let args1 = first_frame.get_args();
        assert_eq!(args1.len(), 4);
        assert_eq!(args1[0], "CLIENT");
        assert_eq!(args1[1], "SETINFO");
        assert_eq!(args1[2], "LIB-NAME");
        assert_eq!(args1[3], "redis-rs");

        // 验证第二个命令
        let second_frame = &frames[1];
        let args2 = second_frame.get_args();
        assert_eq!(args2.len(), 4);
        assert_eq!(args2[0], "CLIENT");
        assert_eq!(args2[1], "SETINFO");
        assert_eq!(args2[2], "LIB-VER");
        assert_eq!(args2[3], "1.0.0-rc.4");

        Ok(())
    }

    #[test]
    fn resp3_scalar_and_aggregate_types_round_trip() -> Result<(), Error> {
        let cases = [
            (b"_\r\n".as_slice(), Frame::Null),
            (b"#t\r\n".as_slice(), Frame::Boolean(true)),
            (b",1.25\r\n".as_slice(), Frame::Double(1.25)),
            (
                b"(3492890328409238509324850943850943825024385\r\n".as_slice(),
                Frame::BigNumber("3492890328409238509324850943850943825024385".to_string()),
            ),
            (
                b"!7\r\nERR bad\r\n".as_slice(),
                Frame::BlobError(b"ERR bad".to_vec()),
            ),
            (
                b"=9\r\ntxt:hello\r\n".as_slice(),
                Frame::VerbatimString {
                    format: *b"txt",
                    data: b"hello".to_vec(),
                },
            ),
        ];
        for (wire, expected) in cases {
            let parsed = Frame::parse_from_bytes(wire)?;
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_bytes_for_protocol(RespVersion::Resp3), wire);
        }

        let map = Frame::Map(vec![(Frame::bulk_string("key"), Frame::Integer(1))]);
        assert_eq!(
            map.as_bytes_for_protocol(RespVersion::Resp3),
            b"%1\r\n$3\r\nkey\r\n:1\r\n"
        );
        assert_eq!(
            Frame::parse_from_bytes(&map.as_bytes_for_protocol(RespVersion::Resp3))?,
            map
        );

        let push = Frame::Push(vec![
            Frame::bulk_string("message"),
            Frame::bulk_string("news"),
        ]);
        assert_eq!(
            push.as_bytes_for_protocol(RespVersion::Resp3),
            b">2\r\n$7\r\nmessage\r\n$4\r\nnews\r\n"
        );
        assert_eq!(
            Frame::parse_from_bytes(&push.as_bytes_for_protocol(RespVersion::Resp3))?,
            push
        );
        Ok(())
    }

    #[test]
    fn resp3_values_have_deterministic_resp2_downgrades() {
        assert_eq!(
            Frame::Boolean(true).as_bytes_for_protocol(RespVersion::Resp2),
            b":1\r\n"
        );
        assert_eq!(
            Frame::Map(vec![(Frame::bulk_string("a"), Frame::bulk_string("b"))])
                .as_bytes_for_protocol(RespVersion::Resp2),
            b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
        assert_eq!(
            Frame::Push(vec![Frame::bulk_string("invalidate")])
                .as_bytes_for_protocol(RespVersion::Resp2),
            b"*1\r\n$10\r\ninvalidate\r\n"
        );
    }
}
