fn parse_borrowed_resp_commands(bytes: &[u8]) -> Option<Vec<Vec<&[u8]>>> {
    let mut commands = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] != b'*' {
            return None;
        }
        let header_end = find_crlf(bytes, pos + 1)?;
        let argc = parse_usize_ascii(&bytes[pos + 1..header_end])?;
        pos = header_end + 2;

        let mut args = Vec::with_capacity(argc);
        for _ in 0..argc {
            if pos >= bytes.len() || bytes[pos] != b'$' {
                return None;
            }
            let len_end = find_crlf(bytes, pos + 1)?;
            let len = parse_usize_ascii(&bytes[pos + 1..len_end])?;
            let data_start = len_end + 2;
            let data_end = data_start.checked_add(len)?;
            if data_end + 2 > bytes.len() || &bytes[data_end..data_end + 2] != b"\r\n" {
                return None;
            }
            args.push(&bytes[data_start..data_end]);
            pos = data_end + 2;
        }
        commands.push(args);
    }
    Some(commands)
}

fn parse_borrowed_plain_set_commands(bytes: &[u8]) -> Option<Vec<(&[u8], &[u8])>> {
    let mut commands = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] != b'*' {
            return None;
        }
        let header_end = find_crlf(bytes, pos + 1)?;
        let argc = parse_usize_ascii(&bytes[pos + 1..header_end])?;
        if argc != 3 {
            return None;
        }
        pos = header_end + 2;

        let command = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        if !command.eq_ignore_ascii_case(b"SET") {
            return None;
        }
        let key = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        let value = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        commands.push((key, value));
    }
    Some(commands)
}

fn parse_borrowed_plain_hset_commands(bytes: &[u8]) -> Option<Vec<BorrowedHsetCommand<'_>>> {
    let mut commands = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] != b'*' {
            return None;
        }
        let header_end = find_crlf(bytes, pos + 1)?;
        let argc = parse_usize_ascii(&bytes[pos + 1..header_end])?;
        if argc < 4 || argc % 2 != 0 {
            return None;
        }
        pos = header_end + 2;

        let command = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        if !command.eq_ignore_ascii_case(b"HSET") {
            return None;
        }
        let key = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        let mut fields = Vec::with_capacity((argc - 2) / 2);
        for _ in 0..(argc - 2) / 2 {
            let field = parse_borrowed_bulk_arg(bytes, &mut pos)?;
            let value = parse_borrowed_bulk_arg(bytes, &mut pos)?;
            fields.push((field, value));
        }
        commands.push((key, fields));
    }
    Some(commands)
}

fn parse_borrowed_plain_hdel_commands(bytes: &[u8]) -> Option<Vec<BorrowedHdelCommand<'_>>> {
    parse_borrowed_resp_commands(bytes)?
        .into_iter()
        .map(|args| {
            if args.len() < 3 || !args[0].eq_ignore_ascii_case(b"HDEL") {
                return None;
            }
            Some((args[1], args[2..].to_vec()))
        })
        .collect()
}

fn parse_borrowed_plain_hgetdel_commands(bytes: &[u8]) -> Option<Vec<BorrowedHdelCommand<'_>>> {
    parse_borrowed_resp_commands(bytes)?
        .into_iter()
        .map(|args| {
            if args.len() < 5
                || !args[0].eq_ignore_ascii_case(b"HGETDEL")
                || !args[2].eq_ignore_ascii_case(b"FIELDS")
            {
                return None;
            }
            let count = parse_usize_ascii(args[3])?;
            if count == 0 || args.len() != 4usize.checked_add(count)? {
                return None;
            }
            Some((args[1], args[4..].to_vec()))
        })
        .collect()
}

fn parse_borrowed_bulk_arg<'a>(bytes: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if *pos >= bytes.len() || bytes[*pos] != b'$' {
        return None;
    }
    let len_end = find_crlf(bytes, *pos + 1)?;
    let len = parse_usize_ascii(&bytes[*pos + 1..len_end])?;
    let data_start = len_end + 2;
    let data_end = data_start.checked_add(len)?;
    if data_end + 2 > bytes.len() || &bytes[data_end..data_end + 2] != b"\r\n" {
        return None;
    }
    *pos = data_end + 2;
    Some(&bytes[data_start..data_end])
}

fn borrowed_read_supported(args: &[&[u8]]) -> bool {
    let Some(command) = args.first() else {
        return false;
    };
    if command.eq_ignore_ascii_case(b"GEOPOS") {
        return args.len() >= 3
            && args.len() - 2 <= (crate::frame::MAX_FRAME_NODES - 1) / 3;
    }
    command.eq_ignore_ascii_case(b"GET")
        || command.eq_ignore_ascii_case(b"MGET")
        || command.eq_ignore_ascii_case(b"EXISTS")
        || command.eq_ignore_ascii_case(b"TOUCH")
        || command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"STRLEN")
        || command.eq_ignore_ascii_case(b"GETRANGE")
        || command.eq_ignore_ascii_case(b"SUBSTR")
        || command.eq_ignore_ascii_case(b"GETBIT")
        || command.eq_ignore_ascii_case(b"BITCOUNT")
        || command.eq_ignore_ascii_case(b"BITPOS")
        || command.eq_ignore_ascii_case(b"TYPE")
        || command.eq_ignore_ascii_case(b"HGET")
        || command.eq_ignore_ascii_case(b"HMGET")
        || command.eq_ignore_ascii_case(b"HEXISTS")
        || command.eq_ignore_ascii_case(b"HSTRLEN")
        || command.eq_ignore_ascii_case(b"HLEN")
        || command.eq_ignore_ascii_case(b"SISMEMBER")
        || command.eq_ignore_ascii_case(b"SMISMEMBER")
        || command.eq_ignore_ascii_case(b"SCARD")
        || command.eq_ignore_ascii_case(b"ZSCORE")
        || command.eq_ignore_ascii_case(b"ZMSCORE")
        || command.eq_ignore_ascii_case(b"ZCARD")
        || command.eq_ignore_ascii_case(b"GEOHASH")
        || command.eq_ignore_ascii_case(b"GEODIST")
        || command.eq_ignore_ascii_case(b"LLEN")
        || command.eq_ignore_ascii_case(b"XLEN")
        || command.eq_ignore_ascii_case(b"PFCOUNT")
        || command.eq_ignore_ascii_case(b"JSON.GET")
}

fn borrowed_plain_set_supported(args: &[&[u8]]) -> bool {
    args.len() == 3
        && args
            .first()
            .is_some_and(|command| command.eq_ignore_ascii_case(b"SET"))
}

fn parse_borrowed_string_mutations<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<StringBatchMutation<'a>>> {
    commands
        .iter()
        .map(|args| {
            let command = args.first()?;
            let key = std::str::from_utf8(args.get(1)?).ok()?;
            if command.eq_ignore_ascii_case(b"APPEND") && args.len() == 3 {
                Some(StringBatchMutation::Append {
                    key,
                    value: args[2],
                })
            } else if command.eq_ignore_ascii_case(b"GETSET") && args.len() == 3 {
                Some(StringBatchMutation::GetSet {
                    key,
                    value: args[2],
                })
            } else if command.eq_ignore_ascii_case(b"GETDEL") && args.len() == 2 {
                Some(StringBatchMutation::GetDel { key })
            } else if command.eq_ignore_ascii_case(b"SETNX") && args.len() == 3 {
                Some(StringBatchMutation::SetNx {
                    key,
                    value: args[2],
                })
            } else if command.eq_ignore_ascii_case(b"SETBIT") && args.len() == 4 {
                let offset = parse_usize_ascii(args[2])?;
                let bit = parse_u64_ascii(args[3])?;
                if offset >= crate::frame::MAX_BULK_STRING_BYTES.saturating_mul(8) || bit > 1 {
                    return None;
                }
                Some(StringBatchMutation::SetBit {
                    key,
                    offset,
                    bit: bit as u8,
                })
            } else if command.eq_ignore_ascii_case(b"SETRANGE") && args.len() == 4 {
                let offset = parse_i64_ascii(args[2])?;
                Some(StringBatchMutation::SetRange {
                    key,
                    offset: usize::try_from(offset).ok()?,
                    value: args[3],
                })
            } else if command.eq_ignore_ascii_case(b"PSETEX") && args.len() == 4 {
                let ttl_ms = parse_u64_ascii(args[2])?;
                (ttl_ms > 0).then_some(StringBatchMutation::Psetex {
                    key,
                    ttl_ms,
                    value: args[3],
                })
            } else if command.eq_ignore_ascii_case(b"SETEX") && args.len() == 4 {
                let seconds = parse_u64_ascii(args[2])?;
                let ttl_ms = seconds.checked_mul(1000)?;
                (ttl_ms > 0).then_some(StringBatchMutation::Psetex {
                    key,
                    ttl_ms,
                    value: args[3],
                })
            } else {
                None
            }
        })
        .collect()
}

fn parse_borrowed_key_expiration_mutations<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<KeyExpirationBatchMutation<'a>>> {
    commands
        .iter()
        .map(|args| {
            let command = args.first()?;
            let key = std::str::from_utf8(args.get(1)?).ok()?;
            if command.eq_ignore_ascii_case(b"PERSIST") && args.len() == 2 {
                Some(KeyExpirationBatchMutation::Persist { key })
            } else if command.eq_ignore_ascii_case(b"PEXPIRE") && args.len() == 3 {
                let ttl_ms = parse_i64_ascii(args[2])?;
                (ttl_ms > 0).then_some(KeyExpirationBatchMutation::Expire {
                    key,
                    ttl_ms: ttl_ms as u64,
                })
            } else if command.eq_ignore_ascii_case(b"EXPIRE") && args.len() == 3 {
                let seconds = parse_i64_ascii(args[2])?;
                let ttl_ms = (seconds > 0)
                    .then(|| (seconds as u64).checked_mul(1000))
                    .flatten()?;
                Some(KeyExpirationBatchMutation::Expire { key, ttl_ms })
            } else {
                None
            }
        })
        .collect()
}

type BorrowedStreamAddCommand<'a> =
    (&'a str, Option<StreamId>, Vec<(&'a str, &'a str)>);
type BorrowedStreamDeleteCommand<'a> = (&'a str, Vec<StreamId>);
type BorrowedRootJsonSetCommand<'a> = (&'a str, &'a str);
type BorrowedHllAddCommand<'a> = (&'a str, Vec<&'a [u8]>);
type BorrowedListPopCommand<'a> = (&'a str, bool, Option<usize>);
type BorrowedZsetPopCommand<'a> = (&'a str, bool, usize);
type BorrowedZsetIncrementCommand<'a> = (&'a str, f64, &'a str);
type BorrowedZsetAddCommand<'a> = (&'a str, Vec<(f64, &'a str)>);
type BorrowedBitopCommand<'a> = (&'a str, &'a str, Vec<&'a str>);
type BorrowedKeyDeleteCommand<'a> = (bool, Vec<&'a str>);

fn parse_borrowed_plain_zset_add_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedZsetAddCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 4
                || !args.len().is_multiple_of(2)
                || !args[0].eq_ignore_ascii_case(b"ZADD")
            {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let members = args[2..]
                .chunks_exact(2)
                .map(|pair| {
                    let score = std::str::from_utf8(pair[0]).ok()?.parse::<f64>().ok()?;
                    if score.is_nan() {
                        return None;
                    }
                    Some((score, std::str::from_utf8(pair[1]).ok()?))
                })
                .collect::<Option<Vec<_>>>()?;
            Some((key, members))
        })
        .collect()
}

fn parse_borrowed_bitop_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedBitopCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 4 || !args[0].eq_ignore_ascii_case(b"BITOP") {
                return None;
            }
            let op = std::str::from_utf8(args[1]).ok()?;
            if !matches!(op.to_ascii_uppercase().as_str(), "AND" | "OR" | "XOR" | "NOT")
                || op.eq_ignore_ascii_case("NOT") && args.len() != 4
            {
                return None;
            }
            let dest = std::str::from_utf8(args[2]).ok()?;
            let sources = args[3..]
                .iter()
                .map(|source| std::str::from_utf8(source).ok())
                .collect::<Option<Vec<_>>>()?;
            Some((op, dest, sources))
        })
        .collect()
}

fn parse_borrowed_key_delete_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedKeyDeleteCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 2 {
                return None;
            }
            let unlink = if args[0].eq_ignore_ascii_case(b"DEL") {
                false
            } else if args[0].eq_ignore_ascii_case(b"UNLINK") {
                true
            } else {
                return None;
            };
            let keys = args[1..]
                .iter()
                .map(|key| std::str::from_utf8(key).ok())
                .collect::<Option<Vec<_>>>()?;
            Some((unlink, keys))
        })
        .collect()
}

fn parse_borrowed_zset_increment_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedZsetIncrementCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() != 4 || !args[0].eq_ignore_ascii_case(b"ZINCRBY") {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let increment = std::str::from_utf8(args[2]).ok()?.parse::<f64>().ok()?;
            if increment.is_nan() {
                return None;
            }
            let member = std::str::from_utf8(args[3]).ok()?;
            Some((key, increment, member))
        })
        .collect()
}

fn parse_borrowed_plain_zset_pop_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedZsetPopCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let min = if args[0].eq_ignore_ascii_case(b"ZPOPMIN") {
                true
            } else if args[0].eq_ignore_ascii_case(b"ZPOPMAX") {
                false
            } else {
                return None;
            };
            let count = if args.len() == 3 {
                parse_usize_ascii(args[2])?
            } else {
                1
            };
            if count > crate::frame::MAX_ARRAY_ELEMENTS / 2 {
                return None;
            }
            Some((key, min, count))
        })
        .collect()
}

fn parse_borrowed_plain_list_pop_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedListPopCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let count = if args.len() == 3 {
                let count = parse_usize_ascii(args[2])?;
                (count <= crate::frame::MAX_ARRAY_ELEMENTS).then_some(count)?
            } else {
                return if args[0].eq_ignore_ascii_case(b"LPOP") {
                    Some((key, true, None))
                } else if args[0].eq_ignore_ascii_case(b"RPOP") {
                    Some((key, false, None))
                } else {
                    None
                };
            };
            if args[0].eq_ignore_ascii_case(b"LPOP") {
                Some((key, true, Some(count)))
            } else if args[0].eq_ignore_ascii_case(b"RPOP") {
                Some((key, false, Some(count)))
            } else {
                None
            }
        })
        .collect()
}

fn parse_borrowed_set_mutations<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<SetBatchMutation<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 3 {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let members = args[2..]
                .iter()
                .map(|member| std::str::from_utf8(member).ok())
                .collect::<Option<Vec<_>>>()?;
            if args[0].eq_ignore_ascii_case(b"SADD") {
                Some(SetBatchMutation::Add { key, members })
            } else if args[0].eq_ignore_ascii_case(b"SREM") {
                Some(SetBatchMutation::Remove { key, members })
            } else {
                None
            }
        })
        .collect()
}

fn parse_borrowed_plain_pfadd_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedHllAddCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 2 || !args[0].eq_ignore_ascii_case(b"PFADD") {
                return None;
            }
            Some((
                std::str::from_utf8(args[1]).ok()?,
                args[2..].to_vec(),
            ))
        })
        .collect()
}

fn parse_borrowed_root_json_set_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedRootJsonSetCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() != 4
                || !args[0].eq_ignore_ascii_case(b"JSON.SET")
                || !(args[2] == b"$" || args[2] == b".")
            {
                return None;
            }
            Some((
                std::str::from_utf8(args[1]).ok()?,
                std::str::from_utf8(args[3]).ok()?,
            ))
        })
        .collect()
}

fn parse_borrowed_plain_xadd_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedStreamAddCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 5
                || !(args.len() - 3).is_multiple_of(2)
                || !args[0].eq_ignore_ascii_case(b"XADD")
            {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let id_text = std::str::from_utf8(args[2]).ok()?;
            let id = if id_text == "*" {
                None
            } else {
                Some(StreamId::parse(id_text)?)
            };
            let fields = args[3..]
                .chunks_exact(2)
                .map(|pair| {
                    Some((
                        std::str::from_utf8(pair[0]).ok()?,
                        std::str::from_utf8(pair[1]).ok()?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            Some((key, id, fields))
        })
        .collect()
}

fn parse_borrowed_plain_xdel_commands<'a>(
    commands: &[Vec<&'a [u8]>],
) -> Option<Vec<BorrowedStreamDeleteCommand<'a>>> {
    commands
        .iter()
        .map(|args| {
            if args.len() < 3 || !args[0].eq_ignore_ascii_case(b"XDEL") {
                return None;
            }
            let key = std::str::from_utf8(args[1]).ok()?;
            let ids = args[2..]
                .iter()
                .map(|id| StreamId::parse(std::str::from_utf8(id).ok()?))
                .collect::<Option<Vec<_>>>()?;
            Some((key, ids))
        })
        .collect()
}

fn borrowed_list_push_supported(args: &[&[u8]]) -> bool {
    args.len() >= 3
        && args.first().is_some_and(|command| {
            command.eq_ignore_ascii_case(b"LPUSH") || command.eq_ignore_ascii_case(b"RPUSH")
        })
}

fn borrowed_lrange_supported(args: &[&[u8]]) -> bool {
    args.len() == 4
        && args
            .first()
            .is_some_and(|command| command.eq_ignore_ascii_case(b"LRANGE"))
}

fn borrowed_collection_read_supported(args: &[&[u8]]) -> bool {
    args.len() == 2
        && args.first().is_some_and(|command| {
            command.eq_ignore_ascii_case(b"SMEMBERS")
                || command.eq_ignore_ascii_case(b"HGETALL")
                || command.eq_ignore_ascii_case(b"HKEYS")
                || command.eq_ignore_ascii_case(b"HVALS")
        })
}

fn find_crlf(bytes: &[u8], start: usize) -> Option<usize> {
    bytes[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|offset| start + offset)
}

fn parse_usize_ascii(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0usize;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add((byte - b'0') as usize)?;
    }
    Some(value)
}

fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (negative, digits) = if let Some(rest) = bytes.strip_prefix(b"-") {
        (true, rest)
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut value = 0i64;
    for byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add((byte - b'0') as i64)?;
    }
    if negative {
        value.checked_neg()
    } else {
        Some(value)
    }
}

fn parse_u64_ascii(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0u64;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

fn append_simple_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(b"+");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_error(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(b"-");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_integer(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(format!(":{}\r\n", value).as_bytes());
}

fn append_array_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(format!("*{}\r\n", len).as_bytes());
}

fn append_bulk_string(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(b"$");
    append_usize_decimal(out, value.len());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

fn append_null(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

fn append_usize_decimal(out: &mut Vec<u8>, mut value: usize) {
    if value == 0 {
        out.push(b'0');
        return;
    }

    let mut buf = [0u8; 20];
    let mut idx = buf.len();
    while value > 0 {
        idx -= 1;
        buf[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    out.extend_from_slice(&buf[idx..]);
}

type BorrowedHsetCommand<'a> = (&'a [u8], Vec<(&'a [u8], &'a [u8])>);
type BorrowedHdelCommand<'a> = (&'a [u8], Vec<&'a [u8]>);
