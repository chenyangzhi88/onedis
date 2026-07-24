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
        if argc != 4 {
            return None;
        }
        pos = header_end + 2;

        let command = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        if !command.eq_ignore_ascii_case(b"HSET") {
            return None;
        }
        let key = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        let field = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        let value = parse_borrowed_bulk_arg(bytes, &mut pos)?;
        commands.push((key, field, value));
    }
    Some(commands)
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
    command.eq_ignore_ascii_case(b"GET")
        || command.eq_ignore_ascii_case(b"MGET")
        || command.eq_ignore_ascii_case(b"EXISTS")
        || command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"STRLEN")
        || command.eq_ignore_ascii_case(b"TYPE")
}

fn borrowed_plain_set_supported(args: &[&[u8]]) -> bool {
    args.len() == 3
        && args
            .first()
            .is_some_and(|command| command.eq_ignore_ascii_case(b"SET"))
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

type BorrowedHsetCommand<'a> = (&'a [u8], &'a [u8], &'a [u8]);

