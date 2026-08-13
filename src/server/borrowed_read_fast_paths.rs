impl Handler {
    async fn handle_borrowed_read_commands(&self, commands: Vec<Vec<&[u8]>>) -> Vec<u8> {
        let db = self.session.get_db().clone();
        let mut key_ranges = Vec::with_capacity(commands.len());
        let mut raw_keys = Vec::new();
        for args in &commands {
            let command = args.first().copied().unwrap_or_default();
            let keys = if (command.eq_ignore_ascii_case(b"GET")
                || command.eq_ignore_ascii_case(b"TTL")
                || command.eq_ignore_ascii_case(b"PTTL")
                || command.eq_ignore_ascii_case(b"STRLEN")
                || command.eq_ignore_ascii_case(b"TYPE")
                || command.eq_ignore_ascii_case(b"SCARD")
                || command.eq_ignore_ascii_case(b"LLEN")
                || command.eq_ignore_ascii_case(b"XLEN"))
                && args.len() == 2
            {
                &args[1..2]
            } else if ((command.eq_ignore_ascii_case(b"GETRANGE")
                || command.eq_ignore_ascii_case(b"SUBSTR"))
                && args.len() == 4)
                || (command.eq_ignore_ascii_case(b"GETBIT") && args.len() == 3)
                || (command.eq_ignore_ascii_case(b"BITCOUNT")
                    && (args.len() == 2 || args.len() == 4 || args.len() == 5))
                || (command.eq_ignore_ascii_case(b"BITPOS")
                    && (3..=6).contains(&args.len()))
            {
                &args[1..2]
            } else if (command.eq_ignore_ascii_case(b"MGET")
                || command.eq_ignore_ascii_case(b"EXISTS")
                || command.eq_ignore_ascii_case(b"TOUCH"))
                && args.len() >= 2
            {
                &args[1..]
            } else {
                &args[0..0]
            };
            let start = raw_keys.len();
            raw_keys.extend(keys.iter().copied());
            key_ranges.push(start..raw_keys.len());
        }
        let raw_values = db.read_live_raw_byte_keys_many_async(&raw_keys).await;
        let mut set_batch_commands = Vec::new();
        let mut set_batch_positions = Vec::new();
        let mut zset_batch_commands = Vec::new();
        let mut zset_batch_positions = Vec::new();
        let mut hash_batch_commands = Vec::new();
        let mut hash_batch_positions = Vec::new();
        let mut hash_len_keys = Vec::new();
        let mut hash_len_positions = Vec::new();
        let mut zset_card_keys = Vec::new();
        let mut zset_card_positions = Vec::new();
        let mut hll_count_commands = Vec::new();
        let mut hll_count_positions = Vec::new();
        let mut json_get_commands = Vec::new();
        let mut json_get_positions = Vec::new();
        for (position, args) in commands.iter().enumerate() {
            let command = args.first().copied().unwrap_or_default();
            if command.eq_ignore_ascii_case(b"PFCOUNT") && args.len() >= 2 {
                if let Some(keys) = args[1..]
                    .iter()
                    .map(|key| std::str::from_utf8(key).ok())
                    .collect::<Option<Vec<_>>>()
                {
                    hll_count_positions.push(position);
                    hll_count_commands.push(keys);
                }
                continue;
            }
            if command.eq_ignore_ascii_case(b"JSON.GET")
                && (args.len() == 2 || args.len() == 3)
            {
                if let (Ok(key), Ok(path)) = (
                    std::str::from_utf8(args[1]),
                    std::str::from_utf8(args.get(2).copied().unwrap_or(b"$")),
                ) {
                    json_get_positions.push(position);
                    json_get_commands.push((key, path));
                }
                continue;
            }
            if args.len() == 2
                && (command.eq_ignore_ascii_case(b"HLEN")
                    || command.eq_ignore_ascii_case(b"ZCARD"))
            {
                if let Ok(key) = std::str::from_utf8(args[1]) {
                    if command.eq_ignore_ascii_case(b"HLEN") {
                        hash_len_positions.push(position);
                        hash_len_keys.push(key);
                    } else {
                        zset_card_positions.push(position);
                        zset_card_keys.push(key);
                    }
                }
                continue;
            }
            let valid_arity = if command.eq_ignore_ascii_case(b"SISMEMBER")
                || command.eq_ignore_ascii_case(b"ZSCORE")
                || command.eq_ignore_ascii_case(b"HGET")
                || command.eq_ignore_ascii_case(b"HEXISTS")
                || command.eq_ignore_ascii_case(b"HSTRLEN")
            {
                args.len() == 3
            } else if command.eq_ignore_ascii_case(b"SMISMEMBER")
                || command.eq_ignore_ascii_case(b"ZMSCORE")
                || command.eq_ignore_ascii_case(b"HMGET")
            {
                args.len() >= 3
            } else {
                false
            };
            if !valid_arity {
                continue;
            }
            let Some(key) = std::str::from_utf8(args[1]).ok() else {
                continue;
            };
            let Some(members) = args[2..]
                .iter()
                .map(|member| std::str::from_utf8(member).ok().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if command.eq_ignore_ascii_case(b"SMISMEMBER")
                || command.eq_ignore_ascii_case(b"SISMEMBER")
            {
                set_batch_positions.push(position);
                set_batch_commands.push((key, members));
            } else if command.eq_ignore_ascii_case(b"ZMSCORE")
                || command.eq_ignore_ascii_case(b"ZSCORE")
            {
                zset_batch_positions.push(position);
                zset_batch_commands.push((key, members));
            } else {
                hash_batch_positions.push(position);
                hash_batch_commands.push((key, members));
            }
        }
        let mut set_batch_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in set_batch_positions.into_iter().zip(
            db.set_multi_contains_batch_async(&set_batch_commands)
                .await,
        ) {
            set_batch_replies[position] = Some(reply);
        }
        let mut zset_batch_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in zset_batch_positions.into_iter().zip(
            db.zset_multi_score_batch_async(&zset_batch_commands)
                .await,
        ) {
            zset_batch_replies[position] = Some(reply);
        }
        let mut hash_batch_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in hash_batch_positions.into_iter().zip(
            db.hash_multi_get_bytes_batch_async(&hash_batch_commands)
                .await,
        ) {
            hash_batch_replies[position] = Some(reply);
        }
        let mut hash_len_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in hash_len_positions
            .into_iter()
            .zip(db.hash_len_batch_async(&hash_len_keys).await)
        {
            hash_len_replies[position] = Some(reply);
        }
        let mut zset_card_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in zset_card_positions
            .into_iter()
            .zip(db.zset_card_batch_async(&zset_card_keys).await)
        {
            zset_card_replies[position] = Some(reply);
        }
        let mut hll_count_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in hll_count_positions
            .into_iter()
            .zip(db.hll_count_batch_async(&hll_count_commands).await)
        {
            hll_count_replies[position] = Some(reply);
        }
        let mut json_get_replies = std::iter::repeat_with(|| None)
            .take(commands.len())
            .collect::<Vec<_>>();
        for (position, reply) in json_get_positions
            .into_iter()
            .zip(db.json_get_batch_async(&json_get_commands).await)
        {
            json_get_replies[position] = Some(reply);
        }
        let mut out = Vec::with_capacity(commands.len() * 16);
        for (position, (args, key_range)) in commands.into_iter().zip(key_ranges).enumerate() {
            let command = args.first().copied().unwrap_or_default();
            if command.eq_ignore_ascii_case(b"GET") {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'get' command");
                    continue;
                }
                match raw_values[key_range.start].as_deref() {
                    Some(raw) => match decode_string_bytes_slice(raw) {
                        Some(value) => append_bulk_string(&mut out, value),
                        None => append_error(
                            &mut out,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ),
                    },
                    None => append_null(&mut out),
                }
            } else if command.eq_ignore_ascii_case(b"MGET") {
                if args.len() < 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'mget' command");
                    continue;
                }
                append_array_len(&mut out, args.len().saturating_sub(1));
                for raw in &raw_values[key_range] {
                    match raw.as_deref().and_then(decode_string_bytes_slice) {
                        Some(value) => {
                            append_bulk_string(&mut out, value);
                        }
                        None => append_null(&mut out),
                    }
                }
            } else if command.eq_ignore_ascii_case(b"EXISTS")
                || command.eq_ignore_ascii_case(b"TOUCH")
            {
                if args.len() < 2 {
                    let name = if command.eq_ignore_ascii_case(b"EXISTS") {
                        "exists"
                    } else {
                        "touch"
                    };
                    append_error(&mut out, &format!(
                        "ERR wrong number of arguments for '{name}' command"
                    ));
                    continue;
                }
                let count = raw_values[key_range]
                    .iter()
                    .filter(|raw| raw.is_some())
                    .count() as i64;
                append_integer(&mut out, count);
            } else if command.eq_ignore_ascii_case(b"TTL") || command.eq_ignore_ascii_case(b"PTTL")
            {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for ttl command");
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                let millis = raw_values[key_range.start]
                    .as_deref()
                    .map_or(-2, crate::store::db::Db::ttl_millis_from_live_raw);
                let value = if command.eq_ignore_ascii_case(b"TTL") && millis >= 0 {
                    millis / 1000
                } else {
                    millis
                };
                append_integer(&mut out, value);
            } else if command.eq_ignore_ascii_case(b"STRLEN") {
                if args.len() != 2 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'strlen' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                match raw_values[key_range.start].as_deref() {
                    Some(raw) => match decode_string_bytes_slice(raw) {
                        Some(value) => append_integer(&mut out, value.len() as i64),
                        None => append_error(
                            &mut out,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ),
                    },
                    None => append_integer(&mut out, 0),
                }
            } else if command.eq_ignore_ascii_case(b"GETRANGE")
                || command.eq_ignore_ascii_case(b"SUBSTR")
            {
                if args.len() != 4 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'getrange' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR command arguments must be valid UTF-8");
                    continue;
                }
                let (Some(start), Some(end)) =
                    (parse_i64_ascii(args[2]), parse_i64_ascii(args[3]))
                else {
                    append_error(&mut out, "ERR value is not an integer or out of range");
                    continue;
                };
                match crate::store::db::Db::string_range_from_live_raw(
                    raw_values[key_range.start].as_deref(),
                    start,
                    end,
                ) {
                    Ok(value) => append_bulk_string(&mut out, value),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"GETBIT") {
                if args.len() != 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'getbit' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR command arguments must be valid UTF-8");
                    continue;
                }
                let Some(offset) = parse_usize_ascii(args[2]).filter(|offset| {
                    *offset < crate::frame::MAX_BULK_STRING_BYTES.saturating_mul(8)
                }) else {
                    append_error(&mut out, "ERR bit offset is not an integer or out of range");
                    continue;
                };
                match crate::store::db::Db::string_get_bit_from_live_raw(
                    raw_values[key_range.start].as_deref(),
                    offset,
                ) {
                    Ok(bit) => append_integer(&mut out, bit as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"BITCOUNT") {
                if args.len() != 2 && args.len() != 4 && args.len() != 5 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'bitcount' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR command arguments must be valid UTF-8");
                    continue;
                }
                let range = if args.len() >= 4 {
                    match (parse_i64_ascii(args[2]), parse_i64_ascii(args[3])) {
                        (Some(start), Some(end)) => (Some(start), Some(end)),
                        _ => {
                            append_error(
                                &mut out,
                                "ERR value is not an integer or out of range",
                            );
                            continue;
                        }
                    }
                } else {
                    (None, None)
                };
                let bit_unit = if args.len() == 5 {
                    if args[4].eq_ignore_ascii_case(b"BIT") {
                        true
                    } else if args[4].eq_ignore_ascii_case(b"BYTE") {
                        false
                    } else {
                        append_error(&mut out, "ERR syntax error");
                        continue;
                    }
                } else {
                    false
                };
                match crate::store::db::Db::string_bitcount_from_live_raw(
                    raw_values[key_range.start].as_deref(),
                    range.0,
                    range.1,
                    bit_unit,
                ) {
                    Ok(count) => append_integer(&mut out, count as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"BITPOS") {
                if !(3..=6).contains(&args.len()) {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'bitpos' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR command arguments must be valid UTF-8");
                    continue;
                }
                let Some(bit) = parse_u64_ascii(args[2]).and_then(|bit| u8::try_from(bit).ok())
                else {
                    append_error(&mut out, "ERR bit is not an integer or out of range");
                    continue;
                };
                let start = if args.len() >= 4 {
                    let Some(value) = parse_i64_ascii(args[3]) else {
                        append_error(&mut out, "ERR value is not an integer or out of range");
                        continue;
                    };
                    Some(value)
                } else {
                    None
                };
                let end = if args.len() >= 5 {
                    let Some(value) = parse_i64_ascii(args[4]) else {
                        append_error(&mut out, "ERR value is not an integer or out of range");
                        continue;
                    };
                    Some(value)
                } else {
                    None
                };
                let bit_unit = if args.len() == 6 {
                    if args[5].eq_ignore_ascii_case(b"BIT") {
                        true
                    } else if args[5].eq_ignore_ascii_case(b"BYTE") {
                        false
                    } else {
                        append_error(&mut out, "ERR syntax error");
                        continue;
                    }
                } else {
                    false
                };
                match crate::store::db::Db::string_bitpos_from_live_raw(
                    raw_values[key_range.start].as_deref(),
                    bit,
                    start,
                    end,
                    bit_unit,
                ) {
                    Ok(position) => append_integer(&mut out, position),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"TYPE") {
                if args.len() != 2 {
                    append_error(&mut out, "ERR wrong number of arguments for 'type' command");
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                let type_name = raw_values[key_range.start]
                    .as_deref()
                    .map_or("none", crate::store::db::Db::type_name_from_live_raw);
                append_simple_string(&mut out, type_name);
            } else if command.eq_ignore_ascii_case(b"SCARD")
                || command.eq_ignore_ascii_case(b"LLEN")
                || command.eq_ignore_ascii_case(b"XLEN")
            {
                let name = if command.eq_ignore_ascii_case(b"SCARD") {
                    "scard"
                } else if command.eq_ignore_ascii_case(b"LLEN") {
                    "llen"
                } else {
                    "xlen"
                };
                if args.len() != 2 {
                    append_error(
                        &mut out,
                        &format!("ERR wrong number of arguments for '{name}' command"),
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                let result = match raw_values[key_range.start].as_deref() {
                    None => Ok(0),
                    Some(raw) if command.eq_ignore_ascii_case(b"SCARD") => {
                        crate::store::db::Db::set_len_from_live_raw(raw)
                    }
                    Some(raw) if command.eq_ignore_ascii_case(b"LLEN") => {
                        crate::store::db::Db::list_len_from_live_raw(raw)
                    }
                    Some(raw) => crate::store::db::Db::stream_len_from_live_raw(raw),
                };
                match result {
                    Ok(len) => append_integer(&mut out, len as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"HLEN")
                || command.eq_ignore_ascii_case(b"ZCARD")
            {
                let name = if command.eq_ignore_ascii_case(b"HLEN") {
                    "hlen"
                } else {
                    "zcard"
                };
                if args.len() != 2 {
                    append_error(
                        &mut out,
                        &format!("ERR wrong number of arguments for '{name}' command"),
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                let reply = if command.eq_ignore_ascii_case(b"HLEN") {
                    hash_len_replies[position]
                        .take()
                        .expect("valid HLEN command has a batch reply")
                } else {
                    zset_card_replies[position]
                        .take()
                        .expect("valid ZCARD command has a batch reply")
                };
                match reply {
                    Ok(len) => append_integer(&mut out, len as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"HGET")
                || command.eq_ignore_ascii_case(b"HMGET")
                || command.eq_ignore_ascii_case(b"HEXISTS")
                || command.eq_ignore_ascii_case(b"HSTRLEN")
            {
                let valid_arity = if command.eq_ignore_ascii_case(b"HMGET") {
                    args.len() >= 3
                } else {
                    args.len() == 3
                };
                if !valid_arity {
                    append_error(&mut out, "ERR wrong number of arguments for hash read command");
                    continue;
                }
                if args[1..].iter().any(|arg| std::str::from_utf8(arg).is_err()) {
                    append_error(&mut out, "ERR invalid UTF-8 hash key or field");
                    continue;
                }
                match hash_batch_replies[position]
                    .take()
                    .expect("valid Hash read command has a batch reply")
                {
                    Ok(values) if command.eq_ignore_ascii_case(b"HMGET") => {
                        append_array_len(&mut out, values.len());
                        for value in values {
                            if let Some(value) = value {
                                append_bulk_string(&mut out, &value);
                            } else {
                                append_null(&mut out);
                            }
                        }
                    }
                    Ok(values) if command.eq_ignore_ascii_case(b"HGET") => {
                        if let Some(value) = values.into_iter().next().flatten() {
                            append_bulk_string(&mut out, &value);
                        } else {
                            append_null(&mut out);
                        }
                    }
                    Ok(values) if command.eq_ignore_ascii_case(b"HEXISTS") => append_integer(
                        &mut out,
                        values.first().is_some_and(Option::is_some) as i64,
                    ),
                    Ok(values) => append_integer(
                        &mut out,
                        values
                            .into_iter()
                            .next()
                            .flatten()
                            .map_or(0, |value| value.len()) as i64,
                    ),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"SISMEMBER") {
                if args.len() != 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'sismember' command",
                    );
                    continue;
                }
                if args[1..].iter().any(|arg| std::str::from_utf8(arg).is_err()) {
                    append_error(&mut out, "ERR invalid UTF-8 set key or member");
                    continue;
                }
                match set_batch_replies[position]
                    .take()
                    .expect("valid SISMEMBER command has a batch reply")
                {
                    Ok(results) => append_integer(&mut out, results[0] as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"SMISMEMBER") {
                if args.len() < 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'smismember' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                if args[2..]
                    .iter()
                    .map(|member| std::str::from_utf8(member).ok().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
                    .is_none()
                {
                    append_error(&mut out, "ERR invalid UTF-8 set member");
                    continue;
                }
                match set_batch_replies[position]
                    .take()
                    .expect("valid SMISMEMBER command has a batch reply")
                {
                    Ok(results) => {
                        append_array_len(&mut out, results.len());
                        for present in results {
                            append_integer(&mut out, present as i64);
                        }
                    }
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"ZSCORE") {
                if args.len() != 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'zscore' command",
                    );
                    continue;
                }
                if args[1..].iter().any(|arg| std::str::from_utf8(arg).is_err()) {
                    append_error(&mut out, "ERR invalid UTF-8 sorted-set key or member");
                    continue;
                }
                match zset_batch_replies[position]
                    .take()
                    .expect("valid ZSCORE command has a batch reply")
                {
                    Ok(scores) => {
                        if let Some(score) = scores[0] {
                            append_bulk_string(&mut out, score.to_string().as_bytes());
                        } else {
                            append_null(&mut out);
                        }
                    }
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"ZMSCORE") {
                if args.len() < 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'zmscore' command",
                    );
                    continue;
                }
                if std::str::from_utf8(args[1]).is_err() {
                    append_error(&mut out, "ERR invalid UTF-8 key");
                    continue;
                }
                if args[2..]
                    .iter()
                    .map(|member| std::str::from_utf8(member).ok().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
                    .is_none()
                {
                    append_error(&mut out, "ERR invalid UTF-8 sorted-set member");
                    continue;
                }
                match zset_batch_replies[position]
                    .take()
                    .expect("valid ZMSCORE command has a batch reply")
                {
                    Ok(scores) => {
                        append_array_len(&mut out, scores.len());
                        for score in scores {
                            if let Some(score) = score {
                                append_bulk_string(&mut out, score.to_string().as_bytes());
                            } else {
                                append_null(&mut out);
                            }
                        }
                    }
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"PFCOUNT") {
                if args.len() < 2 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'pfcount' command",
                    );
                    continue;
                }
                if args[1..].iter().any(|arg| std::str::from_utf8(arg).is_err()) {
                    append_error(&mut out, "ERR command arguments must be valid UTF-8");
                    continue;
                }
                match hll_count_replies[position]
                    .take()
                    .expect("valid PFCOUNT command has a batch reply")
                {
                    Ok(count) => append_integer(&mut out, count as i64),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            } else if command.eq_ignore_ascii_case(b"JSON.GET") {
                if args.len() != 2 && args.len() != 3 {
                    append_error(
                        &mut out,
                        "ERR wrong number of arguments for 'json.get' command",
                    );
                    continue;
                }
                if args[1..].iter().any(|arg| std::str::from_utf8(arg).is_err()) {
                    append_error(&mut out, "ERR invalid UTF-8 key or JSON path");
                    continue;
                }
                match json_get_replies[position]
                    .take()
                    .expect("valid JSON.GET command has a batch reply")
                {
                    Ok(Some(value)) => append_bulk_string(&mut out, value.as_bytes()),
                    Ok(None) => append_null(&mut out),
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
            }
        }
        out
    }
}
