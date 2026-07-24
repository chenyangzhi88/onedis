fn arg(frame: &Frame, idx: usize, error: &'static str) -> Result<String, Error> {
    frame.get_arg(idx).ok_or_else(|| Error::msg(error))
}

fn parse_index_only(frame: Frame, command: &'static str) -> Result<String, Error> {
    if frame.arg_len() != 2 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    arg(&frame, 1, "ERR invalid vector index")
}

fn upper_arg(frame: &Frame, idx: usize) -> Result<String, Error> {
    Ok(arg(frame, idx, "ERR syntax error")?.to_ascii_uppercase())
}

fn parse_usize_arg(frame: &Frame, idx: usize, error: &'static str) -> Result<usize, Error> {
    arg(frame, idx, error)?
        .parse::<usize>()
        .map_err(|_| Error::msg(error))
}

fn parse_f32_arg(frame: &Frame, idx: usize, error: &'static str) -> Result<f32, Error> {
    let value = arg(frame, idx, error)?
        .parse::<f32>()
        .map_err(|_| Error::msg(error))?;
    if !value.is_finite() {
        return Err(Error::msg(error));
    }
    Ok(value)
}

fn parse_redis_vector_arg(frame: &Frame, idx: &mut usize) -> Result<Vec<f32>, Error> {
    match upper_arg(frame, *idx)?.as_str() {
        "FP32" => {
            let bytes = frame
                .get_arg_bytes(*idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid vector blob"))?;
            *idx += 2;
            parse_vector_blob(&bytes)
        }
        "VALUES" => {
            let count = parse_usize_arg(frame, *idx + 1, "ERR invalid vector VALUES")?;
            *idx += 2;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(parse_f32_arg(frame, *idx, "ERR invalid vector value")?);
                *idx += 1;
            }
            if values.is_empty() {
                return Err(Error::msg("ERR vector VALUES cannot be empty"));
            }
            Ok(values)
        }
        _ => Err(Error::msg("ERR missing vector payload")),
    }
}

fn parse_vector_blob(bytes: &[u8]) -> Result<Vec<f32>, Error> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return Err(Error::msg("ERR invalid vector blob length"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("chunk length is 4")))
        .collect())
}
