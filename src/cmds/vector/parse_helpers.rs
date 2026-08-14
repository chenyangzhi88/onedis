fn arg(frame: &Frame, idx: usize, error: &'static str) -> Result<String, Error> {
    frame.get_arg(idx).ok_or_else(|| Error::msg(error))
}

fn vector_key_arg(frame: &Frame, idx: usize) -> Result<String, Error> {
    let key = arg(frame, idx, "ERR invalid vector key")?;
    if key.starts_with("__onedis_fulltext_vector__:") {
        return Err(Error::msg("ERR reserved internal vector key"));
    }
    Ok(key)
}

fn parse_index_only(frame: Frame, command: &'static str) -> Result<String, Error> {
    if frame.arg_len() != 2 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    vector_key_arg(&frame, 1)
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

fn validate_reduce_dimensions(input_dim: usize, output_dim: usize) -> Result<(), Error> {
    if output_dim == 0
        || output_dim >= input_dim
        || input_dim
            .checked_mul(output_dim)
            .is_none_or(|cells| cells > 16 * 1024 * 1024)
    {
        return Err(Error::msg("ERR invalid vector REDUCE dimension"));
    }
    Ok(())
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
            if count == 0 || count > MAX_VECTOR_DIMENSIONS {
                return Err(Error::msg("ERR invalid vector dimension"));
            }
            *idx += 2;
            let values_end = (*idx)
                .checked_add(count)
                .filter(|end| *end <= frame.arg_len())
                .ok_or_else(|| Error::msg("ERR invalid vector VALUES"))?;
            let mut values = Vec::with_capacity(count);
            while *idx < values_end {
                values.push(parse_f32_arg(frame, *idx, "ERR invalid vector value")?);
                *idx += 1;
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
    let dimensions = bytes.len() / 4;
    if dimensions > MAX_VECTOR_DIMENSIONS {
        return Err(Error::msg("ERR invalid vector dimension"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}
