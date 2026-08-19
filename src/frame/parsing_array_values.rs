impl Frame {
    fn parse_aggregate(bytes: &[u8], depth: usize) -> Result<(Frame, usize), Error> {
        if depth >= MAX_ARRAY_NESTING_DEPTH {
            return Err(Error::msg("ERR aggregate nesting exceeds configured limit"));
        }
        let prefix = bytes[0];
        let header_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::msg("Invalid aggregate: missing length terminator"))?;
        let len_str = std::str::from_utf8(&bytes[1..header_end])?;
        let header_len = header_end
            .checked_add(2)
            .ok_or_else(|| Error::msg("Aggregate header length overflow"))?;
        if prefix == b'*' && len_str == "-1" {
            return Ok((Frame::Null, header_len));
        }
        let logical_len = parse_protocol_usize(len_str)
            .ok_or_else(|| Error::msg("Invalid aggregate length"))?;
        if logical_len > MAX_ARRAY_ELEMENTS {
            return Err(Error::msg("ERR aggregate length exceeds configured limit"));
        }

        let pair_count = usize::from(matches!(prefix, b'%' | b'|'));
        let element_count = if pair_count == 1 {
            logical_len
                .checked_mul(2)
                .ok_or_else(|| Error::msg("Aggregate element count overflow"))?
        } else {
            logical_len
        };
        let trailing = usize::from(prefix == b'|');
        let total_count = element_count
            .checked_add(trailing)
            .ok_or_else(|| Error::msg("Aggregate element count overflow"))?;
        if total_count > MAX_FRAME_NODES {
            return Err(Error::msg(
                "ERR aggregate element count exceeds configured limit",
            ));
        }

        let mut frames = Vec::with_capacity(total_count);
        let mut current_pos = header_len;
        for _ in 0..total_count {
            let remaining = bytes
                .get(current_pos..)
                .ok_or_else(|| Error::msg("Incomplete aggregate element"))?;
            let (frame, element_end) = Frame::parse_validated_frame_prefix(remaining, depth + 1)?;
            frames.push(frame);
            current_pos = current_pos
                .checked_add(element_end)
                .ok_or_else(|| Error::msg("Aggregate frame length overflow"))?;
        }

        let frame = match prefix {
            b'*' => Frame::Array(frames),
            b'~' => Frame::Set(frames),
            b'>' => Frame::Push(frames),
            b'%' => Frame::Map(pair_frames(frames)?),
            b'|' => {
                let data = frames
                    .pop()
                    .ok_or_else(|| Error::msg("RESP3 attribute missing data frame"))?;
                Frame::Attribute {
                    attributes: pair_frames(frames)?,
                    data: Box::new(data),
                }
            }
            _ => return Err(Error::msg("ERR invalid aggregate type")),
        };
        Ok((frame, current_pos))
    }
}

fn pair_frames(frames: Vec<Frame>) -> Result<Vec<(Frame, Frame)>, Error> {
    if !frames.len().is_multiple_of(2) {
        return Err(Error::msg("ERR RESP3 map has an odd number of elements"));
    }
    let mut pairs = Vec::with_capacity(frames.len() / 2);
    let mut frames = frames.into_iter();
    while let Some(key) = frames.next() {
        let value = frames
            .next()
            .ok_or_else(|| Error::msg("ERR RESP3 map is missing a value"))?;
        pairs.push((key, value));
    }
    Ok(pairs)
}
