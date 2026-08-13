fn vector_embedding_frame(vector: Vec<f32>, raw: bool) -> Frame {
    if raw {
        let mut bytes = Vec::with_capacity(vector.len() * 4);
        for value in vector {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Frame::BulkString(bytes)
    } else {
        Frame::Array(
            vector
                .into_iter()
                .map(|value| Frame::bulk_string(format_float(value)))
                .collect(),
        )
    }
}

fn redis_attr_frame(attrs_json: Option<String>) -> Frame {
    match attrs_json {
        Some(attrs) if attrs != "{}" => Frame::bulk_string(attrs),
        _ => Frame::Null,
    }
}

fn vector_similarity_score(distance: f32) -> f32 {
    // Redis vector sets use cosine distance and expose it as a [0, 1] similarity where
    // identical vectors are 1 and opposite vectors are 0.
    (1.0 - distance / 2.0).clamp(0.0, 1.0)
}

fn redis_vsim_results_frame(
    db: &Db,
    key: &str,
    results: Vec<VectorSearchResult>,
    with_scores: bool,
    with_attrs: bool,
) -> Result<Frame, Error> {
    let multiplier = 1 + usize::from(with_scores) + usize::from(with_attrs);
    validate_vector_response_count(results.len(), multiplier)?;
    let mut frames = Vec::with_capacity(results.len().saturating_mul(multiplier));
    let mut response_bytes = 32usize;
    for result in results {
        response_bytes = response_bytes
            .checked_add(result.id.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(result.id.clone()));
        if with_scores {
            let score = format_float(vector_similarity_score(result.score));
            response_bytes = response_bytes
                .checked_add(score.len().saturating_add(32))
                .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
                .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
            frames.push(Frame::bulk_string(score));
        }
        if with_attrs {
            let attrs = db
                .vector_element(key, &result.id)?
                .map(|element| element.attrs_json);
            response_bytes = response_bytes
                .checked_add(
                    attrs
                        .as_ref()
                        .filter(|attrs| attrs.as_str() != "{}")
                        .map_or(5, |attrs| attrs.len().saturating_add(32)),
                )
                .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
                .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
            frames.push(redis_attr_frame(attrs));
        }
    }
    Ok(Frame::Array(frames))
}

fn redis_vrandmember_frame(ids: Vec<String>, count: Option<i64>) -> Result<Frame, Error> {
    if ids.is_empty() {
        return Ok(count.map_or(Frame::Null, |_| Frame::Array(Vec::new())));
    }
    let Some(count) = count else {
        return Ok(Frame::bulk_string(ids[0].clone()));
    };
    if count == 0 {
        return Ok(Frame::Array(Vec::new()));
    }
    let mut response_bytes = 32usize;
    let mut frames = Vec::with_capacity(ids.len());
    for id in ids {
        response_bytes = response_bytes
            .checked_add(id.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(id));
    }
    Ok(Frame::Array(frames))
}

fn redis_vlinks_frame(layers: Vec<Vec<(String, f32)>>, with_scores: bool) -> Frame {
    Frame::Array(
        layers
            .into_iter()
            .map(|layer| {
                Frame::Array(
                    layer
                        .into_iter()
                        .flat_map(|(id, distance)| {
                            let mut frames = vec![Frame::bulk_string(id)];
                            if with_scores {
                                frames.push(Frame::bulk_string(format_float(
                                    vector_similarity_score(distance),
                                )));
                            }
                            frames
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

async fn redis_vsim_results_frame_async(
    db: &Db,
    key: &str,
    results: Vec<VectorSearchResult>,
    with_scores: bool,
    with_attrs: bool,
) -> Result<Frame, Error> {
    let multiplier = 1 + usize::from(with_scores) + usize::from(with_attrs);
    validate_vector_response_count(results.len(), multiplier)?;
    let attrs = if with_attrs {
        let ids = results
            .iter()
            .map(|result| result.id.clone())
            .collect::<Vec<_>>();
        db.vector_elements_async(key, &ids)
            .await?
            .into_iter()
            .map(|element| element.map(|element| element.attrs_json))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut frames = Vec::with_capacity(results.len().saturating_mul(multiplier));
    let mut response_bytes = 32usize;
    for (position, result) in results.into_iter().enumerate() {
        response_bytes = response_bytes
            .checked_add(result.id.len().saturating_add(32))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(result.id.clone()));
        if with_scores {
            let score = format_float(vector_similarity_score(result.score));
            response_bytes = response_bytes
                .checked_add(score.len().saturating_add(32))
                .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
                .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
            frames.push(Frame::bulk_string(score));
        }
        if with_attrs {
            let attrs = attrs[position].clone();
            response_bytes = response_bytes
                .checked_add(
                    attrs
                        .as_ref()
                        .filter(|attrs| attrs.as_str() != "{}")
                        .map_or(5, |attrs| attrs.len().saturating_add(32)),
                )
                .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
                .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
            frames.push(redis_attr_frame(attrs));
        }
    }
    Ok(Frame::Array(frames))
}

fn info_frame(entries: Vec<(String, String)>) -> Frame {
    Frame::Array(
        entries
            .into_iter()
            .flat_map(|(key, value)| [Frame::bulk_string(key), Frame::bulk_string(value)])
            .collect(),
    )
}

fn format_float(value: f32) -> String {
    value.to_string()
}
