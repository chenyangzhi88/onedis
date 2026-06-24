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
    if distance <= 0.0 {
        1.0
    } else {
        (1.0 / (1.0 + distance)).clamp(0.0, 1.0)
    }
}

fn redis_vsim_results_frame(
    db: &Db,
    key: &str,
    results: Vec<VectorSearchResult>,
    with_scores: bool,
    with_attrs: bool,
) -> Result<Frame, Error> {
    let mut frames = Vec::new();
    for result in results {
        frames.push(Frame::bulk_string(result.id.clone()));
        if with_scores {
            frames.push(Frame::bulk_string(format_float(vector_similarity_score(
                result.score,
            ))));
        }
        if with_attrs {
            frames.push(redis_attr_frame(
                db.vector_element(key, &result.id)?
                    .map(|element| element.attrs_json),
            ));
        }
    }
    Ok(Frame::Array(frames))
}

fn redis_vrandmember_frame(ids: Vec<String>, count: Option<i64>) -> Frame {
    if ids.is_empty() {
        return count.map_or(Frame::Null, |_| Frame::Array(Vec::new()));
    }
    let Some(count) = count else {
        return Frame::bulk_string(ids[0].clone());
    };
    if count == 0 {
        return Frame::Array(Vec::new());
    }
    let mut out = Vec::new();
    if count > 0 {
        for id in ids.into_iter().take(count as usize) {
            out.push(Frame::bulk_string(id));
        }
    } else {
        let count = count.unsigned_abs() as usize;
        for idx in 0..count {
            out.push(Frame::bulk_string(ids[idx % ids.len()].clone()));
        }
    }
    Frame::Array(out)
}

fn redis_vlinks_frame(results: Vec<VectorSearchResult>, element: &str, with_scores: bool) -> Frame {
    let layer = results
        .into_iter()
        .filter(|result| result.id != element)
        .take(16)
        .flat_map(|result| {
            let mut frames = vec![Frame::bulk_string(result.id)];
            if with_scores {
                frames.push(Frame::bulk_string(format_float(vector_similarity_score(
                    result.score,
                ))));
            }
            frames
        })
        .collect::<Vec<_>>();
    Frame::Array(vec![Frame::Array(layer)])
}

async fn redis_vsim_results_frame_async(
    db: &Db,
    key: &str,
    results: Vec<VectorSearchResult>,
    with_scores: bool,
    with_attrs: bool,
) -> Result<Frame, Error> {
    let mut frames = Vec::new();
    for result in results {
        frames.push(Frame::bulk_string(result.id.clone()));
        if with_scores {
            frames.push(Frame::bulk_string(format_float(vector_similarity_score(
                result.score,
            ))));
        }
        if with_attrs {
            frames.push(redis_attr_frame(
                db.vector_element_async(key, &result.id)
                    .await?
                    .map(|element| element.attrs_json),
            ));
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
    let text = format!("{value:.6}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}
