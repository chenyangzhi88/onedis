use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, VectorSearchOptions, VectorSearchResult},
};

pub struct VAdd {
    pub key: String,
    pub element: String,
    pub vector: Vec<f32>,
    pub attrs_json: Option<String>,
    pub m: Option<usize>,
    pub ef: Option<usize>,
}

pub struct VSim {
    pub key: String,
    pub query: VSimQuery,
    pub with_scores: bool,
    pub with_attrs: bool,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub epsilon: Option<f32>,
}

pub enum VSimQuery {
    Element(String),
    Vector(Vec<f32>),
}

pub struct VRem {
    pub key: String,
    pub element: String,
}

pub struct VCard {
    pub key: String,
}

pub struct VDim {
    pub key: String,
}

pub struct VEmb {
    pub key: String,
    pub element: String,
    pub raw: bool,
}

pub struct VGetAttr {
    pub key: String,
    pub element: String,
}

pub struct VSetAttr {
    pub key: String,
    pub element: String,
    pub attrs_json: Option<String>,
}

pub struct VInfo {
    pub key: String,
}

pub struct VRandMember {
    pub key: String,
    pub count: Option<i64>,
}

pub struct VLinks {
    pub key: String,
    pub element: String,
    pub with_scores: bool,
}

impl VAdd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vadd' command",
            ));
        }
        let key = arg(&frame, 1, "ERR invalid vector key")?;
        let mut idx = 2;
        let mut reduce = None;
        if upper_arg(&frame, idx)? == "REDUCE" {
            reduce = Some(parse_usize_arg(
                &frame,
                idx + 1,
                "ERR invalid vector REDUCE",
            )?);
            idx += 2;
        }
        let mut vector = parse_redis_vector_arg(&frame, &mut idx)?;
        if let Some(dim) = reduce {
            if dim == 0 || dim > vector.len() {
                return Err(Error::msg("ERR invalid vector REDUCE"));
            }
            vector.truncate(dim);
        }
        let element = arg(&frame, idx, "ERR invalid vector element")?;
        idx += 1;
        let mut attrs_json = None;
        let mut m = None;
        let mut ef = None;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "CAS" | "NOQUANT" | "Q8" | "BIN" => idx += 1,
                "SETATTR" => {
                    let attrs = arg(&frame, idx + 1, "ERR invalid vector attrs")?;
                    attrs_json = (!attrs.is_empty()).then_some(attrs);
                    idx += 2;
                }
                "EF" => {
                    ef = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector EF")?);
                    idx += 2;
                }
                "M" => {
                    m = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector M")?);
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key,
            element,
            vector,
            attrs_json,
            m,
            ef,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            if db.vector_add_autocreate(
                &self.key,
                &self.element,
                self.vector,
                self.attrs_json,
                self.m,
                self.ef,
            )? {
                1
            } else {
                0
            },
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            if db
                .vector_add_autocreate_async(
                    &self.key,
                    &self.element,
                    self.vector,
                    self.attrs_json,
                    self.m,
                    self.ef,
                )
                .await?
            {
                1
            } else {
                0
            },
        ))
    }
}

impl VSim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vsim' command",
            ));
        }
        let key = arg(&frame, 1, "ERR invalid vector key")?;
        let mut idx = 2;
        let query = match upper_arg(&frame, idx)?.as_str() {
            "ELE" => {
                let element = arg(&frame, idx + 1, "ERR invalid vector element")?;
                idx += 2;
                VSimQuery::Element(element)
            }
            "FP32" | "VALUES" => VSimQuery::Vector(parse_redis_vector_arg(&frame, &mut idx)?),
            _ => return Err(Error::msg("ERR syntax error")),
        };
        let mut with_scores = false;
        let mut with_attrs = false;
        let mut count = 10usize;
        let mut ef = None;
        let mut filter = None;
        let mut epsilon = None;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "WITHSCORES" => {
                    with_scores = true;
                    idx += 1;
                }
                "WITHATTRIBS" => {
                    with_attrs = true;
                    idx += 1;
                }
                "COUNT" => {
                    count = parse_usize_arg(&frame, idx + 1, "ERR invalid vector COUNT")?;
                    idx += 2;
                }
                "EF" => {
                    ef = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid vector EF")?);
                    idx += 2;
                }
                "FILTER" => {
                    filter = Some(arg(&frame, idx + 1, "ERR invalid vector filter")?);
                    idx += 2;
                }
                "EPSILON" => {
                    epsilon = Some(parse_f32_arg(
                        &frame,
                        idx + 1,
                        "ERR invalid vector EPSILON",
                    )?);
                    idx += 2;
                }
                "FILTER-EF" => {
                    let _ = parse_usize_arg(&frame, idx + 1, "ERR invalid vector FILTER-EF")?;
                    idx += 2;
                }
                "TRUTH" | "NOTHREAD" => idx += 1,
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key,
            query,
            with_scores,
            with_attrs,
            count,
            ef,
            filter,
            epsilon,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let vector = match &self.query {
            VSimQuery::Vector(vector) => vector.clone(),
            VSimQuery::Element(element) => {
                db.vector_element(&self.key, element)?
                    .ok_or_else(|| Error::msg("ERR vector element does not exist"))?
                    .vector
            }
        };
        let options = VectorSearchOptions {
            k: self.count,
            filter: self.filter.clone(),
            with_scores: false,
            with_attrs: Vec::new(),
            ef: self.ef,
            offset: 0,
            limit: Some(self.count),
        };
        let mut results = db.vector_search(&self.key, &vector, options)?;
        if let Some(epsilon) = self.epsilon {
            results.retain(|result| vector_similarity_score(result.score) >= 1.0 - epsilon);
        }
        redis_vsim_results_frame(db, &self.key, results, self.with_scores, self.with_attrs)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let vector = match &self.query {
            VSimQuery::Vector(vector) => vector.clone(),
            VSimQuery::Element(element) => {
                db.vector_element_async(&self.key, element)
                    .await?
                    .ok_or_else(|| Error::msg("ERR vector element does not exist"))?
                    .vector
            }
        };
        let options = VectorSearchOptions {
            k: self.count,
            filter: self.filter.clone(),
            with_scores: false,
            with_attrs: Vec::new(),
            ef: self.ef,
            offset: 0,
            limit: Some(self.count),
        };
        let mut results = db.vector_search_async(&self.key, &vector, options).await?;
        if let Some(epsilon) = self.epsilon {
            results.retain(|result| vector_similarity_score(result.score) >= 1.0 - epsilon);
        }
        redis_vsim_results_frame_async(db, &self.key, results, self.with_scores, self.with_attrs)
            .await
    }
}

impl VRem {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vrem' command",
            ));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_del(&self.key, &[self.element])? as i64
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_del_async(&self.key, &[self.element]).await? as i64,
        ))
    }
}

impl VCard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vcard")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(db.vector_card(&self.key)? as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_card_async(&self.key).await? as i64,
        ))
    }
}

impl VDim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vdim")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(db
            .vector_dim(&self.key)?
            .map(|dim| Frame::Integer(dim as i64))
            .unwrap_or(Frame::Null))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(db
            .vector_dim_async(&self.key)
            .await?
            .map(|dim| Frame::Integer(dim as i64))
            .unwrap_or(Frame::Null))
    }
}

impl VEmb {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 || frame.arg_len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vemb' command",
            ));
        }
        let raw = frame.arg_len() == 4 && upper_arg(&frame, 3)? == "RAW";
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            raw,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(match db.vector_element(&self.key, &self.element)? {
            Some(element) => vector_embedding_frame(element.vector, self.raw),
            None => Frame::Null,
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(
            match db.vector_element_async(&self.key, &self.element).await? {
                Some(element) => vector_embedding_frame(element.vector, self.raw),
                None => Frame::Null,
            },
        )
    }
}

impl VGetAttr {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vgetattr' command",
            ));
        }
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_attr_frame(
            db.vector_element(&self.key, &self.element)?
                .map(|element| element.attrs_json),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_attr_frame(
            db.vector_element_async(&self.key, &self.element)
                .await?
                .map(|element| element.attrs_json),
        ))
    }
}

impl VSetAttr {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vsetattr' command",
            ));
        }
        let attrs = arg(&frame, 3, "ERR invalid vector attrs")?;
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            attrs_json: (!attrs.is_empty()).then_some(attrs),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_set_attrs(&self.key, &self.element, self.attrs_json)? as i64,
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.vector_set_attrs_async(&self.key, &self.element, self.attrs_json)
                .await? as i64,
        ))
    }
}

impl VInfo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            key: parse_index_only(frame, "vinfo")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(info_frame(db.vector_info(&self.key)?))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(info_frame(db.vector_info_async(&self.key).await?))
    }
}

impl VRandMember {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vrandmember' command",
            ));
        }
        let count = if frame.arg_len() == 3 {
            Some(
                arg(&frame, 2, "ERR invalid vector count")?
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR invalid vector count"))?,
            )
        } else {
            None
        };
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_vrandmember_frame(
            db.vector_ids(&self.key)?,
            self.count,
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(redis_vrandmember_frame(
            db.vector_ids_async(&self.key).await?,
            self.count,
        ))
    }
}

impl VLinks {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 || frame.arg_len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'vlinks' command",
            ));
        }
        let with_scores = frame.arg_len() == 4 && upper_arg(&frame, 3)? == "WITHSCORES";
        Ok(Self {
            key: arg(&frame, 1, "ERR invalid vector key")?,
            element: arg(&frame, 2, "ERR invalid vector element")?,
            with_scores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let element = db
            .vector_element(&self.key, &self.element)?
            .ok_or_else(|| Error::msg("ERR vector element does not exist"))?;
        let results = db.vector_search(
            &self.key,
            &element.vector,
            VectorSearchOptions {
                k: 17,
                filter: None,
                with_scores: false,
                with_attrs: Vec::new(),
                ef: None,
                offset: 0,
                limit: Some(17),
            },
        )?;
        Ok(redis_vlinks_frame(results, &self.element, self.with_scores))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let element = db
            .vector_element_async(&self.key, &self.element)
            .await?
            .ok_or_else(|| Error::msg("ERR vector element does not exist"))?;
        let results = db
            .vector_search_async(
                &self.key,
                &element.vector,
                VectorSearchOptions {
                    k: 17,
                    filter: None,
                    with_scores: false,
                    with_attrs: Vec::new(),
                    ef: None,
                    offset: 0,
                    limit: Some(17),
                },
            )
            .await?;
        Ok(redis_vlinks_frame(results, &self.element, self.with_scores))
    }
}

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
    if !bytes.len().is_multiple_of(4) {
        return Err(Error::msg("ERR invalid vector blob length"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("chunk length is 4")))
        .collect())
}

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
