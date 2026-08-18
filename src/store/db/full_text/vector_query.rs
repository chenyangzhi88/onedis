use super::*;
use crate::store::db::VectorQuantization;
use half::{bf16, f16};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FullTextVectorElementType {
    Float32,
    Float64,
    BFloat16,
    Float16,
    Int8,
    UInt8,
}

impl FullTextVectorElementType {
    fn byte_width(self) -> usize {
        match self {
            Self::Float64 => 8,
            Self::Float32 => 4,
            Self::BFloat16 | Self::Float16 => 2,
            Self::Int8 | Self::UInt8 => 1,
        }
    }
}
pub(super) fn fulltext_vector_plan(ast: &FullTextQueryAst) -> Result<FullTextVectorPlan, Error> {
    match ast {
        FullTextQueryAst::VectorKnn {
            filter,
            k,
            field,
            blob_param,
        } => Ok(FullTextVectorPlan {
            kind: FullTextVectorPlanKind::Knn { k: *k },
            filter: (!matches!(filter.as_ref(), FullTextQueryAst::All))
                .then(|| filter.as_ref().clone()),
            field: field.clone(),
            blob_param: blob_param.clone(),
        }),
        FullTextQueryAst::VectorRange {
            field,
            radius,
            blob_param,
        } => Ok(FullTextVectorPlan {
            kind: FullTextVectorPlanKind::Range {
                radius: *radius as f32,
            },
            filter: None,
            field: field.clone(),
            blob_param: blob_param.clone(),
        }),
        FullTextQueryAst::Attributed { expr, .. } => fulltext_vector_plan(expr),
        FullTextQueryAst::Field { fields, expr } => {
            let mut plan = fulltext_vector_plan(expr)?;
            if let Some(filter) = plan.filter.take() {
                plan.filter = Some(FullTextQueryAst::Field {
                    fields: fields.clone(),
                    expr: Box::new(filter),
                });
            }
            Ok(plan)
        }
        FullTextQueryAst::And(children) => {
            let mut vector_plan = None;
            let mut scalar_filters = Vec::new();
            for child in children {
                if contains_fulltext_vector_query(child) {
                    if vector_plan.is_some() {
                        return Err(Error::msg(
                            "ERR multiple vector clauses are not supported in one query",
                        ));
                    }
                    vector_plan = Some(fulltext_vector_plan(child)?);
                } else {
                    scalar_filters.push(child.clone());
                }
            }
            let mut plan = vector_plan.ok_or_else(|| {
                Error::msg("ERR fulltext vector query execution is not implemented")
            })?;
            if let Some(filter) = plan.filter.take() {
                scalar_filters.push(filter);
            }
            plan.filter = fulltext_combine_vector_filters(scalar_filters);
            Ok(plan)
        }
        FullTextQueryAst::Or(_) | FullTextQueryAst::Not(_) | FullTextQueryAst::Optional(_) => Err(
            Error::msg("ERR vector clauses support scalar filters through conjunction only"),
        ),
        _ => Err(Error::msg(
            "ERR fulltext vector query execution is not implemented",
        )),
    }
}

pub(super) fn fulltext_combine_vector_filters(
    mut filters: Vec<FullTextQueryAst>,
) -> Option<FullTextQueryAst> {
    filters.retain(|filter| !matches!(filter, FullTextQueryAst::All));
    match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(FullTextQueryAst::And(filters)),
    }
}

pub(super) fn fulltext_vector_schema_field<'a>(
    meta: &'a FullTextIndexMeta,
    field: &str,
) -> Result<&'a FullTextFieldSchema, Error> {
    meta.schema
        .iter()
        .find(|schema| {
            matches!(schema.kind, FullTextFieldKind::Vector)
                && (schema.name == field || schema.attribute_name() == field)
        })
        .ok_or_else(|| Error::msg("ERR invalid vector field"))
}

pub(super) fn fulltext_vector_index_name(index: &str, generation: u64, field: &str) -> String {
    format!(
        "__onedis_fulltext_vector__:{generation}:{}:{index}:{}:{field}",
        index.len(),
        field.len()
    )
}

pub(super) fn fulltext_vector_create_options(
    field: &FullTextFieldSchema,
) -> Result<VectorCreateOptions, Error> {
    let options = field
        .options
        .vector
        .as_ref()
        .ok_or_else(|| Error::msg("ERR missing VECTOR options"))?;
    Ok(VectorCreateOptions {
        dim: fulltext_vector_attr_usize(options, "DIM")?,
        source_dim: None,
        distance: fulltext_vector_attr(options, "DISTANCE_METRIC")?,
        schema: Vec::new(),
        segment_max_docs: None,
        m: fulltext_vector_attr_optional_usize(options, "M")?,
        ef_construction: fulltext_vector_attr_optional_usize(options, "EF_CONSTRUCTION")?,
        ef_runtime: fulltext_vector_attr_optional_usize(options, "EF_RUNTIME")?,
        initial_cap: fulltext_vector_attr_optional_usize(options, "INITIAL_CAP")?,
        quantization: VectorQuantization::F32,
    })
}

pub(super) fn fulltext_vector_attr(
    options: &FullTextVectorOptions,
    name: &str,
) -> Result<String, Error> {
    options
        .attributes
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
        .ok_or_else(|| Error::msg("ERR missing VECTOR attribute"))
}

pub(super) fn fulltext_vector_attr_usize(
    options: &FullTextVectorOptions,
    name: &str,
) -> Result<usize, Error> {
    fulltext_vector_attr(options, name)?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR invalid VECTOR attribute"))
}

pub(super) fn fulltext_vector_attr_optional_usize(
    options: &FullTextVectorOptions,
    name: &str,
) -> Result<Option<usize>, Error> {
    options
        .attributes
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| {
            value
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR invalid VECTOR attribute"))
        })
        .transpose()
}

#[cfg(test)]
pub(super) fn parse_fulltext_vector_param(
    params: &HashMap<String, Vec<u8>>,
    name: &str,
) -> Result<Vec<f32>, Error> {
    let raw = params
        .get(name)
        .ok_or_else(|| Error::msg("ERR missing query parameter"))?;
    parse_fulltext_vector_bytes(raw)
}

pub(super) fn parse_fulltext_vector_param_for_field(
    params: &HashMap<String, Vec<u8>>,
    name: &str,
    field: &FullTextFieldSchema,
) -> Result<Vec<f32>, Error> {
    let raw = params
        .get(name)
        .ok_or_else(|| Error::msg("ERR missing query parameter"))?;
    parse_fulltext_vector_value(raw, field)
}

pub(super) fn fulltext_vector_element_type(
    field: &FullTextFieldSchema,
) -> Result<FullTextVectorElementType, Error> {
    let options = field
        .options
        .vector
        .as_ref()
        .ok_or_else(|| Error::msg("ERR missing VECTOR options"))?;
    match fulltext_vector_attr(options, "TYPE")?
        .to_ascii_uppercase()
        .as_str()
    {
        "FLOAT32" => Ok(FullTextVectorElementType::Float32),
        "FLOAT64" => Ok(FullTextVectorElementType::Float64),
        "BFLOAT16" => Ok(FullTextVectorElementType::BFloat16),
        "FLOAT16" => Ok(FullTextVectorElementType::Float16),
        "INT8" => Ok(FullTextVectorElementType::Int8),
        "UINT8" => Ok(FullTextVectorElementType::UInt8),
        _ => Err(Error::msg("ERR invalid VECTOR TYPE")),
    }
}

pub(super) fn parse_fulltext_vector_value(
    raw: &[u8],
    field: &FullTextFieldSchema,
) -> Result<Vec<f32>, Error> {
    let options = field
        .options
        .vector
        .as_ref()
        .ok_or_else(|| Error::msg("ERR missing VECTOR options"))?;
    let dim = fulltext_vector_attr_usize(options, "DIM")?;
    let element_type = fulltext_vector_element_type(field)?;
    let binary_len = dim
        .checked_mul(element_type.byte_width())
        .ok_or_else(|| Error::msg("ERR invalid vector blob"))?;
    let text = std::str::from_utf8(raw).ok().map(str::trim);
    let looks_textual = text.is_some_and(|text| {
        (text.starts_with('[') && text.ends_with(']'))
            || text.contains(',')
            || text.split_ascii_whitespace().count() > 1
    });
    let vector = if looks_textual {
        parse_fulltext_vector_text(text.expect("checked textual vector"))?
    } else if raw.len() == binary_len {
        parse_fulltext_vector_binary(raw, element_type)?
    } else if let Some(text) = text {
        parse_fulltext_vector_text(text)?
    } else {
        return Err(Error::msg("ERR invalid vector blob"));
    };
    validate_fulltext_vector_values(vector, dim)
}

fn parse_fulltext_vector_binary(
    raw: &[u8],
    element_type: FullTextVectorElementType,
) -> Result<Vec<f32>, Error> {
    let values = match element_type {
        FullTextVectorElementType::Float32 => raw
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("four-byte chunk")))
            .collect(),
        FullTextVectorElementType::Float64 => raw
            .chunks_exact(8)
            .map(|chunk| f64::from_le_bytes(chunk.try_into().expect("eight-byte chunk")) as f32)
            .collect(),
        FullTextVectorElementType::BFloat16 => raw
            .chunks_exact(2)
            .map(|chunk| bf16::from_bits(u16::from_le_bytes([chunk[0], chunk[1]])).to_f32())
            .collect(),
        FullTextVectorElementType::Float16 => raw
            .chunks_exact(2)
            .map(|chunk| f16::from_bits(u16::from_le_bytes([chunk[0], chunk[1]])).to_f32())
            .collect(),
        FullTextVectorElementType::Int8 => raw.iter().map(|value| (*value as i8) as f32).collect(),
        FullTextVectorElementType::UInt8 => raw.iter().map(|value| *value as f32).collect(),
    };
    Ok(values)
}

fn validate_fulltext_vector_values(vector: Vec<f32>, dim: usize) -> Result<Vec<f32>, Error> {
    if vector.len() != dim || vector.iter().any(|value| !value.is_finite()) {
        return Err(Error::msg("ERR invalid vector blob"));
    }
    Ok(vector)
}

#[cfg(test)]
pub(super) fn parse_fulltext_vector_bytes(raw: &[u8]) -> Result<Vec<f32>, Error> {
    if let Ok(text) = std::str::from_utf8(raw)
        && let Ok(vector) = parse_fulltext_vector_text(text)
    {
        return Ok(vector);
    }
    if raw.is_empty() || !raw.len().is_multiple_of(4) {
        return Err(Error::msg("ERR invalid vector blob"));
    }
    let mut out = Vec::with_capacity(raw.len() / 4);
    for chunk in raw.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

pub(super) fn parse_fulltext_vector_text(raw: &str) -> Result<Vec<f32>, Error> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::msg("ERR invalid vector blob"));
    }
    if let Ok(values) = serde_json::from_str::<Vec<f32>>(trimmed) {
        return Ok(values);
    }
    trimmed
        .trim_matches(|ch| ch == '[' || ch == ']')
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<f32>()
                .map_err(|_| Error::msg("ERR invalid vector blob"))
        })
        .collect()
}

pub(super) fn parse_fulltext_vector_json_value(
    value: &serde_json::Value,
) -> Result<Vec<f32>, Error> {
    match value {
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .map(|number| number as f32)
                    .ok_or_else(|| Error::msg("ERR invalid vector blob"))
            })
            .collect(),
        serde_json::Value::String(value) => parse_fulltext_vector_text(value),
        _ => Err(Error::msg("ERR invalid vector blob")),
    }
}

#[cfg(test)]
pub(super) fn fulltext_vector_distance(
    distance: &str,
    lhs: &[f32],
    rhs: &[f32],
) -> Result<f32, Error> {
    let lhs_norm_squared = lhs.iter().map(|value| value * value).sum::<f32>();
    fulltext_vector_distance_prepared(distance, lhs, lhs_norm_squared, rhs)
}

#[cfg(test)]
pub(super) fn fulltext_vector_distance_prepared(
    distance: &str,
    lhs: &[f32],
    lhs_norm_squared: f32,
    rhs: &[f32],
) -> Result<f32, Error> {
    if lhs.len() != rhs.len() {
        return Err(Error::msg("ERR vector dimension mismatch"));
    }
    match distance.to_ascii_uppercase().as_str() {
        "L2" => Ok(lhs
            .iter()
            .zip(rhs)
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum()),
        "IP" => Ok(-lhs
            .iter()
            .zip(rhs)
            .map(|(left, right)| left * right)
            .sum::<f32>()),
        "COSINE" => {
            let dot = lhs
                .iter()
                .zip(rhs)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let lhs_norm = lhs_norm_squared.sqrt();
            let rhs_norm = rhs.iter().map(|value| value * value).sum::<f32>().sqrt();
            if lhs_norm == 0.0 || rhs_norm == 0.0 {
                return Err(Error::msg("ERR zero norm vector for cosine distance"));
            }
            Ok(1.0 - dot / (lhs_norm * rhs_norm))
        }
        _ => Err(Error::msg("ERR invalid VECTOR DISTANCE_METRIC")),
    }
}
