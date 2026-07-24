use super::*;

pub(super) fn wasm_error_frame(error: Error) -> Frame {
    Frame::Error(error.to_string().replace(['\r', '\n'], " "))
}

pub(super) fn wasm_values_frame(values: Vec<WasmValue>) -> Result<Frame> {
    if values.len() > (MAX_FRAME_NODES - 1) / 3 || values.len() > MAX_ARRAY_ELEMENTS {
        return Err(Error::msg("ERR wasm response has too many values"));
    }
    let mut encoded_bytes = 16usize;
    let mut frames = Vec::with_capacity(values.len());
    for value in values {
        let kind = value.type_name();
        let rendered = value.value_string();
        encoded_bytes = encoded_bytes
            .checked_add(kind.len())
            .and_then(|bytes| bytes.checked_add(rendered.len()))
            .and_then(|bytes| bytes.checked_add(48))
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR wasm response exceeds configured limit"))?;
        frames.push(Frame::Array(vec![
            Frame::bulk_string(kind),
            Frame::bulk_string(rendered),
        ]));
    }
    Ok(Frame::Array(frames))
}
