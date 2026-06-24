use super::*;

pub(super) fn wasm_error_frame(error: Error) -> Frame {
    Frame::Error(error.to_string().replace(['\r', '\n'], " "))
}

pub(super) fn wasm_values_frame(values: Vec<WasmValue>) -> Frame {
    Frame::Array(
        values
            .into_iter()
            .map(|value| {
                Frame::Array(vec![
                    Frame::bulk_string(value.type_name()),
                    Frame::bulk_string(value.value_string()),
                ])
            })
            .collect(),
    )
}
