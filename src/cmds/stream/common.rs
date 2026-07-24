use crate::{frame::Frame, store::db::StreamEntry};

pub(crate) fn stream_entry_frame(entry: StreamEntry) -> Frame {
    let mut field_values = Vec::with_capacity(entry.fields.len() * 2);
    for (field, value) in entry.fields {
        field_values.push(Frame::bulk_string(field));
        field_values.push(Frame::bulk_string(value));
    }
    Frame::Array(vec![
        Frame::bulk_string(entry.id),
        Frame::Array(field_values),
    ])
}
