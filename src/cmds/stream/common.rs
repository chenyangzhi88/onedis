use anyhow::Error;

use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES, MAX_FRAME_NODES},
    store::db::{
        StreamConsumerInfo, StreamEntry, StreamGroupInfo, StreamPendingEntry, StreamPendingSummary,
    },
};

pub(crate) fn text_arg(frame: &Frame, index: usize) -> Result<String, Error> {
    frame
        .get_arg(index)
        .ok_or_else(|| Error::msg("ERR invalid UTF-8 argument"))
}

pub(crate) fn validate_count(count: usize) -> Result<(), Error> {
    if count > MAX_ARRAY_ELEMENTS || count > (MAX_FRAME_NODES - 1) / 5 {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

pub(crate) fn stream_entries_frame(entries: Vec<StreamEntry>) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(entries.len())?;
    let frames = entries
        .into_iter()
        .map(|entry| budget.entry(entry))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Frame::Array(frames))
}

pub(crate) fn stream_reads_frame(streams: Vec<(String, Vec<StreamEntry>)>) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(streams.len())?;
    let mut stream_frames = Vec::with_capacity(streams.len());
    for (key, entries) in streams {
        budget.array(2)?;
        let key = budget.bulk(key)?;
        budget.array(entries.len())?;
        let entries = entries
            .into_iter()
            .map(|entry| budget.entry(entry))
            .collect::<Result<Vec<_>, _>>()?;
        stream_frames.push(Frame::Array(vec![key, Frame::Array(entries)]));
    }
    Ok(Frame::Array(stream_frames))
}

pub(crate) fn stream_claimed_frame(
    next: String,
    entries: Vec<StreamEntry>,
    just_id: bool,
) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(3)?;
    let next = budget.bulk(next)?;
    budget.array(entries.len())?;
    let entries = if just_id {
        entries
            .into_iter()
            .map(|entry| budget.bulk(entry.id))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        entries
            .into_iter()
            .map(|entry| budget.entry(entry))
            .collect::<Result<Vec<_>, _>>()?
    };
    budget.array(0)?;
    Ok(Frame::Array(vec![
        next,
        Frame::Array(entries),
        Frame::Array(Vec::new()),
    ]))
}

pub(crate) fn stream_string_array(values: Vec<String>) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(values.len())?;
    let values = values
        .into_iter()
        .map(|value| budget.bulk(value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Frame::Array(values))
}

pub(crate) fn stream_groups_frame(groups: Vec<StreamGroupInfo>) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(groups.len())?;
    let mut frames = Vec::with_capacity(groups.len());
    for group in groups {
        budget.array(10)?;
        frames.push(Frame::Array(vec![
            budget.bulk("name")?,
            budget.bulk(group.name)?,
            budget.bulk("consumers")?,
            budget.integer(group.consumers as i64)?,
            budget.bulk("pending")?,
            budget.integer(group.pending as i64)?,
            budget.bulk("last-delivered-id")?,
            budget.bulk(group.last_delivered_id)?,
            budget.bulk("entries-read")?,
            budget.integer(group.entries_read as i64)?,
        ]));
    }
    Ok(Frame::Array(frames))
}

pub(crate) fn stream_consumers_frame(consumers: Vec<StreamConsumerInfo>) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(consumers.len())?;
    let mut frames = Vec::with_capacity(consumers.len());
    for consumer in consumers {
        budget.array(6)?;
        frames.push(Frame::Array(vec![
            budget.bulk("name")?,
            budget.bulk(consumer.name)?,
            budget.bulk("pending")?,
            budget.integer(consumer.pending as i64)?,
            budget.bulk("idle")?,
            budget.integer(consumer.idle_ms as i64)?,
        ]));
    }
    Ok(Frame::Array(frames))
}

pub(crate) fn stream_pending_summary_frame(summary: StreamPendingSummary) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(4)?;
    let total = budget.integer(summary.total as i64)?;
    let smallest = match summary.smallest_id {
        Some(id) => budget.bulk(id)?,
        None => budget.null()?,
    };
    let greatest = match summary.greatest_id {
        Some(id) => budget.bulk(id)?,
        None => budget.null()?,
    };
    budget.array(summary.consumers.len())?;
    let mut consumers = Vec::with_capacity(summary.consumers.len());
    for (name, count) in summary.consumers {
        budget.array(2)?;
        consumers.push(Frame::Array(vec![
            budget.bulk(name)?,
            budget.integer(count as i64)?,
        ]));
    }
    Ok(Frame::Array(vec![
        total,
        smallest,
        greatest,
        Frame::Array(consumers),
    ]))
}

pub(crate) fn stream_pending_entries_frame(
    entries: Vec<StreamPendingEntry>,
) -> Result<Frame, Error> {
    let mut budget = StreamFrameBudget::default();
    budget.array(entries.len())?;
    let mut frames = Vec::with_capacity(entries.len());
    for entry in entries {
        budget.array(4)?;
        frames.push(Frame::Array(vec![
            budget.bulk(entry.id)?,
            budget.bulk(entry.consumer)?,
            budget.integer(entry.idle_ms as i64)?,
            budget.integer(entry.deliveries as i64)?,
        ]));
    }
    Ok(Frame::Array(frames))
}

#[derive(Default)]
struct StreamFrameBudget {
    nodes: usize,
    bytes: usize,
}

impl StreamFrameBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= MAX_FRAME_NODES)
            .ok_or_else(response_limit_error)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(response_limit_error)?;
        Ok(())
    }

    fn array(&mut self, len: usize) -> Result<(), Error> {
        if len > MAX_ARRAY_ELEMENTS {
            return Err(response_limit_error());
        }
        self.charge(len.to_string().len().saturating_add(3))
    }

    fn bulk(&mut self, value: impl Into<Vec<u8>>) -> Result<Frame, Error> {
        let value = value.into();
        let bytes = value
            .len()
            .checked_add(value.len().to_string().len())
            .and_then(|bytes| bytes.checked_add(5))
            .ok_or_else(response_limit_error)?;
        self.charge(bytes)?;
        Ok(Frame::BulkString(value))
    }

    fn integer(&mut self, value: i64) -> Result<Frame, Error> {
        self.charge(value.to_string().len().saturating_add(3))?;
        Ok(Frame::Integer(value))
    }

    fn null(&mut self) -> Result<Frame, Error> {
        self.charge(5)?;
        Ok(Frame::Null)
    }

    fn entry(&mut self, entry: StreamEntry) -> Result<Frame, Error> {
        self.array(2)?;
        let id = self.bulk(entry.id)?;
        let field_count = entry
            .fields
            .len()
            .checked_mul(2)
            .ok_or_else(response_limit_error)?;
        self.array(field_count)?;
        let mut field_values = Vec::with_capacity(field_count);
        for (field, value) in entry.fields {
            field_values.push(self.bulk(field)?);
            field_values.push(self.bulk(value)?);
        }
        Ok(Frame::Array(vec![id, Frame::Array(field_values)]))
    }
}

fn response_limit_error() -> Error {
    Error::msg("ERR stream response exceeds configured limit")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_response_budget_bounds_counts_nodes_and_bytes() {
        assert!(validate_count((MAX_FRAME_NODES - 1) / 5).is_ok());
        assert!(validate_count((MAX_FRAME_NODES - 1) / 5 + 1).is_err());

        let mut bytes = StreamFrameBudget {
            nodes: 0,
            bytes: MAX_FRAME_BYTES,
        };
        assert!(bytes.bulk("x").is_err());

        let mut nodes = StreamFrameBudget {
            nodes: MAX_FRAME_NODES,
            bytes: 0,
        };
        assert!(nodes.array(0).is_err());
    }

    #[test]
    fn stream_nested_response_builder_preserves_entry_shape() {
        let frame = stream_reads_frame(vec![(
            "stream".to_string(),
            vec![StreamEntry {
                id: "1-0".to_string(),
                fields: vec![("f".to_string(), "v".to_string())],
            }],
        )])
        .unwrap();
        assert!(matches!(
            frame,
            Frame::Array(streams)
                if matches!(
                    streams.as_slice(),
                    [Frame::Array(pair)]
                        if matches!(pair.as_slice(), [Frame::BulkString(key), Frame::Array(entries)] if key == b"stream" && entries.len() == 1)
                )
        ));
    }
}
