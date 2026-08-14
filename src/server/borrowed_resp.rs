enum BorrowedLrangeOp {
    Command { key: String, start: i64, stop: i64 },
    Error(String),
}

enum BorrowedCollectionReadOp {
    SetMembers(String),
    HashGetAll(String),
    HashKeys(String),
    HashValues(String),
    Error(String),
}

async fn encode_borrowed_collection_read_ops(
    db: Arc<crate::store::db::Db>,
    ops: Vec<BorrowedCollectionReadOp>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(ops.len() * 128);
    for op in ops {
        let (key, with_fields, with_values) = match op {
            BorrowedCollectionReadOp::SetMembers(key) => {
                match db
                    .set_members_bounded_async(
                        &key,
                        crate::frame::MAX_ARRAY_ELEMENTS,
                        crate::frame::MAX_FRAME_BYTES,
                    )
                    .await
                {
                    Ok(members) => {
                        append_array_len(&mut out, members.len());
                        for member in members {
                            append_bulk_string(&mut out, member.as_bytes());
                        }
                    }
                    Err(error) => append_error(&mut out, &error.to_string()),
                }
                continue;
            }
            BorrowedCollectionReadOp::HashGetAll(key) => (key, true, true),
            BorrowedCollectionReadOp::HashKeys(key) => (key, true, false),
            BorrowedCollectionReadOp::HashValues(key) => (key, false, true),
            BorrowedCollectionReadOp::Error(error) => {
                append_error(&mut out, &error);
                continue;
            }
        };
        match db.hash_get_all_bytes_async(&key).await {
            Ok(entries) => {
                let item_count = entries
                    .len()
                    .checked_mul(usize::from(with_fields) + usize::from(with_values));
                let Some(item_count) =
                    item_count.filter(|count| *count <= crate::frame::MAX_ARRAY_ELEMENTS)
                else {
                    append_error(&mut out, "ERR response exceeds configured limit");
                    continue;
                };
                let response_start = out.len();
                append_array_len(&mut out, item_count);
                for (field, value) in entries {
                    if with_fields {
                        append_bulk_string(&mut out, field.as_bytes());
                    }
                    if with_values {
                        append_bulk_string(&mut out, &value);
                    }
                    if out.len().saturating_sub(response_start) > crate::frame::MAX_FRAME_BYTES {
                        break;
                    }
                }
                if out.len().saturating_sub(response_start) > crate::frame::MAX_FRAME_BYTES {
                    out.truncate(response_start);
                    append_error(&mut out, "ERR response exceeds configured limit");
                }
            }
            Err(error) => append_error(&mut out, &error.to_string()),
        }
    }
    out
}

async fn encode_borrowed_lrange_ops(
    db: Arc<crate::store::db::Db>,
    ops: Vec<BorrowedLrangeOp>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(ops.len() * 128);
    for op in ops {
        let (key, start, stop) = match op {
            BorrowedLrangeOp::Command { key, start, stop } => (key, start, stop),
            BorrowedLrangeOp::Error(error) => {
                append_error(&mut out, &error);
                continue;
            }
        };
        let mut body = Vec::with_capacity(4096);
        match db
            .list_range_visit_bytes_async(&key, start, stop, |value| {
                append_bulk_string(&mut body, value);
                true
            })
            .await
        {
            Ok(count) => {
                append_array_len(&mut out, count);
                out.extend_from_slice(&body);
            }
            Err(error) => append_error(&mut out, &error.to_string()),
        }
    }
    out
}

#[cfg(test)]
fn format_command_for_monitor(command: &Command) -> String {
    format_command_name_for_monitor(command.effective_name())
}

#[cfg(test)]
fn format_command_name_for_monitor(command_name: &str) -> String {
    format_command_name_for_monitor_context(command_name, 0, "127.0.0.1:0")
}

fn format_command_name_for_monitor_context(
    command_name: &str,
    db_index: usize,
    peer_addr: &str,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "{}.{:06} [{} {}] \"{}\"",
        now.as_secs(),
        now.subsec_micros(),
        db_index,
        peer_addr,
        command_name.to_ascii_lowercase()
    )
}
