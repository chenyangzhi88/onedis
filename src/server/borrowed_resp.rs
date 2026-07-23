enum BorrowedLrangeOp {
    Command { key: String, start: i64, stop: i64 },
    Error(String),
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
