impl Handler {
    #[allow(dead_code)]
    fn execute_transaction_commands(
        txn_db: &crate::store::db::Db,
        parsed_commands: Vec<Command>,
        database_count: usize,
    ) -> Result<Frame, Error> {
        let mut results = Vec::new();
        let mut response_budget = TransactionResponseBudget::new();
        for command in parsed_commands {
            if !Self::can_queue_transaction_command(&command) {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command '{}' is not allowed in MULTI",
                    command.name()
                )));
            }
            if let Command::Move(r#move) = &command
                && database_count <= r#move.get_db_index()
            {
                response_budget.push(
                    &mut results,
                    Frame::Error("ERR DB index is out of range".to_string()),
                );
                continue;
            }
            if let Command::Copy(copy) = &command
                && copy
                    .db_index()
                    .is_some_and(|db_index| database_count <= db_index)
            {
                response_budget.push(
                    &mut results,
                    Frame::Error("ERR DB index is out of range".to_string()),
                );
                continue;
            }

            let frame = match command {
                Command::Unwatch(_) => Frame::Ok,
                command => match crate::command_dispatch::handle_command(txn_db, command) {
                    Ok(frame) => frame,
                    Err(error) => Frame::Error(error.to_string()),
                },
            };
            response_budget.push(&mut results, frame);
        }

        if let Err(error) = txn_db.commit_transaction() {
            return Ok(Frame::Error(format!(
                "EXECABORT Transaction discarded because commit failed: {}",
                error
            )));
        }
        Ok(response_budget.finish(results))
    }

    async fn execute_transaction_commands_async(
        txn_db: &crate::store::db::Db,
        parsed_commands: Vec<Command>,
        database_count: usize,
    ) -> Result<Frame, Error> {
        let mut results = Vec::new();
        let mut response_budget = TransactionResponseBudget::new();
        for command in parsed_commands {
            if !Self::can_queue_transaction_command(&command) {
                return Ok(Frame::Error(format!(
                    "EXECABORT Transaction discarded because command '{}' is not allowed in MULTI",
                    command.name()
                )));
            }
            if let Command::Move(r#move) = &command
                && database_count <= r#move.get_db_index()
            {
                response_budget.push(
                    &mut results,
                    Frame::Error("ERR DB index is out of range".to_string()),
                );
                continue;
            }
            if let Command::Copy(copy) = &command
                && copy
                    .db_index()
                    .is_some_and(|db_index| database_count <= db_index)
            {
                response_budget.push(
                    &mut results,
                    Frame::Error("ERR DB index is out of range".to_string()),
                );
                continue;
            }

            let frame = match command {
                Command::Unwatch(_) => Frame::Ok,
                command => {
                    match crate::command_dispatch::handle_command_async(txn_db, command).await {
                        Ok(frame) => frame,
                        Err(error) => Frame::Error(error.to_string()),
                    }
                }
            };
            response_budget.push(&mut results, frame);
        }

        if let Err(error) = txn_db.commit_transaction_async().await {
            return Ok(Frame::Error(format!(
                "EXECABORT Transaction discarded because commit failed: {}",
                error
            )));
        }
        Ok(response_budget.finish(results))
    }
}

struct TransactionResponseBudget {
    nodes: usize,
    bytes: usize,
    exceeded: bool,
}

impl TransactionResponseBudget {
    fn new() -> Self {
        Self {
            nodes: 1,
            bytes: 16,
            exceeded: false,
        }
    }

    fn push(&mut self, results: &mut Vec<Frame>, frame: Frame) {
        if self.exceeded {
            return;
        }
        let Some((nodes, bytes)) = frame_response_cost(&frame) else {
            self.exceeded = true;
            results.clear();
            return;
        };
        let Some(next_nodes) = self.nodes.checked_add(nodes) else {
            self.exceeded = true;
            results.clear();
            return;
        };
        let Some(next_bytes) = self.bytes.checked_add(bytes) else {
            self.exceeded = true;
            results.clear();
            return;
        };
        if next_nodes > crate::frame::MAX_FRAME_NODES
            || next_bytes > crate::frame::MAX_FRAME_BYTES
        {
            self.exceeded = true;
            results.clear();
            return;
        }
        self.nodes = next_nodes;
        self.bytes = next_bytes;
        results.push(frame);
    }

    fn finish(self, results: Vec<Frame>) -> Frame {
        if self.exceeded {
            Frame::Array(vec![Frame::Error(
                "ERR EXEC response exceeds configured limit".to_string(),
            )])
        } else {
            Frame::Array(results)
        }
    }
}

fn frame_response_cost(frame: &Frame) -> Option<(usize, usize)> {
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    let mut pending = vec![frame];
    while let Some(frame) = pending.pop() {
        nodes = nodes.checked_add(1)?;
        if nodes > crate::frame::MAX_FRAME_NODES {
            return None;
        }
        let cost = match frame {
            Frame::Ok => 5,
            Frame::Integer(value) => value.to_string().len().checked_add(3)?,
            Frame::SimpleString(value) | Frame::Error(value) => value.len().checked_add(3)?,
            Frame::Null => 5,
            Frame::Array(values) => {
                pending.extend(values.iter());
                values.len().to_string().len().checked_add(3)?
            }
            Frame::BulkString(value) => value
                .len()
                .checked_add(value.len().to_string().len())?
                .checked_add(5)?,
            Frame::Boolean(_) => 4,
            Frame::Double(value) => value.to_string().len().checked_add(3)?,
            Frame::BigNumber(value) => value.len().checked_add(3)?,
            Frame::BlobError(value) => value
                .len()
                .checked_add(value.len().to_string().len())?
                .checked_add(5)?,
            Frame::VerbatimString { data, .. } => data
                .len()
                .checked_add(data.len().to_string().len())?
                .checked_add(9)?,
            Frame::Set(values) | Frame::Push(values) => {
                pending.extend(values.iter());
                values.len().to_string().len().checked_add(3)?
            }
            Frame::Map(entries) => {
                for (key, value) in entries {
                    pending.push(key);
                    pending.push(value);
                }
                entries.len().to_string().len().checked_add(3)?
            }
            Frame::Attribute { attributes, data } => {
                for (key, value) in attributes {
                    pending.push(key);
                    pending.push(value);
                }
                pending.push(data);
                attributes.len().to_string().len().checked_add(3)?
            }
        };
        bytes = bytes.checked_add(cost)?;
        if bytes > crate::frame::MAX_FRAME_BYTES {
            return None;
        }
    }
    Some((nodes, bytes))
}

#[cfg(test)]
mod transaction_response_budget_tests {
    use super::*;

    #[test]
    fn exec_response_budget_discards_partial_results_on_overflow() {
        let mut budget = TransactionResponseBudget::new();
        let mut results = vec![Frame::Ok];
        budget.bytes = crate::frame::MAX_FRAME_BYTES;
        budget.push(&mut results, Frame::Integer(1));
        assert!(results.is_empty());
        assert!(matches!(
            budget.finish(results),
            Frame::Array(values)
                if matches!(values.as_slice(), [Frame::Error(message)] if message.contains("exceeds configured limit"))
        ));
    }
}
