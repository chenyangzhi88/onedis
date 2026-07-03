use super::*;

pub fn handle_command_autocommit(db: &Db, command: Command) -> Result<Frame, Error> {
    let txn_db = db.transactional_view()?;
    let frame = handle_command(&txn_db, command)?;
    txn_db.commit_transaction()?;
    Ok(frame)
}

pub async fn handle_command_autocommit_async(db: &Db, command: Command) -> Result<Frame, Error> {
    let txn_db = db.transactional_view()?;
    let frame = handle_command_async(&txn_db, command).await?;
    txn_db.commit_transaction_async().await?;
    Ok(frame)
}
