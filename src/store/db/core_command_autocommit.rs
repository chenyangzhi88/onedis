impl Db {
    pub fn handle_command_autocommit(&self, command: Command) -> Result<Frame, Error> {
        let txn_db = self.transactional_view()?;
        let frame = txn_db.handle_command(command)?;
        txn_db.commit_transaction()?;
        Ok(frame)
    }

    pub async fn handle_command_autocommit_async(&self, command: Command) -> Result<Frame, Error> {
        let txn_db = self.transactional_view()?;
        let frame = txn_db.handle_command_async(command).await?;
        txn_db.commit_transaction_async().await?;
        Ok(frame)
    }
}
