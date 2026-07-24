use super::response_frames::{wasm_error_frame, wasm_values_frame};
use super::*;

impl WasmCommand {
    pub async fn apply(self, registry: &Arc<WasmRegistry>, db: Arc<Db>) -> Frame {
        match self {
            Self::Load { name, bytes } => match registry.load(&name, &bytes) {
                Ok(()) => Frame::Ok,
                Err(error) => wasm_error_frame(error),
            },
            Self::Call {
                name,
                function,
                args,
                read_only,
                ..
            } => {
                let started = Instant::now();
                let result = call_wasm(registry, db, &name, &function, &args, read_only).await;
                global_metrics().record_wasm_call(elapsed_us(started), result.is_err());
                match result {
                    Ok(values) => wasm_values_frame(values),
                    Err(error) => wasm_error_frame(error),
                }
            }
            Self::Scan {
                name,
                function,
                prefix,
                limit,
            } => {
                let started = Instant::now();
                let result = registry.scan(db, &name, &function, &prefix, limit).await;
                global_metrics().record_wasm_call(elapsed_us(started), result.is_err());
                match result {
                    Ok(keys) => Frame::Array(keys.into_iter().map(Frame::bulk_string).collect()),
                    Err(error) => wasm_error_frame(error),
                }
            }
            Self::Delete { name } => Frame::Integer(i64::from(registry.delete(&name))),
            Self::FunctionLoad { name, bytes } => match registry.load(&name, &bytes) {
                Ok(()) => Frame::Ok,
                Err(error) => wasm_error_frame(error),
            },
            Self::FunctionDelete { name } => Frame::Integer(i64::from(registry.delete(&name))),
            Self::FunctionList => Frame::Array(
                registry
                    .list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Self::List => Frame::Array(
                registry
                    .list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
        }
    }
}

async fn call_wasm(
    registry: &Arc<WasmRegistry>,
    db: Arc<Db>,
    name: &str,
    function: &str,
    args: &[String],
    read_only: bool,
) -> Result<Vec<WasmValue>> {
    if read_only {
        return registry.call(db, name, function, args, true).await;
    }
    let txn_db = Arc::new(db.transactional_view()?);
    let values = registry
        .call(txn_db.clone(), name, function, args, false)
        .await?;
    txn_db.commit_transaction_async().await?;
    Ok(values)
}
