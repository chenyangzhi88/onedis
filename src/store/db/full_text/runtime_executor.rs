use super::*;

/// Dedicated, bounded executor for CPU and storage-heavy full-text queries.
///
/// The generic Tokio blocking pool is shared by unrelated commands and can
/// grow independently of search memory budgets. This executor gives search a
/// fixed concurrency ceiling and rejects overload before an unbounded work
/// queue is formed.
pub(super) struct FullTextSearchExecutor {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
    permits: Arc<tokio::sync::Semaphore>,
}

impl FullTextSearchExecutor {
    fn new(worker_threads: usize, max_in_flight: usize) -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads.max(1))
            .thread_name("onedis-fulltext")
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        Ok(Self {
            runtime: Mutex::new(Some(runtime)),
            handle,
            permits: Arc::new(tokio::sync::Semaphore::new(max_in_flight.max(1))),
        })
    }

    fn from_env() -> Result<Self, Error> {
        let default_workers = std::thread::available_parallelism()
            .map(|parallelism| (parallelism.get() / 2).max(1))
            .unwrap_or(2);
        let workers = std::env::var("ONEDIS_FULLTEXT_SEARCH_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default_workers);
        let max_in_flight = std::env::var("ONEDIS_FULLTEXT_SEARCH_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| workers.saturating_mul(4).max(workers));
        Self::new(workers, max_in_flight)
    }

    async fn execute<T, F>(&self, operation: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Error> + Send + 'static,
    {
        let queued_at = Instant::now();
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::msg("BUSY fulltext search executor is saturated"))?;
        self.handle
            .spawn_blocking(move || {
                global_metrics().record_fulltext_search_stage(
                    FullTextSearchStage::ExecutorQueue,
                    elapsed_us(queued_at),
                );
                let _permit = permit;
                operation()
            })
            .await
            .map_err(|error| Error::msg(format!("fulltext search worker failed: {error}")))?
    }

    fn shutdown_background(&self) {
        let runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(runtime) = runtime {
            runtime.shutdown_background();
        }
    }
}

impl Drop for FullTextSearchExecutor {
    fn drop(&mut self) {
        self.shutdown_background();
    }
}

fn fulltext_search_executor() -> Result<&'static FullTextSearchExecutor, Error> {
    static EXECUTOR: OnceLock<Result<FullTextSearchExecutor, String>> = OnceLock::new();
    EXECUTOR
        .get_or_init(|| FullTextSearchExecutor::from_env().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| {
            Error::msg(format!(
                "failed to initialize fulltext search executor: {error}"
            ))
        })
}

impl Db {
    pub(super) async fn run_fulltext_search_task<T, F>(&self, operation: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(Db) -> Result<T, Error> + Send + 'static,
    {
        let db = self.shared_task_view();
        fulltext_search_executor()?
            .execute(move || operation(db))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_rejects_work_beyond_its_bounded_capacity() {
        let executor = Arc::new(FullTextSearchExecutor::new(1, 1).unwrap());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_executor = executor.clone();
        let first = tokio::spawn(async move {
            first_executor
                .execute(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();

        let error = executor.execute(|| Ok(())).await.unwrap_err();
        assert!(error.to_string().contains("executor is saturated"));
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
    }
}
