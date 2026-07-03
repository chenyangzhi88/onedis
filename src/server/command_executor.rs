use anyhow::Error;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct CommandExecutor {
    runtime: Arc<Runtime>,
    permits: Arc<Semaphore>,
}

impl CommandExecutor {
    pub fn new(worker_threads: usize, max_in_flight: usize) -> Result<Self, Error> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads.max(1))
            .thread_name("onedis-command")
            .enable_all()
            .build()?;
        Ok(Self {
            runtime: Arc::new(runtime),
            permits: Arc::new(Semaphore::new(max_in_flight.max(1))),
        })
    }

    pub fn from_env() -> Result<Self, Error> {
        let default_workers = std::thread::available_parallelism()
            .map(|parallelism| (parallelism.get() / 2).max(1))
            .unwrap_or(2);
        let worker_threads = std::env::var("ONEDIS_COMMAND_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(default_workers);
        let max_in_flight = std::env::var("ONEDIS_COMMAND_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(worker_threads.saturating_mul(64).max(64));
        Self::new(worker_threads, max_in_flight)
    }

    pub async fn execute<F, T>(&self, future: F) -> Result<T, Error>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.permits.clone().acquire_owned().await?;
        let join = self.runtime.spawn(async move {
            let _permit = permit;
            future.await
        });
        join.await.map_err(Error::from)
    }
}
