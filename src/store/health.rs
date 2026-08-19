use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
pub struct StorageHealth {
    failures: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl StorageHealth {
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn record_failure(&self, operation: &str, error: impl std::fmt::Display) {
        let message = format!("{operation}: {error}");
        *self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(message.clone());
        self.failures.fetch_add(1, Ordering::AcqRel);
        log::error!("kv-engine storage failure: {message}");
    }
}

pub fn storage_health() -> &'static StorageHealth {
    static HEALTH: OnceLock<StorageHealth> = OnceLock::new();
    HEALTH.get_or_init(StorageHealth::default)
}
