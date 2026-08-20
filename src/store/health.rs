use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use common::types::status::Status;

const HEALTHY: u8 = 0;
const DEGRADED: u8 = 1;
const PROBING: u8 = 2;

#[derive(Default)]
pub struct StorageHealth {
    state: AtomicU8,
    failures: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl StorageHealth {
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Acquire)
    }

    pub fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == HEALTHY
    }

    pub fn begin_probe(&self) -> bool {
        self.state
            .compare_exchange(DEGRADED, PROBING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn record_probe_success(&self) {
        *self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.state.store(HEALTHY, Ordering::Release);
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn record_failure(&self, operation: &str, error: &Status) {
        if !is_infrastructure_failure(error) {
            return;
        }
        self.record_infrastructure_failure(operation, error);
    }

    pub fn record_internal_failure(&self, operation: &str, error: impl std::fmt::Display) {
        self.record_infrastructure_failure(operation, error);
    }

    fn record_infrastructure_failure(&self, operation: &str, error: impl std::fmt::Display) {
        let message = format!("{operation}: {error}");
        *self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(message.clone());
        self.state.store(DEGRADED, Ordering::Release);
        self.failures.fetch_add(1, Ordering::AcqRel);
        log::error!("kv-engine storage failure: {message}");
    }
}

fn is_infrastructure_failure(error: &Status) -> bool {
    matches!(
        error,
        Status::Io(_)
            | Status::IOError(_)
            | Status::WalError(_)
            | Status::TabletFull
            | Status::Corruption(_)
            | Status::Internal(_)
    )
}
