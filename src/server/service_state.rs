use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

/// Process-local lifecycle state shared by the RESP listener, health endpoint and handlers.
///
/// `healthy` means the process can still report diagnostics. `ready` is deliberately stricter:
/// it is withdrawn before shutdown starts and whenever a fatal storage/runtime fault is reported.
pub struct ServiceState {
    healthy: AtomicBool,
    ready: AtomicBool,
    shutting_down: AtomicBool,
    degraded_reason: RwLock<Option<String>>,
    storage_health: Arc<crate::store::health::StorageHealth>,
    background_health: Option<Arc<crate::store::db_manager::BackgroundTaskHealth>>,
}

impl Default for ServiceState {
    fn default() -> Self {
        Self::new(Arc::new(crate::store::health::StorageHealth::default()))
    }
}

impl ServiceState {
    pub fn new(storage_health: Arc<crate::store::health::StorageHealth>) -> Self {
        Self {
            healthy: AtomicBool::new(true),
            ready: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            degraded_reason: RwLock::new(None),
            storage_health,
            background_health: None,
        }
    }

    pub fn new_with_background(
        storage_health: Arc<crate::store::health::StorageHealth>,
        background_health: Arc<crate::store::db_manager::BackgroundTaskHealth>,
    ) -> Self {
        Self {
            background_health: Some(background_health),
            ..Self::new(storage_health)
        }
    }

    pub fn storage_health(&self) -> &crate::store::health::StorageHealth {
        &self.storage_health
    }
    pub fn mark_ready(&self) {
        if !self.shutting_down.load(Ordering::Acquire) && self.degraded_reason().is_none() {
            self.ready.store(true, Ordering::Release);
        }
    }

    pub fn begin_shutdown(&self) {
        self.ready.store(false, Ordering::Release);
        self.shutting_down.store(true, Ordering::Release);
    }

    pub fn mark_degraded(&self, reason: impl Into<String>) {
        self.ready.store(false, Ordering::Release);
        *self
            .degraded_reason
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reason.into());
    }

    pub fn mark_unhealthy(&self, reason: impl Into<String>) {
        self.mark_degraded(reason);
        self.healthy.store(false, Ordering::Release);
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.shutting_down.load(Ordering::Acquire)
            && self.degraded_reason().is_none()
            && self.storage_health.is_healthy()
            && self
                .background_health
                .as_ref()
                .is_none_or(|health| health.is_healthy())
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    pub fn accepts_writes(&self) -> bool {
        self.is_ready()
    }

    pub fn degraded_reason(&self) -> Option<String> {
        let reason = self
            .degraded_reason
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        reason
            .or_else(|| {
                self.storage_health
                    .last_error()
                    .map(|error| format!("storage degraded: {error}"))
            })
            .or_else(|| {
                self.background_health
                    .as_ref()
                    .and_then(|health| health.degraded_reason())
            })
    }

    pub fn health_body(&self) -> String {
        if self.is_healthy() {
            "ok\n".to_string()
        } else {
            format!(
                "unhealthy: {}\n",
                self.degraded_reason()
                    .unwrap_or_else(|| "unknown failure".to_string())
            )
        }
    }

    pub fn readiness_body(&self) -> String {
        if self.is_ready() {
            return "ready\n".to_string();
        }
        if self.is_shutting_down() {
            return "not ready: shutting down\n".to_string();
        }
        format!(
            "not ready: {}\n",
            self.degraded_reason()
                .unwrap_or_else(|| "startup in progress".to_string())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceState;

    #[test]
    fn readiness_is_withdrawn_on_degrade_and_shutdown() {
        let state = ServiceState::default();
        assert!(!state.is_ready());
        state.mark_ready();
        assert!(state.is_ready());
        state.mark_degraded("storage unavailable");
        assert!(!state.is_ready());
        assert!(state.readiness_body().contains("storage unavailable"));

        let state = ServiceState::default();
        state.mark_ready();
        state.begin_shutdown();
        assert!(!state.accepts_writes());
        assert!(state.readiness_body().contains("shutting down"));
    }
}
