use anyhow::{Context, Error};

use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::args::ResolvedArgs;
use crate::command::Command;
use crate::frame::{Frame, RespVersion};
use crate::network::connection::Connection;
use crate::network::session::{Session, WatchedKey};
use crate::network::session_manager::{
    ClientUnblockMode, SessionControl, SessionManager, SubscriptionKind,
};
use crate::observability::metrics::{OnedisMetrics, global_metrics};
use crate::observability::prometheus::spawn_prometheus_endpoint;
use crate::store::db::{
    KeyExpirationBatchMutation, SetBatchMutation, StreamId, StringBatchMutation, StringBatchReply,
    decode_string_bytes_slice,
};
use crate::store::db_manager::DatabaseManager;
use crate::wasm::WasmRegistry;
use kv_engine::monitor::{CoordinatorMonitorConfig, MonitorMetric, spawn_coordinator_monitor};

pub mod command_executor;
mod service_state;
pub use service_state::ServiceState;

use self::command_executor::CommandExecutor;

const DEFAULT_HARD_MAX_CLIENTS: usize = 10_000;

pub struct Server {
    args: Arc<ResolvedArgs>,
    session_manager: Arc<SessionManager>,
    db_manager: Arc<DatabaseManager>,
    command_executor: Arc<CommandExecutor>,
    wasm_registry: Arc<WasmRegistry>,
    metrics: Arc<OnedisMetrics>,
    maxclients_limit: usize,
    service_state: Arc<ServiceState>,
    observability_task: Option<tokio::task::JoinHandle<()>>,
}

impl Server {
    pub async fn new(args: Arc<ResolvedArgs>) -> Result<Self, Error> {
        crate::resource_limits::validate_resource_limit_environment()?;
        let session_manager = Arc::new(SessionManager::with_default_password(
            args.requirepass.as_deref(),
        ));
        let db_manager = Arc::new(DatabaseManager::try_new_async(args.clone()).await?);
        let service_state = Arc::new(ServiceState::new_with_background(
            db_manager.store().storage_health(),
            Arc::clone(db_manager.background_health()),
        ));
        let command_executor = Arc::new(CommandExecutor::from_env()?);
        let wasm_registry = Arc::new(WasmRegistry::new());
        let hard_maxclients = std::env::var("ONEDIS_HARD_MAX_CLIENTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_HARD_MAX_CLIENTS);
        let maxclients_limit = if args.maxclients == 0 {
            hard_maxclients
        } else {
            args.maxclients.min(hard_maxclients)
        };
        let metrics = global_metrics();
        metrics.configure(args.databases, maxclients_limit);
        metrics.set_enabled(args.observability_enabled);
        metrics.initialize_command_index();
        if let Some(mut monitor_config) =
            CoordinatorMonitorConfig::from_options(db_manager.options())
        {
            if monitor_config.advertise_addr.is_empty() {
                monitor_config.advertise_addr = format!("{}:{}", args.bind, args.port);
            }
            let session_manager_for_metrics = session_manager.clone();
            let args_for_metrics = args.clone();
            let maxclients_for_metrics = maxclients_limit;
            let _monitor_task = spawn_coordinator_monitor(
                db_manager.store().engine_handle_for_monitoring(),
                monitor_config,
                Arc::new(move || {
                    vec![
                        MonitorMetric {
                            name: "onedis.connections.current".to_string(),
                            value: session_manager_for_metrics.get_connection_count() as f64,
                            unit: "count".to_string(),
                        },
                        MonitorMetric {
                            name: "onedis.connections.max".to_string(),
                            value: maxclients_for_metrics as f64,
                            unit: "count".to_string(),
                        },
                        MonitorMetric {
                            name: "onedis.databases".to_string(),
                            value: args_for_metrics.databases as f64,
                            unit: "count".to_string(),
                        },
                    ]
                }),
            );
        }

        Ok(Server {
            args,
            session_manager,
            db_manager,
            command_executor,
            wasm_registry,
            metrics,
            maxclients_limit,
            service_state,
            observability_task: None,
        })
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let address = format!("{}:{}", self.args.bind, self.args.port);
        let listener = TcpListener::bind(&address)
            .await
            .with_context(|| format!("failed to bind OneDis RESP endpoint {address}"))?;
        if self.args.observability_enabled && self.args.metrics_port != 0 {
            self.observability_task = Some(
                spawn_prometheus_endpoint(
                    self.metrics.clone(),
                    self.db_manager.clone(),
                    self.service_state.clone(),
                    self.args.metrics_bind.clone(),
                    self.args.metrics_port,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to bind OneDis observability endpoint {}:{}",
                        self.args.metrics_bind, self.args.metrics_port
                    )
                })?,
            );
        }
        self.service_state.mark_ready();
        log::info!("Server initialized");
        log::info!("Ready to accept connections");
        let mut handlers = tokio::task::JoinSet::new();
        let mut shutdown = Box::pin(shutdown_signal());
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    log::info!("Shutdown signal received; stopping server");
                    break;
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _address)) => {
                        self.metrics.connection_accepted();
                        if self
                            .session_manager
                            .is_over_max_clients(self.maxclients_limit)
                        {
                            self.metrics.connection_rejected("maxclients");
                            let mut connection =
                                crate::network::connection::Connection::new(stream);
                            let error_frame = crate::frame::Frame::Error(
                                "ERR max number of clients reached".to_string(),
                            );
                            self.metrics.add_output_bytes(error_frame.as_bytes().len());
                            tokio::spawn(async move {
                                let _ = connection.write_bytes(error_frame.as_bytes()).await;
                            });
                            continue;
                        }

                        let mut handler = Handler::new_with_state(
                            self.db_manager.clone(),
                            self.session_manager.clone(),
                            self.command_executor.clone(),
                            self.wasm_registry.clone(),
                            stream,
                            self.args.clone(),
                            self.service_state.clone(),
                        );
                        handlers.spawn(async move {
                            handler.handle().await;
                        });
                    }
                    Err(err) => {
                        log::error!("Failed to accept connection: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
                joined = handlers.join_next(), if !handlers.is_empty() => {
                    if let Some(Err(err)) = joined {
                        log::error!("Connection handler terminated unexpectedly: {err}");
                    }
                }
            }
        }

        self.service_state.begin_shutdown();
        let requested = self.session_manager.request_shutdown_all();
        log::info!("Draining {requested} active client connection(s)");
        let deadline = Instant::now() + Duration::from_millis(self.args.shutdown_timeout_ms);
        loop {
            if handlers.is_empty() {
                break;
            }
            match tokio::time::timeout_at(deadline, handlers.join_next()).await {
                Ok(Some(Err(err))) => {
                    log::error!("Connection handler terminated unexpectedly: {err}");
                }
                Ok(Some(Ok(()))) => {}
                Ok(None) => break,
                Err(_) => {
                    let remaining = handlers.len();
                    log::warn!(
                        "Graceful shutdown deadline reached; cancelling {remaining} connection handler(s)"
                    );
                    handlers.abort_all();
                    while handlers.join_next().await.is_some() {}
                    break;
                }
            }
        }
        let maintenance_budget = deadline.saturating_duration_since(Instant::now());
        self.db_manager
            .shutdown(maintenance_budget.max(Duration::from_millis(1)))
            .await?;
        self.command_executor.shutdown_background();
        if let Some(task) = self.observability_task.take() {
            task.abort();
            let _ = task.await;
        }
        log::info!("Server shutdown complete");
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            log::error!("Failed to listen for Ctrl-C: {err}");
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(err) => {
                log::error!("Failed to listen for SIGTERM: {err}");
                ctrl_c.await;
            }
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

pub struct Handler {
    session: Session,
    client_control: Arc<SessionControl>,
    connection: Connection,
    session_manager: Arc<SessionManager>,
    db_manager: Arc<DatabaseManager>,
    command_executor: Arc<CommandExecutor>,
    wasm_registry: Arc<WasmRegistry>,
    args: Arc<ResolvedArgs>,
    transaction_db: Option<crate::store::db::Db>,
    metrics: Arc<OnedisMetrics>,
    service_state: Arc<ServiceState>,
}

impl Handler {
    fn encode_frame(&self, frame: &Frame) -> Vec<u8> {
        frame.as_bytes_for_protocol(self.session.resp_version())
    }

    pub fn get_session(&self) -> &Session {
        &self.session
    }

    pub fn get_db_manager(&self) -> &Arc<DatabaseManager> {
        &self.db_manager
    }

    pub fn get_args(&self) -> &Arc<ResolvedArgs> {
        &self.args
    }

    pub fn get_session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }

    pub fn set_client_name(&mut self, name: Option<String>) {
        self.session.set_name(name);
        self.session_manager.update_session(&self.session);
    }

    pub fn client_name(&self) -> Option<String> {
        self.session.name().map(ToString::to_string)
    }

    pub fn set_client_library_name(&mut self, name: Option<String>) {
        self.session.set_library_name(name);
        self.session_manager.update_session(&self.session);
    }

    pub fn set_client_library_version(&mut self, version: Option<String>) {
        self.session.set_library_version(version);
        self.session_manager.update_session(&self.session);
    }

    pub fn set_client_no_evict(&mut self, enabled: bool) {
        self.session.set_no_evict(enabled);
        self.session_manager.update_session(&self.session);
    }

    pub fn set_client_no_touch(&mut self, enabled: bool) {
        self.session.set_no_touch(enabled);
        self.session_manager.update_session(&self.session);
    }

    fn client_unblock_response(mode: ClientUnblockMode) -> Vec<u8> {
        match mode {
            ClientUnblockMode::Timeout => Frame::Null,
            ClientUnblockMode::Error => {
                Frame::Error("UNBLOCKED client unblocked via CLIENT UNBLOCK".to_string())
            }
        }
        .as_bytes()
    }
}

include!("server/handler_commands.rs");

include!("server/borrowed_resp.rs");

include!("server/borrowed_fast_paths.rs");

include!("server/resp_helpers.rs");

#[cfg(test)]
#[path = "server/tests.rs"]
mod tests;
