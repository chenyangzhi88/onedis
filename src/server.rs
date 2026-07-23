use anyhow::Error;

use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::args::ResolvedArgs;
use crate::command::Command;
use crate::frame::Frame;
use crate::network::connection::Connection;
use crate::network::session::{Session, WatchedKey};
use crate::network::session_manager::{SessionManager, SubscriptionKind};
use crate::observability::metrics::{OnedisMetrics, global_metrics};
use crate::observability::prometheus::spawn_prometheus_endpoint;
use crate::store::db::decode_string_bytes_slice;
use crate::store::db_manager::DatabaseManager;
use crate::wasm::WasmRegistry;
use kv_engine::monitor::{CoordinatorMonitorConfig, MonitorMetric, spawn_coordinator_monitor};

pub mod command_executor;

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
}

impl Server {
    pub async fn new(args: Arc<ResolvedArgs>) -> Self {
        let session_manager = Arc::new(SessionManager::with_default_password(
            args.requirepass.as_deref(),
        ));
        let db_manager = Arc::new(DatabaseManager::new_async(args.clone()).await);
        let command_executor =
            Arc::new(CommandExecutor::from_env().expect("failed to start command executor"));
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
        if args.observability_enabled && args.metrics_port != 0 {
            spawn_prometheus_endpoint(
                metrics.clone(),
                db_manager.clone(),
                args.metrics_bind.clone(),
                args.metrics_port,
            );
        }
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

        Server {
            args,
            session_manager,
            db_manager,
            command_executor,
            wasm_registry,
            metrics,
            maxclients_limit,
        }
    }

    pub async fn start(&mut self) {
        match TcpListener::bind(format!("{}:{}", self.args.bind, self.args.port)).await {
            Ok(listener) => {
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

                                let mut handler = Handler::new(
                                    self.db_manager.clone(),
                                    self.session_manager.clone(),
                                    self.command_executor.clone(),
                                    self.wasm_registry.clone(),
                                    stream,
                                    self.args.clone(),
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
                handlers.abort_all();
                while handlers.join_next().await.is_some() {}
                self.db_manager.shutdown().await;
                log::info!("Server shutdown complete");
            }
            Err(err) => {
                log::error!(
                    "Failed to bind to address {}:{}: {}",
                    self.args.bind,
                    self.args.port,
                    err
                );
            }
        }
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
    connection: Connection,
    session_manager: Arc<SessionManager>,
    db_manager: Arc<DatabaseManager>,
    command_executor: Arc<CommandExecutor>,
    wasm_registry: Arc<WasmRegistry>,
    args: Arc<ResolvedArgs>,
    transaction_db: Option<crate::store::db::Db>,
    metrics: Arc<OnedisMetrics>,
}

impl Handler {
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
}

include!("server/handler_commands.rs");

include!("server/borrowed_resp.rs");

include!("server/borrowed_fast_paths.rs");

include!("server/resp_helpers.rs");

#[cfg(test)]
mod tests {
    include!("server/tests.rs");
}
