use anyhow::Error;

use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::args::ResolvedArgs;
use crate::command::Command;
use crate::command_executor::CommandExecutor;
use crate::frame::Frame;
use crate::network::connection::Connection;
use crate::network::session::{Session, WatchedKey};
use crate::network::session_manager::SessionManager;
use crate::store::db::decode_string_bytes_slice;
use crate::store::db_manager::DatabaseManager;
use crate::wasm::WasmRegistry;
use kv_engine::monitor::{CoordinatorMonitorConfig, MonitorMetric, spawn_coordinator_monitor};

pub struct Server {
    args: Arc<ResolvedArgs>,
    session_manager: Arc<SessionManager>,
    db_manager: Arc<DatabaseManager>,
    command_executor: Arc<CommandExecutor>,
    wasm_registry: Arc<WasmRegistry>,
}

impl Server {
    pub async fn new(args: Arc<ResolvedArgs>) -> Self {
        let session_manager = Arc::new(SessionManager::new());
        let db_manager = Arc::new(DatabaseManager::new_async(args.clone()).await);
        let command_executor =
            Arc::new(CommandExecutor::from_env().expect("failed to start command executor"));
        let wasm_registry = Arc::new(WasmRegistry::new());
        if let Some(mut monitor_config) =
            CoordinatorMonitorConfig::from_options(db_manager.options())
        {
            if monitor_config.advertise_addr.is_empty() {
                monitor_config.advertise_addr = format!("{}:{}", args.bind, args.port);
            }
            let session_manager_for_metrics = session_manager.clone();
            let args_for_metrics = args.clone();
            let _monitor_task = spawn_coordinator_monitor(
                db_manager.store().db(),
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
                            value: args_for_metrics.maxclients as f64,
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
        }
    }

    pub async fn start(&mut self) {
        match TcpListener::bind(format!("{}:{}", self.args.bind, self.args.port)).await {
            Ok(listener) => {
                log::info!("Server initialized");
                log::info!("Ready to accept connections");
                loop {
                    match listener.accept().await {
                        Ok((stream, _address)) => {
                            // 检查 maxclients 限制
                            if self
                                .session_manager
                                .is_over_max_clients(self.args.maxclients)
                            {
                                let mut connection =
                                    crate::network::connection::Connection::new(stream);
                                let error_frame = crate::frame::Frame::Error(
                                    "ERR max number of clients reached".to_string(),
                                );
                                tokio::spawn(async move {
                                    connection.write_bytes(error_frame.as_bytes()).await;
                                });
                                continue;
                            }

                            let session_manager_clone = self.session_manager.clone();
                            let db_manager_clone = self.db_manager.clone();
                            let command_executor_clone = self.command_executor.clone();
                            let wasm_registry_clone = self.wasm_registry.clone();
                            let mut handler = Handler::new(
                                db_manager_clone,
                                session_manager_clone,
                                command_executor_clone,
                                wasm_registry_clone,
                                stream,
                                self.args.clone(),
                            );
                            tokio::spawn(async move {
                                handler.handle().await;
                            });
                        }
                        Err(e) => {
                            log::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
            }
            Err(_e) => {
                log::error!(
                    "Failed to bind to address {}:{}",
                    self.args.bind,
                    self.args.port
                );
                std::process::exit(1);
            }
        }
    }
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
        self.session_manager.update_session(self.session.clone());
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
