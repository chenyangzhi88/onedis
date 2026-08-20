use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use super::metrics::OnedisMetrics;
use crate::server::ServiceState;
use crate::store::db_manager::DatabaseManager;

pub async fn spawn_prometheus_endpoint(
    metrics: Arc<OnedisMetrics>,
    db_manager: Arc<DatabaseManager>,
    service_state: Arc<ServiceState>,
    bind: String,
    port: u16,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let address = format!("{bind}:{port}");
    let listener = TcpListener::bind(&address).await?;
    Ok(tokio::spawn(async move {
        log::info!("Prometheus metrics endpoint listening on http://{address}/metrics");

        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                continue;
            };
            let metrics = metrics.clone();
            let db_manager = db_manager.clone();
            let service_state = service_state.clone();
            tokio::spawn(async move {
                let mut request = [0_u8; 1024];
                let read = stream.read(&mut request).await.unwrap_or(0);
                let path_is_metrics = request[..read].starts_with(b"GET /metrics ")
                    || request[..read].starts_with(b"GET /metrics?");
                let path_is_health = request[..read].starts_with(b"GET /healthz ")
                    || request[..read].starts_with(b"GET /healthz?");
                let path_is_ready = request[..read].starts_with(b"GET /readyz ")
                    || request[..read].starts_with(b"GET /readyz?");
                let (status, content_type, body) = if path_is_metrics {
                    match db_manager.render_observability_prometheus() {
                        Ok(db_body) => {
                            let mut body = metrics.render_prometheus();
                            body.push_str(&db_body);
                            body.push_str(
                                "# HELP onedis_ready Whether this process is ready for traffic.\n",
                            );
                            body.push_str("# TYPE onedis_ready gauge\n");
                            body.push_str(&format!(
                                "onedis_ready {}\n",
                                u8::from(service_state.is_ready())
                            ));
                            body.push_str("# HELP onedis_storage_failures_total Fatal kv-engine operation failures observed by this process.\n");
                            body.push_str("# TYPE onedis_storage_failures_total counter\n");
                            body.push_str(&format!(
                                "onedis_storage_failures_total {}\n",
                                service_state.storage_health().failure_count()
                            ));
                            ("200 OK", "text/plain; version=0.0.4; charset=utf-8", body)
                        }
                        Err(error) => (
                            "503 Service Unavailable",
                            "text/plain; charset=utf-8",
                            format!("metrics unavailable: {error}\n"),
                        ),
                    }
                } else if path_is_health {
                    (
                        if service_state.is_healthy() {
                            "200 OK"
                        } else {
                            "503 Service Unavailable"
                        },
                        "text/plain; charset=utf-8",
                        service_state.health_body(),
                    )
                } else if path_is_ready {
                    (
                        if service_state.is_ready() {
                            "200 OK"
                        } else {
                            "503 Service Unavailable"
                        },
                        "text/plain; charset=utf-8",
                        service_state.readiness_body(),
                    )
                } else {
                    (
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        "not found\n".to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{args::ResolvedArgs, observability::metrics::global_metrics};
    use std::{io, net::TcpListener as StdTcpListener, sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[tokio::test]
    async fn metrics_endpoint_serves_prometheus_text() {
        let temp = tempfile::tempdir().expect("failed to create temp dir");
        let db_path = temp.path().join("db");
        let wal_dir = temp.path().join("wal");
        let config_path = temp.path().join("onedis.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[db]
path = "{}"

[wal]
dir = "{}"

[onedis_server]
port = 0
bind = "127.0.0.1"
databases = 1
hz = 10.0
loglevel = "info"
maxclients = 1000
"#,
                db_path.display(),
                wal_dir.display()
            ),
        )
        .expect("failed to write config");
        let port = reserve_port().expect("failed to reserve metrics port");
        let args = Arc::new(ResolvedArgs {
            config: config_path.to_string_lossy().into_owned(),
            requirepass: None,
            bind: "127.0.0.1".to_string(),
            databases: 1,
            hz: 10.0,
            port: 0,
            loglevel: "info".to_string(),
            maxclients: 1000,
            observability_enabled: true,
            metrics_bind: "127.0.0.1".to_string(),
            metrics_port: port,
            slow_command_threshold_ms: 10,
            shutdown_timeout_ms: 30_000,
        });
        let db_manager = Arc::new(crate::store::db_manager::DatabaseManager::new_async(args).await);
        let metrics = global_metrics();
        metrics.configure(1, 1000);
        metrics.record_command("GET", 100, None, 10_000);
        let service_state = Arc::new(ServiceState::default());
        service_state.mark_ready();
        let endpoint = spawn_prometheus_endpoint(
            metrics,
            db_manager,
            service_state,
            "127.0.0.1".to_string(),
            port,
        )
        .await
        .unwrap();

        let body = scrape_metrics(port).await;
        assert!(body.contains("onedis_up 1"));
        assert!(body.contains("onedis_commands_total{command=\"GET\"}"));
        assert!(body.contains("onedis_storage_reads_total"));
        assert!(body.contains("onedis_storage_engine_property"));
        assert!(body.contains("onedis_fulltext_outbox_pending"));
        assert!(body.contains("onedis_background_task_running"));
        assert!(body.contains("onedis_stream_blocked_clients"));
        assert!(body.contains("onedis_vector_indexes_total"));
        endpoint.abort();
    }

    fn reserve_port() -> io::Result<u16> {
        let listener = StdTcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }

    async fn scrape_metrics(port: u16) -> String {
        let address = format!("127.0.0.1:{port}");
        let mut last_err = None;
        for _ in 0..50 {
            match TcpStream::connect(&address).await {
                Ok(mut stream) => {
                    stream
                        .write_all(b"GET /metrics HTTP/1.1\r\nhost: localhost\r\n\r\n")
                        .await
                        .expect("failed to write metrics request");
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .await
                        .expect("failed to read metrics response");
                    let response = String::from_utf8(response).expect("metrics response utf8");
                    let (_, body) = response
                        .split_once("\r\n\r\n")
                        .expect("HTTP response has body separator");
                    return body.to_string();
                }
                Err(err) => {
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!("metrics endpoint did not start: {last_err:?}");
    }
}
