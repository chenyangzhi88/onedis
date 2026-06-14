use std::fs::{self, File};
use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use redis::{Client, Connection};
use tempfile::TempDir;

pub struct TestServer {
    _data_dir: TempDir,
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    url: String,
}

impl TestServer {
    pub fn spawn() -> Self {
        let port = reserve_port();
        let data_dir = tempfile::tempdir().expect("failed to create test data dir");
        let bin = resolve_server_binary();
        let stdout_path = data_dir.path().join("onedis.stdout.log");
        let stderr_path = data_dir.path().join("onedis.stderr.log");
        let config_path = data_dir.path().join("onedis.toml");
        let db_path = data_dir.path().join("db");
        let wal_dir = data_dir.path().join("wal");
        fs::write(
            &config_path,
            format!(
                "[db]\npath = \"{}\"\n\n[wal]\ndir = \"{}\"\n",
                db_path.display(),
                wal_dir.display()
            ),
        )
        .expect("failed to write onedis test config");
        let stdout = File::create(&stdout_path).expect("failed to create onedis stdout log");
        let stderr = File::create(&stderr_path).expect("failed to create onedis stderr log");

        let mut child = Command::new(&bin)
            .current_dir(workspace_root())
            .arg("--config")
            .arg(&config_path)
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--databases")
            .arg("16")
            .arg("--hz")
            .arg("20")
            .arg("--loglevel")
            .arg("error")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("failed to spawn onedis-server");

        let url = format!("redis://127.0.0.1:{port}/");
        wait_for_server(&url, &mut child, &stdout_path, &stderr_path);

        Self {
            _data_dir: data_dir,
            child,
            stdout_path,
            stderr_path,
            url,
        }
    }

    pub fn connection(&self) -> Connection {
        let client = Client::open(self.url.as_str()).expect("failed to create redis client");
        client
            .get_connection()
            .expect("failed to connect to spawned onedis-server")
    }

    #[allow(dead_code)]
    pub fn stderr_log(&self) -> String {
        read_log(&self.stderr_path)
    }

    #[allow(dead_code)]
    pub fn stdout_log(&self) -> String {
        read_log(&self.stdout_path)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to reserve local TCP port")
        .local_addr()
        .expect("failed to read reserved local address")
        .port()
}

pub fn setup_connection() -> (TestServer, Connection) {
    let server = TestServer::spawn();
    let con = server.connection();
    (server, con)
}

fn wait_for_server(url: &str, child: &mut Child, stdout_path: &Path, stderr_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let client = Client::open(url).expect("failed to build redis client while waiting");

    loop {
        match client.get_connection() {
            Ok(_) => return,
            Err(err) if Instant::now() < deadline => {
                if let Some(status) = child.try_wait().expect("failed to poll onedis-server") {
                    panic!(
                        "spawned onedis-server exited before ready: status={status}, stdout_log={}, stderr_log={}\nstdout:\n{}\nstderr:\n{}",
                        stdout_path.display(),
                        stderr_path.display(),
                        read_log(stdout_path),
                        read_log(stderr_path)
                    );
                }
                if is_connection_pending(&err) {
                    sleep(Duration::from_millis(50));
                    continue;
                }
                sleep(Duration::from_millis(50));
            }
            Err(err) => panic!(
                "spawned onedis-server did not become ready: {err}, stdout_log={}, stderr_log={}\nstdout:\n{}\nstderr:\n{}",
                stdout_path.display(),
                stderr_path.display(),
                read_log(stdout_path),
                read_log(stderr_path)
            ),
        }
    }
}

fn resolve_server_binary() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_onedis-server") {
        return path.into();
    }

    let current_exe = std::env::current_exe().expect("failed to locate current test binary");
    let debug_dir = current_exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("failed to locate target/debug directory");
    let candidate = debug_dir.join("onedis-server");
    if candidate.exists() {
        return candidate;
    }

    panic!("failed to locate onedis-server test binary");
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("onedis crate should live below workspace root")
        .to_path_buf()
}

fn is_connection_pending(err: &redis::RedisError) -> bool {
    matches!(err.kind(), redis::ErrorKind::IoError)
        || err
            .detail()
            .map(|detail| {
                detail.contains("Connection refused")
                    || detail.contains("timed out")
                    || detail.contains("No route to host")
            })
            .unwrap_or(false)
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| String::from("<unavailable>"))
}

#[allow(dead_code)]
pub fn flushdb(con: &mut Connection) -> redis::RedisResult<()> {
    redis::cmd("FLUSHDB").query(con)
}

#[allow(dead_code)]
pub fn io_error(message: &str) -> io::Error {
    io::Error::other(message)
}

#[allow(dead_code)]
pub fn hgetall(con: &mut Connection, key: &str) -> redis::RedisResult<Vec<String>> {
    redis::cmd("HGETALL").arg(key).query(con)
}

#[allow(dead_code)]
pub fn hmget(
    con: &mut Connection,
    key: &str,
    fields: &[&str],
) -> redis::RedisResult<Vec<Option<String>>> {
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(key);
    for field in fields {
        cmd.arg(field);
    }
    cmd.query(con)
}

#[allow(dead_code)]
pub fn hkeys(con: &mut Connection, key: &str) -> redis::RedisResult<Vec<String>> {
    redis::cmd("HKEYS").arg(key).query(con)
}

#[allow(dead_code)]
pub fn hvals(con: &mut Connection, key: &str) -> redis::RedisResult<Vec<String>> {
    redis::cmd("HVALS").arg(key).query(con)
}

#[allow(dead_code)]
pub fn hsetnx(
    con: &mut Connection,
    key: &str,
    field: &str,
    value: &str,
) -> redis::RedisResult<i32> {
    redis::cmd("HSETNX")
        .arg(key)
        .arg(field)
        .arg(value)
        .query(con)
}

#[allow(dead_code)]
pub fn hscan(
    con: &mut Connection,
    key: &str,
    cursor: i32,
    pattern: Option<&str>,
    count: Option<i32>,
) -> redis::RedisResult<(i32, Vec<String>)> {
    let mut cmd = redis::cmd("HSCAN");
    cmd.arg(key).arg(cursor);
    if let Some(pattern) = pattern {
        cmd.arg("MATCH").arg(pattern);
    }
    if let Some(count) = count {
        cmd.arg("COUNT").arg(count);
    }
    cmd.query(con)
}

#[allow(dead_code)]
pub fn scan(
    con: &mut Connection,
    cursor: i32,
    pattern: Option<&str>,
    count: Option<i32>,
) -> redis::RedisResult<(i32, Vec<String>)> {
    let mut cmd = redis::cmd("SCAN");
    cmd.arg(cursor);
    if let Some(pattern) = pattern {
        cmd.arg("MATCH").arg(pattern);
    }
    if let Some(count) = count {
        cmd.arg("COUNT").arg(count);
    }
    cmd.query(con)
}

#[allow(dead_code)]
pub fn sscan(
    con: &mut Connection,
    key: &str,
    cursor: i32,
    pattern: Option<&str>,
    count: Option<i32>,
) -> redis::RedisResult<(i32, Vec<String>)> {
    let mut cmd = redis::cmd("SSCAN");
    cmd.arg(key).arg(cursor);
    if let Some(pattern) = pattern {
        cmd.arg("MATCH").arg(pattern);
    }
    if let Some(count) = count {
        cmd.arg("COUNT").arg(count);
    }
    cmd.query(con)
}

#[allow(dead_code)]
pub fn move_key(con: &mut Connection, key: &str, db_index: i32) -> redis::RedisResult<i32> {
    redis::cmd("MOVE").arg(key).arg(db_index).query(con)
}

#[allow(dead_code)]
pub fn select_db(con: &mut Connection, db_index: i32) -> redis::RedisResult<()> {
    redis::cmd("SELECT").arg(db_index).query(con)
}
