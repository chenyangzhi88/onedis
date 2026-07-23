use onedis_server::args::{Args, ResolvedArgs};
use onedis_server::server::Server;
use std::process::id;
use std::sync::Arc;

#[tokio::main(worker_threads = 4)]
async fn main() {
    let args = Arc::new(Args::load());
    common::logging::init_logging().unwrap();

    server_info(args.clone());
    let mut server = Server::new(args.clone()).await;
    server.start().await;
}

fn server_info(args: Arc<ResolvedArgs>) {
    let pid = id();
    let version = env!("CARGO_PKG_VERSION");
    let pattern = format!(
        r#"
      ___  _   _ _____ ____ ___ ____
     / _ \| \ | | ____|  _ \_ _/ ___|
    | | | |  \| |  _| | | | | |\___ \
    | |_| | |\  | |___| |_| | | ___) |
     \___/|_| \_|_____|____/___|____/

    Onedis {}
    Bind: {}:{} PID: {}
    Role: master
    "#,
        version, args.bind, args.port, pid
    );
    log::info!("\n{}", pattern);
}
