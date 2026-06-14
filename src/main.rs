use onedis_server::args::{Args, ResolvedArgs};
use onedis_server::server::Server;
use std::process::id;
use std::sync::Arc;

#[tokio::main(worker_threads = 2)]
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
         /\_____/\
        /  o   o  \          Rudis {}
       ( ==  ^  == )
        )         (          Bind: {} PID: {}
       (           )          Role: master
      ( (  )   (  ) )
     (__(__)___(__)__)

    Rudis is a high-performance in memory database.
    "#,
        version, args.port, pid
    );
    log::info!("\n{}", pattern);
}
