use enginelib::api::ServerAPI;
use enginelib::transport::server::{Server, ServerOptions};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let api = ServerAPI::init();
    let opts = ServerOptions::from_config(&api.cfg.config_toml);
    let server = match Server::start(api, opts) {
        Ok(server) => server,
        Err(err) => {
            error!("Failed to start server: {err}");
            std::process::exit(1);
        }
    };
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down");
    server.shutdown();
}
