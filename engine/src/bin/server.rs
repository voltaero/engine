use enginelib::api::ServerAPI;
use enginelib::transport::server::{Server, ServerOptions};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let api = init_api();
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

#[cfg(not(feature = "wasm"))]
fn init_api() -> std::sync::Arc<ServerAPI> {
    ServerAPI::init()
}

/// `ServerAPI::init`, with `./mods/*.wasm` registered before `populate`.
#[cfg(feature = "wasm")]
fn init_api() -> std::sync::Arc<ServerAPI> {
    let mut api = ServerAPI::default();
    enginewasm::load_mods(&mut api, "./mods", enginewasm::SandboxLimits::default());
    let api = std::sync::Arc::new(api);
    ServerAPI::populate(&api);
    ServerAPI::spawn_reaper(&api);
    api
}
