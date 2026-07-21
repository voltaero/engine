use enginelib::api::ServerAPI;
use enginelib::events::Events;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let api = ServerAPI::init();
    Events::ServerStart(&api);

    // Loading (DB → lease channel) is owned by the loader tasks that serve()
    // spawns — one per registered task type. submit only writes to the DB; the
    // loaders stream those records in (with backpressure) and also perform
    // startup/restart recovery through the same path.
    let endpoint = format!("tcp://{}", api.cfg.config_toml.host);
    info!("engine server listening on {endpoint}");
    if let Err(e) = engine::server::serve(api, &endpoint).await {
        error!("server exited: {e}");
    }
}
