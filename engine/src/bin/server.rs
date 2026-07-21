use std::sync::Arc;

use engine::transport::TransportStats;
use enginelib::api::ServerAPI;
use enginelib::config::Config;
use enginelib::events::Events;
use tokio::sync::watch;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let config = match Config::load_checked() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("server configuration error: {err}");
            std::process::exit(2);
        }
    };
    let transport = config.config_toml.transport.clone();
    let endpoint = format!("tcp://{}", config.config_toml.host);

    let api = ServerAPI::init_with_config(config);
    Events::ServerStart(&api);
    ServerAPI::spawn_loaders(&api);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_shutdown().await;
        let _ = shutdown_tx.send(true);
    });

    let stats = Arc::new(TransportStats::default());
    info!("engine server listening on {endpoint}");
    if let Err(err) = engine::transport::serve_with_shutdown(
        api,
        &endpoint,
        transport,
        shutdown_rx,
        stats.clone(),
    )
    .await
    {
        error!("server exited: {err}");
    }
    info!(
        admitted = stats.admitted(),
        overloaded = stats.overloaded(),
        dropped_replies = stats.dropped_replies(),
        "server transport stopped"
    );
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
