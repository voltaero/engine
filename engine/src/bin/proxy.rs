use enginelib::config::Config;
use enginelib::transport::proxy::{Proxy, ProxyOptions};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    enginelib::api::init_tracing();
    let cfg = Config::new();
    let proxy = match Proxy::start(ProxyOptions::from_config(&cfg.config_toml)) {
        Ok(proxy) => proxy,
        Err(err) => {
            error!("Failed to start proxy: {err}");
            std::process::exit(1);
        }
    };
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down");
    proxy.shutdown();
}
