use std::sync::Arc;

use engine::{
    proto,
    proxy_config::ProxyConfigToml,
    routing::ProxyState,
    service::proxy::{ProxyService, spawn_reaper},
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ProxyConfigToml::load()?;
    let listener = TcpListener::bind(config.listen.as_str()).await?;
    let state = Arc::new(ProxyState::new(config)?);
    let _reaper = spawn_reaper(state.clone());
    let cluster_service = ProxyService::new(state.clone());
    let engine_service = ProxyService::new(state);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Server::builder()
        .add_service(reflection_service)
        .add_service(proto::cluster_server::ClusterServer::new(cluster_service))
        .add_service(proto::engine_server::EngineServer::new(engine_service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(())
}
