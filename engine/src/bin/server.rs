use engine::{
    cluster_client::{registration_from_api, spawn_registration},
    proto,
    service::backend::BackendEngineService,
};
use enginelib::{api::EngineAPI, events::Events};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::TcpListener, sync::RwLock};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut api = EngineAPI::default();
    EngineAPI::init(&mut api);
    Events::init_auth(&mut api);
    Events::StartEvent(&mut api);

    let addr = api
        .cfg
        .config_toml
        .host
        .parse()
        .unwrap_or(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            50051,
        )));
    let listener = TcpListener::bind(addr).await?;

    let api = Arc::new(RwLock::new(api));
    let registration = {
        let api_guard = api.read().await;
        registration_from_api(&api_guard)?
    };
    let _registration_shutdown = CancellationToken::new();
    let _registration_task = registration
        .map(|registration| spawn_registration(registration, _registration_shutdown.clone()));

    EngineAPI::init_chron(api.clone());
    let engine = BackendEngineService::new(api);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Server::builder()
        .add_service(reflection_service)
        .add_service(proto::engine_server::EngineServer::new(engine))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(())
}
