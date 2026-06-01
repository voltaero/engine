use engine::{EngineService, proto};
use enginelib::{api::ServerAPI, event::info, events::Events};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::sync::RwLock;
use tonic::transport::Server;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut api = ServerAPI::default();
    ServerAPI::init(&mut api);
    Events::init_auth(&mut api);
    Events::StartEvent(&mut api);
    Events::ServerStart(&api);
    let addr = api
        .cfg
        .config_toml
        .host
        .parse()
        .unwrap_or(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            50051,
        )));
    let apii = Arc::new(RwLock::new(api));
    ServerAPI::init_chron(apii.clone());
    let engine = EngineService::new(apii);

    info!("Engine listening on {}", addr);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Server::builder()
        .add_service(reflection_service)
        .add_service(engine.into_server())
        .serve(addr)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(())
}
