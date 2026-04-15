use enginelib::{
    api::EngineAPI,
    event::{error, info},
    plugin::LibraryInstance,
};
use proto::engine_client;
use tonic::Request;
//use enginelib::EventHandler;

use std::error::Error;

pub mod proto {
    tonic::include_proto!("engine");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut api = EngineAPI::default();
    EngineAPI::init_client(&mut api);
    let url = "http://[::1]:50051";
    let mut client = engine_client::EngineClient::connect(url).await?;

    // Get server metadata
    let server_meta = client
        .get_metadata(Request::new(proto::Empty {}))
        .await?
        .get_mut()
        .clone();
    // validate server
    assert!(server_meta.engine_api == enginelib::GIT_VERSION);
    for x in server_meta.mods {
        assert!(x.api_version == enginelib::GIT_VERSION);
        #[cfg(not(debug_assertions))]
        assert!(x.rustc_version == enginelib::RUSTC_VERSION);
        assert!(api.lib_manager.libraries.contains_key(&x.mod_id));
        let module: &LibraryInstance = api
            .lib_manager
            .libraries
            .get(&x.mod_id)
            .expect("Client Missing Mod");
        assert!(module.metadata.mod_version == x.mod_version)
        // Init CM(compute module)
    }
    Ok(())
}
