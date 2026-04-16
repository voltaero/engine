use enginelib::{
    Identifier, Registry,
    api::EngineAPI,
    event::{error, info},
    plugin::LibraryInstance,
    prelude::debug,
};
use proto::engine_client;
use std::{error::Error, sync::Arc};
use tonic::{Request, Status, transport::Endpoint};

pub mod proto {
    tonic::include_proto!("engine");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut api = EngineAPI::default_client();
    EngineAPI::init_client(&mut api);
    let mut api_arc = Arc::new(api);
    // Init CM(compute module)
    loop {
        let api_inst = api_arc.clone();
        let handler = tokio::spawn(async move { compute_module(api_inst).await });
        match handler.await {
            Ok(_) => println!("Task finished normally"),
            Err(e) => {
                if e.is_panic() {
                    println!("Task panicked, but main is still alive");
                } else {
                    println!("Task failed: {:?}", e);
                }
            }
        }
    }
}
#[derive(Clone)]
struct Ctx {
    token: Vec<Identifier>,
}

// Compute Module
// Verifies server and also is
// Responsible for getting task, executing and publishing it.
async fn compute_module(api: Arc<EngineAPI>) {
    let ctx = Arc::new(Ctx { token: Vec::new() });
    let interceptor = move |mut req: Request<()>| {
        // access ctx here

        Ok(req)
    };

    let url = "http://[::1]:50051";
    let channel = Endpoint::from_static(url).connect().await.unwrap();
    let mut client = engine_client::EngineClient::with_interceptor(channel, interceptor);

    // Get server metadata
    let server_meta = client
        .get_metadata(Request::new(proto::Empty {}))
        .await
        .unwrap()
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
    }
    let mut task_reg = client
        .aquire_task_reg(Request::new(proto::Empty {}))
        .await
        .unwrap();

    loop {
        for x in task_reg.get_mut().clone().tasks.clone() {
            debug!("Aquring Task {}", x);
            let mut task_req = client
                .aquire_task(Request::new(proto::TaskRequest { task_id: x.clone() }))
                .await
                .unwrap();
            let task_payload = task_req.get_mut();
            let identifier = x.split_once(":").unwrap();
            let task = api
                .task_registry
                .get(&(identifier.0.to_string(), identifier.1.to_string()))
                .unwrap();
            let mut task = task.from_bytes(&task_payload.task_payload);
            info!("Running Task {}", x.clone());
            task.run_hip();
            let solv_task = proto::Task {
                id: task_payload.id.clone(),
                payload: Vec::new(),
                task_id: x.clone(),
                task_payload: task.to_bytes(),
            };
            info!("Publishing Task {}", x);
            client.publish_task(Request::new(solv_task)).await.unwrap();
        }
    }
}
