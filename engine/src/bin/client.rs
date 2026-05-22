use enginelib::{
    Registry,
    api::EngineAPI,
    events::Events,
    event::info,
    plugin::LibraryInstance,
    prelude::debug,
};
use proto::engine_client;
use std::{collections::HashMap, error::Error, sync::Arc};
use tonic::{
    Request,
    metadata::{MetadataKey, MetadataValue},
    transport::Endpoint,
};

pub mod proto {
    tonic::include_proto!("engine");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut api = EngineAPI::default_client();
    EngineAPI::init_client(&mut api);
    Events::ClientStart(&api);

    compute_module(Arc::new(api)).await;
    Ok(())
}

fn make_interceptor(
    api_for_interceptor: Arc<EngineAPI>,
) -> impl FnMut(Request<()>) -> Result<Request<()>, tonic::Status> + Clone {
    move |mut req: Request<()>| {
        let headers = Arc::new(std::sync::RwLock::new(HashMap::<String, String>::new()));
        Events::ClientAuthPrepare(api_for_interceptor.as_ref(), headers.clone());

        if let Ok(headers) = headers.read() {
            for (key, value) in headers.iter() {
                if let (Ok(key), Ok(value)) = (
                    MetadataKey::from_bytes(key.as_bytes()),
                    MetadataValue::try_from(value.as_str()),
                ) {
                    req.metadata_mut().insert(key, value);
                }
            }
        }

        Ok(req)
    }
}

async fn worker_loop(
    worker_id: usize,
    api: Arc<EngineAPI>,
    channel: tonic::transport::Channel,
    task_ids: Arc<Vec<String>>,
) {
    let interceptor = make_interceptor(api.clone());
    let mut client = engine_client::EngineClient::with_interceptor(channel, interceptor);

    loop {
        let mut got_any = false;

        for task_id in task_ids.iter() {
            if Events::BeforeTaskAcquire(api.as_ref(), task_id.clone()) {
                continue;
            }

            let task_resp = client
                .aquire_task(Request::new(proto::TaskRequest {
                    task_id: task_id.clone(),
                }))
                .await;

            let mut task_req = match task_resp {
                Ok(resp) => {
                    got_any = true;
                    resp
                }
                Err(status) if status.code() == tonic::Code::NotFound => continue,
                Err(status) if status.code() == tonic::Code::PermissionDenied => {
                    debug!(
                        "worker {}: auth failed during acquire for {}",
                        worker_id, task_id
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                Err(status) => {
                    debug!("worker {}: acquire failed for {}: {:?}", worker_id, task_id, status);
                    continue;
                }
            };

            let task_payload = task_req.get_mut();

            let acquired_payload = Arc::new(std::sync::RwLock::new(task_payload.task_payload.clone()));
            Events::TaskAcquired(
                api.as_ref(),
                task_id.clone(),
                task_payload.id.clone(),
                acquired_payload.clone(),
            );
            if let Ok(payload) = acquired_payload.read() {
                task_payload.task_payload = payload.clone();
            }

            let exec_payload = Arc::new(std::sync::RwLock::new(task_payload.task_payload.clone()));
            if Events::BeforeTaskExecute(
                api.as_ref(),
                task_id.clone(),
                task_payload.id.clone(),
                exec_payload.clone(),
            ) {
                continue;
            }
            if let Ok(payload) = exec_payload.read() {
                task_payload.task_payload = payload.clone();
            }

            let identifier = match task_id.split_once(':') {
                Some(v) => v,
                None => continue,
            };
            let task = match api
                .task_registry
                .get(&(identifier.0.to_string(), identifier.1.to_string()))
            {
                Some(t) => t,
                None => continue,
            };
            let mut task = task.from_bytes(&task_payload.task_payload);

            task.run_hip();

            let mut solv_task = proto::Task {
                id: task_payload.id.clone(),
                payload: Vec::new(),
                task_id: task_id.clone(),
                task_payload: task.to_bytes(),
            };

            let publish_payload = Arc::new(std::sync::RwLock::new(solv_task.task_payload.clone()));
            if Events::BeforeTaskPublish(
                api.as_ref(),
                task_id.clone(),
                task_payload.id.clone(),
                publish_payload.clone(),
            ) {
                continue;
            }
            if let Ok(payload) = publish_payload.read() {
                solv_task.task_payload = payload.clone();
            }

            if let Err(status) = client.publish_task(Request::new(solv_task)).await {
                debug!(
                    "worker {}: publish failed for {}: {:?}",
                    worker_id, task_id, status
                );
            }
        }

        if !got_any {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}

// Compute Module
// Verifies server and also is
// Responsible for getting task, executing and publishing it.
async fn compute_module(api: Arc<EngineAPI>) {
    let url = "http://[::1]:50051";
    let endpoint = Endpoint::from_static(url)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)));

    let channel = endpoint.connect().await.unwrap();
    let interceptor = make_interceptor(api.clone());
    let mut client = engine_client::EngineClient::with_interceptor(channel.clone(), interceptor);

    // Get server metadata
    let server_meta = client
        .get_metadata(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();

    // validate server
    assert!(server_meta.engine_api == enginelib::GIT_VERSION);
    for x in &server_meta.mods {
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

    let task_reg = client
        .aquire_task_reg(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();

    let task_ids = Arc::new(task_reg.tasks);
    let worker_count = std::env::var("GE_CLIENT_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });

    info!(
        "Starting {} client workers for {} task types",
        worker_count,
        task_ids.len()
    );

    let mut handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let api_i = api.clone();
        let channel_i = channel.clone();
        let task_ids_i = task_ids.clone();
        handles.push(tokio::spawn(async move {
            worker_loop(worker_id, api_i, channel_i, task_ids_i).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}
