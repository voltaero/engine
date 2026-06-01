use enginelib::{
    Registry, api::ServerAPI, event::info, events::Events, plugin::LibraryInstance, prelude::debug,
};
use proto::engine_client;
use rayon::prelude::*;
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
    let mut api = ServerAPI::default_client();
    ServerAPI::init_client(&mut api);
    Events::ClientStart(&api);

    compute_module(Arc::new(api)).await;
    Ok(())
}

fn make_interceptor(
    api_for_interceptor: Arc<ServerAPI>,
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
    api: Arc<ServerAPI>,
    channel: tonic::transport::Channel,
    task_ids: Arc<Vec<String>>,
) {
    let interceptor = make_interceptor(api.clone());
    let mut client = engine_client::EngineClient::with_interceptor(channel, interceptor);

    loop {
        let mut got_any = false;

        for task_id in task_ids.iter() {
            if Events::BeforeTaskBlockAcquire(api.as_ref(), vec![task_id.clone()]) {
                continue;
            }

            let resp = client
                .aquire_task_stream(Request::new(proto::TaskBlockRequest {
                    task_id: task_id.clone(),
                    block_size: 0,
                }))
                .await;

            let mut task_stream = match resp {
                Ok(r) => r.into_inner(),
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
                    debug!(
                        "worker {}: acquire failed for {}: {:?}",
                        worker_id, task_id, status
                    );
                    continue;
                }
            };

            let mut tasks = Vec::new();
            loop {
                match task_stream.message().await {
                    Ok(Some(task)) => tasks.push(task),
                    Ok(None) => break,
                    Err(status) => {
                        debug!(
                            "worker {}: acquire stream failed for {}: {:?}",
                            worker_id, task_id, status
                        );
                        tasks.clear();
                        break;
                    }
                }
            }

            let block = proto::TaskBlock { tasks };
            if block.tasks.is_empty() {
                continue;
            }
            got_any = true;

            let identifier = match task_id.split_once(':') {
                Some(v) => (v.0.to_string(), v.1.to_string()),
                None => continue,
            };
            let task_def = match api.task_registry.get(&identifier) {
                Some(t) => t,
                None => continue,
            };

            let instance_ids: Vec<String> = block.tasks.iter().map(|t| t.id.clone()).collect();
            let acquired_payloads: Vec<Arc<std::sync::RwLock<Vec<u8>>>> = block
                .tasks
                .iter()
                .map(|t| Arc::new(std::sync::RwLock::new(t.task_payload.clone())))
                .collect();

            Events::TaskBlockAcquired(
                api.as_ref(),
                task_id.clone(),
                instance_ids.clone(),
                acquired_payloads.clone(),
            );

            if Events::BeforeTaskBlockExecute(
                api.as_ref(),
                task_id.clone(),
                instance_ids.clone(),
                acquired_payloads.clone(),
            ) {
                continue;
            }

            let task_id_for_execute = task_id.clone();
            let tasks_for_execute = block.tasks;
            let payloads_for_execute = acquired_payloads.clone();
            let task_def_for_execute = task_def.clone();
            let execute_result = tokio::task::spawn_blocking(move || {
                tasks_for_execute
                    .into_par_iter()
                    .zip(payloads_for_execute.into_par_iter())
                    .filter_map(|(t, payload_lock)| {
                        let payload = match payload_lock.read() {
                            Ok(p) => p.clone(),
                            Err(_) => return None,
                        };
                        let mut tsk = task_def_for_execute.clone().from_bytes(&payload);
                        tsk.run_hip();
                        let out_bytes = tsk.to_bytes();
                        let publish_payload_lock =
                            Arc::new(std::sync::RwLock::new(out_bytes.clone()));
                        let solved = proto::Task {
                            id: t.id,
                            task_id: task_id_for_execute.clone(),
                            task_payload: out_bytes,
                            payload: Vec::new(),
                        };
                        Some((solved, publish_payload_lock))
                    })
                    .unzip::<_, _, Vec<_>, Vec<_>>()
            })
            .await;

            let (mut solved, publish_payload_locks) = match execute_result {
                Ok(result) => result,
                Err(err) => {
                    debug!(
                        "worker {}: execute failed for {}: {:?}",
                        worker_id, task_id, err
                    );
                    continue;
                }
            };

            if solved.is_empty() {
                continue;
            }

            if Events::BeforeTaskBlockPublish(
                api.as_ref(),
                task_id.clone(),
                instance_ids,
                publish_payload_locks.clone(),
            ) {
                continue;
            }

            // Sync any handler-modified payloads back into the outgoing block.
            for (s, lock) in solved.iter_mut().zip(publish_payload_locks.iter()) {
                if let Ok(p) = lock.read() {
                    s.task_payload = p.clone();
                }
            }

            if let Err(status) = client
                .publish_task_stream(Request::new(tokio_stream::iter(solved)))
                .await
            {
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
async fn compute_module(api: Arc<ServerAPI>) {
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
    let compute_worker_count = std::env::var("GE_CLIENT_COMPUTE_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(worker_count);
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(compute_worker_count)
        .thread_name(|idx| format!("engine-client-cpu-{idx}"))
        .build_global();

    info!(
        "Starting {} client workers and {} compute workers for {} task types",
        worker_count,
        compute_worker_count,
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
