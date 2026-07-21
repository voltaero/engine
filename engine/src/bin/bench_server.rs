//! Standalone server process for the deployment-faithful benchmark.
//!
//! Mirrors `bin/server.rs` (the deployed pipeline): `populate()` builds the real
//! `bounded(8192)` channels, a startup `load()` recovers any persisted backlog,
//! then `serve()` runs the ROUTER over tcp. The only deviation from a real deploy
//! is that `FibTask` is statically linked here instead of dynamically loaded from
//! a `.rf` mod — that changes registration, not per-task runtime cost.
//!
//! Usage: bench_server <port> <db_path>

use std::sync::Arc;

use engine::bench_task::FibTask;
use enginelib::api::ServerAPI;
use enginelib::task::Task;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(55610);
    let db_path = args.next().unwrap_or_else(|| "target/bench_db".to_string());

    let task_type = ("engine_mod".to_string(), "fib".to_string());

    let api = ServerAPI::with_path(&db_path);
    let fib: Arc<dyn Task> = Arc::new(FibTask::default());
    api.task_registry.tasks.insert(task_type.clone(), fib);
    let api = Arc::new(api);
    // Real bounded queue + dedup set, as the deployed server builds. Depth is
    // configurable (ENGINE_QUEUE_SIZE, default 8192) for the sweep.
    let queue_size: usize = std::env::var("ENGINE_QUEUE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8192);
    ServerAPI::populate_with(&api, queue_size);
    // Match init()'s deployment behavior: run the lease reaper. (At the 3600s TTL
    // it never fires within a bench, but it's free and keeps this faithful.)
    ServerAPI::spawn_reaper(&api);
    // Submit only writes to the DB; one loader per task type feeds the bounded
    // lease queue. Transport shards must not create duplicate loaders.
    ServerAPI::spawn_loaders(&api);

    let endpoint = format!("tcp://127.0.0.1:{port}");
    eprintln!("bench_server listening on {endpoint} (db={db_path})");
    if let Err(e) = engine::server::serve(api, &endpoint).await {
        eprintln!("bench_server exited: {e}");
    }
}
