// End-to-end TPS benchmark for the block-based gRPC pipeline.
//
// Spins the engine server in-process on a localhost ephemeral port, registers a
// no-op BenchTask, then runs two phases over real loopback gRPC:
//   1. Enqueue N tasks via CreateTaskBlock in chunks
//   2. Drain via W concurrent workers (acquire_task_block + publish_task_block)
// Prints per-phase elapsed/TPS and a linear extrapolation to 1B tasks.

use clap::Parser;
use engine::{EngineService, proto};
use enginelib::{
    Identifier, Registry,
    api::ServerAPI,
    task::{Task, Verifiable},
};
use proto::engine_client;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{RwLock, watch},
    task::JoinSet,
};
use tonic::{Request, transport::Server};

const BENCH_NS: &str = "bench";
const BENCH_TASK: &str = "noop";

fn parse_nonzero_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("expected positive integer: {err}"))?;
    if parsed == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_nonzero_u32(value: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|err| format!("expected positive integer: {err}"))?;
    if parsed == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(parsed)
    }
}

fn parse_nonzero_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|err| format!("expected positive integer: {err}"))?;
    if parsed == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(parsed)
    }
}

#[derive(Parser, Debug)]
#[command(name = "bench", about = "Engine block-based TPS bench")]
struct Args {
    /// Total tasks to enqueue + process.
    #[arg(long, default_value_t = 10_000_000, value_parser = parse_nonzero_usize)]
    tasks: usize,
    /// Worker count for the process phase.
    #[arg(long, default_value_t = num_cpus_fallback(), value_parser = parse_nonzero_usize)]
    workers: usize,
    /// Block size for enqueue (CreateTaskBlock chunks).
    #[arg(long, default_value_t = 1024, value_parser = parse_nonzero_u32)]
    block_size: u32,
    /// Payload size in bytes per task (zero-filled).
    #[arg(long, default_value_t = 16)]
    payload_bytes: usize,
    /// Per-acquire timeout so workers can recover if the queue unexpectedly stalls.
    #[arg(long, default_value_t = 1000, value_parser = parse_nonzero_u64)]
    acquire_timeout_ms: u64,
    /// Cargo passes this to harness-free benchmark targets.
    #[arg(long = "bench", hide = true, action = clap::ArgAction::SetTrue)]
    _cargo_bench: bool,
}

fn num_cpus_fallback() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Debug, Clone, Default)]
struct BenchTask;

impl Verifiable for BenchTask {
    fn verify(&self, _b: Vec<u8>) -> bool {
        true
    }
}

impl Task for BenchTask {
    fn get_id(&self) -> Identifier {
        (BENCH_NS.to_string(), BENCH_TASK.to_string())
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn run_hip(&mut self) {}
    fn run_cpu(&mut self) {}
    fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
    fn from_bytes(&self, _bytes: &[u8]) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn from_toml(&self, _d: String) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn to_toml(&self) -> String {
        String::new()
    }
}

/// Pick a free localhost port via an ephemeral TcpListener bind+drop.
async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn build_server_api() -> Arc<RwLock<ServerAPI>> {
    let mut api = ServerAPI::test_default();
    let id: Identifier = (BENCH_NS.to_string(), BENCH_TASK.to_string());
    api.task_registry.register(Arc::new(BenchTask), id.clone());
    api.ensure_task_channel(id);
    // Wire up inventory event handlers — the default core::auth_event handler
    // approves auth, which the bench client relies on (it sends no creds).
    enginelib::event::register_inventory_handlers(&mut api);
    Arc::new(RwLock::new(api))
}

async fn spawn_server(port: u16) -> Arc<RwLock<ServerAPI>> {
    let api = build_server_api().await;
    let engine = EngineService::new(api.clone());
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
    tokio::spawn(async move {
        Server::builder()
            .add_service(engine.into_server())
            .serve(addr)
            .await
            .ok();
    });
    api
}

async fn build_client(port: u16) -> engine_client::EngineClient<tonic::transport::Channel> {
    let url = format!("http://127.0.0.1:{}", port);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let endpoint = tonic::transport::Endpoint::try_from(url.clone())
            .unwrap()
            .tcp_nodelay(true);
        match endpoint.connect().await {
            Ok(channel) => return engine_client::EngineClient::new(channel),
            Err(err) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = err;
            }
            Err(err) => panic!("failed to connect to benchmark server on {url}: {err}"),
        }
    }
}

fn fmt_dur(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 3600.0 {
        format!("{:.2}h", secs / 3600.0)
    } else if secs >= 60.0 {
        format!("{:.2}m", secs / 60.0)
    } else {
        format!("{:.2}s", secs)
    }
}

fn fmt_tps(tps: f64) -> String {
    if tps >= 1_000_000.0 {
        format!("{:.2}M/s", tps / 1_000_000.0)
    } else if tps >= 1_000.0 {
        format!("{:.1}k/s", tps / 1_000.0)
    } else {
        format!("{:.0}/s", tps)
    }
}

async fn enqueue_phase(
    client: &mut engine_client::EngineClient<tonic::transport::Channel>,
    total: usize,
    block_size: u32,
    payload_bytes: usize,
) -> Duration {
    let task_id = format!("{}:{}", BENCH_NS, BENCH_TASK);
    let start = Instant::now();
    let mut sent = 0usize;
    while sent < total {
        let chunk = (block_size as usize).min(total - sent);
        let tasks: Vec<proto::Task> = (0..chunk)
            .map(|_| proto::Task {
                id: String::new(),
                task_id: task_id.clone(),
                task_payload: vec![0u8; payload_bytes],
                payload: Vec::new(),
            })
            .collect();
        client
            .create_task_block(Request::new(proto::TaskBlock { tasks }))
            .await
            .expect("create_task_block failed");
        sent += chunk;
    }
    start.elapsed()
}

async fn process_phase(
    port: u16,
    total: usize,
    workers: usize,
    block_size: u32,
    acquire_timeout: Duration,
) -> Result<Duration, String> {
    let task_id = format!("{}:{}", BENCH_NS, BENCH_TASK);
    let counter = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = watch::channel(false);
    let start = Instant::now();

    let mut handles = JoinSet::new();
    for _ in 0..workers {
        let task_id = task_id.clone();
        let counter = counter.clone();
        let done_tx = done_tx.clone();
        let mut done_rx = done_rx.clone();
        handles.spawn(async move {
            let mut client = build_client(port).await;
            loop {
                if *done_rx.borrow() || counter.load(Ordering::Relaxed) >= total {
                    break;
                }
                // The server-side acquire waits on a queue receive. The timeout
                // catches unexpected stalls, while the finished watch signal
                // keeps normal completion from paying the full timeout tail.
                let acquire = client.aquire_task_block(Request::new(proto::TaskBlockRequest {
                    task_id: task_id.clone(),
                    block_size,
                }));
                let resp = tokio::select! {
                    changed = done_rx.changed() => {
                        if changed.is_ok() && *done_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    resp = tokio::time::timeout(acquire_timeout, acquire) => resp,
                };
                let block = match resp {
                    Ok(Ok(r)) => r.into_inner(),
                    Ok(Err(status)) => {
                        if counter.load(Ordering::Relaxed) >= total {
                            break;
                        }
                        if matches!(
                            status.code(),
                            tonic::Code::InvalidArgument | tonic::Code::PermissionDenied
                        ) {
                            return Err(format!("acquire failed for {task_id}: {status}"));
                        }
                        continue;
                    }
                    Err(_) => {
                        if counter.load(Ordering::Relaxed) >= total {
                            break;
                        }
                        continue;
                    }
                };
                if block.tasks.is_empty() {
                    continue;
                }
                let n = block.tasks.len();
                // Echo the block right back as the publish. BenchTask::run_hip is
                // a no-op so we're measuring pure RPC + storage overhead.
                let solved: Vec<proto::Task> = block.tasks;
                match client
                    .publish_task_block(Request::new(proto::TaskBlock { tasks: solved }))
                    .await
                {
                    Ok(_) => {
                        let processed = counter.fetch_add(n, Ordering::Relaxed) + n;
                        if processed >= total {
                            let _ = done_tx.send(true);
                            break;
                        }
                    }
                    Err(status) => return Err(format!("publish failed for {task_id}: {status}")),
                }
            }
            Ok::<(), String>(())
        });
    }

    while let Some(result) = handles.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = done_tx.send(true);
                handles.abort_all();
                return Err(err);
            }
            Err(err) => {
                let _ = done_tx.send(true);
                handles.abort_all();
                return Err(format!("worker task failed: {err}"));
            }
        }
    }
    Ok(start.elapsed())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();

    println!(
        "engine bench: tasks={} workers={} block_size={} payload_bytes={}",
        args.tasks, args.workers, args.block_size, args.payload_bytes
    );

    let port = pick_port().await;
    let _server_api = spawn_server(port).await;
    let mut client = build_client(port).await;

    println!("Phase 1: enqueue");
    let enq = enqueue_phase(&mut client, args.tasks, args.block_size, args.payload_bytes).await;
    let enq_tps = args.tasks as f64 / enq.as_secs_f64();
    println!(
        "  {} tasks in {} ({})",
        args.tasks,
        fmt_dur(enq),
        fmt_tps(enq_tps)
    );

    println!("Phase 2: process");
    let proc = process_phase(
        port,
        args.tasks,
        args.workers,
        args.block_size,
        Duration::from_millis(args.acquire_timeout_ms),
    )
    .await
    .expect("process phase failed");
    let proc_tps = args.tasks as f64 / proc.as_secs_f64();
    println!(
        "  {} tasks in {} ({})",
        args.tasks,
        fmt_dur(proc),
        fmt_tps(proc_tps)
    );

    let total = enq + proc;
    let total_tps = args.tasks as f64 / total.as_secs_f64();
    println!(
        "Total wall: {} ({} end-to-end)",
        fmt_dur(total),
        fmt_tps(total_tps)
    );

    let scale = 1_000_000_000.0 / args.tasks as f64;
    println!("\n1B linear extrapolation (naive — scaling caveats below):");
    println!(
        "  Enqueue: {} @ {}",
        fmt_dur(enq.mul_f64(scale)),
        fmt_tps(enq_tps)
    );
    println!(
        "  Process: {} @ {}",
        fmt_dur(proc.mul_f64(scale)),
        fmt_tps(proc_tps)
    );
    println!(
        "  End-to-end: {} @ {}",
        fmt_dur(total.mul_f64(scale)),
        fmt_tps(total_tps)
    );

    println!("\nCaveats: linear extrapolation assumes:");
    println!("  - sled scales linearly with prefix size (it doesn't past tens of GB)");
    println!("  - leased_tasks DashMap memory is bounded (here it churns; at 1B it spikes)");
    println!("  - task_queue_size soft cap doesn't throttle producers");
    println!("  - loopback gRPC ≈ real network (it's typically 2-5x faster than LAN)");
}
