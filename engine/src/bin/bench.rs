// End-to-end TPS benchmark for the PoW task gRPC pipeline.
//
// Spins the engine server in-process on a localhost ephemeral port, registers a
// local copy of the engine_mod:pow task, then runs two phases over real loopback gRPC:
//   1. Enqueue N tasks via CreateTaskBlock in chunks
//   2. Drain via W concurrent workers (aquire_task_stream + publish_task_stream)
// Prints per-phase elapsed/TPS and a linear extrapolation to 1B tasks.

use clap::Parser;
use engine::{EngineService, proto};
use enginelib::{
    Identifier, Registry,
    api::ServerAPI,
    task::{Task, Verifiable},
};
use proto::engine_client;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    process::Command,
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

const BENCH_NS: &str = "engine_mod";
const BENCH_TASK: &str = "pow";

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
    /// Rayon CPU worker count used to solve tasks inside acquired blocks.
    #[arg(long, default_value_t = num_cpus_fallback(), value_parser = parse_nonzero_usize)]
    compute_workers: usize,
    /// Block size for enqueue (CreateTaskBlock chunks).
    #[arg(long, default_value_t = 1024, value_parser = parse_nonzero_u32)]
    block_size: u32,
    /// PoW leading zero nibbles required by each task.
    #[arg(long, default_value_t = 1)]
    pow_zeros: u8,
    /// First u32 seed to use when generating PoW task payloads.
    #[arg(long, default_value_t = 0)]
    seed_start: u32,
    /// Per-acquire timeout so workers can recover if the queue unexpectedly stalls.
    #[arg(long, default_value_t = 1000, value_parser = parse_nonzero_u64)]
    acquire_timeout_ms: u64,
    /// Legacy no-op benchmark argument retained for old command lines.
    #[arg(long, hide = true)]
    _payload_bytes: Option<usize>,
    /// Cargo passes this to harness-free benchmark targets.
    #[arg(long = "bench", hide = true, action = clap::ArgAction::SetTrue)]
    _cargo_bench: bool,
}

fn num_cpus_fallback() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PowTask {
    seed: u32,
    zeros: u8,
    nonce: u64,
}

impl PowTask {
    #[inline]
    fn has_leading_zero_nibbles(digest: &[u8; 32], zeros: u8) -> bool {
        let zeros = usize::from(zeros).min(64);
        let full_zero_bytes = zeros / 2;

        if digest[..full_zero_bytes].iter().any(|&b| b != 0) {
            return false;
        }

        if zeros % 2 == 1 {
            (digest[full_zero_bytes] & 0xF0) == 0
        } else {
            true
        }
    }
}

impl Verifiable for PowTask {
    fn verify(&self, b: Vec<u8>) -> bool {
        enginelib::api::postcard::from_bytes::<PowTask>(&b).is_ok()
    }
}

impl Task for PowTask {
    fn get_id(&self) -> Identifier {
        (BENCH_NS.to_string(), BENCH_TASK.to_string())
    }

    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }

    fn run_cpu(&mut self) {
        let zeros = self.zeros.min(64);
        let seed_bytes = self.seed.to_le_bytes();

        for n in 0..=u64::MAX {
            let mut hasher = Sha256::new();
            hasher.update(n.to_le_bytes());
            hasher.update(seed_bytes);
            let digest: [u8; 32] = hasher.finalize().into();

            if Self::has_leading_zero_nibbles(&digest, zeros) {
                self.nonce = n;
                return;
            }
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        enginelib::api::to_allocvec(self).unwrap()
    }

    fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
        Box::new(enginelib::api::from_bytes::<PowTask>(bytes).unwrap())
    }

    fn from_toml(&self, d: String) -> Box<dyn Task> {
        Box::new(toml::from_str::<PowTask>(&d).unwrap())
    }

    fn to_toml(&self) -> String {
        toml::to_string(self).unwrap()
    }
}

/// Pick a free localhost port via an ephemeral TcpListener bind+drop.
async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn build_server_api(block_size: u32) -> Arc<RwLock<ServerAPI>> {
    let mut api = ServerAPI::test_default();
    api.cfg.config_toml.task_block_size = block_size;
    let id: Identifier = (BENCH_NS.to_string(), BENCH_TASK.to_string());
    api.task_registry
        .register(Arc::new(PowTask::default()), id.clone());
    api.ensure_task_channel(id);
    // Wire up inventory event handlers — the default core::auth_event handler
    // approves auth, which the bench client relies on (it sends no creds).
    enginelib::event::register_inventory_handlers(&mut api);
    Arc::new(RwLock::new(api))
}

async fn spawn_server(port: u16, block_size: u32) -> Arc<RwLock<ServerAPI>> {
    let api = build_server_api(block_size).await;
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

fn clock_ticks_per_second() -> f64 {
    Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.trim().parse::<f64>().ok())
        .filter(|ticks| *ticks > 0.0)
        .unwrap_or(100.0)
}

fn process_cpu_time() -> Option<Duration> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat.rsplit_once(") ")?.1;
    let values: Vec<&str> = fields.split_whitespace().collect();
    let utime = values.get(11)?.parse::<u64>().ok()?;
    let stime = values.get(12)?.parse::<u64>().ok()?;
    let secs = (utime + stime) as f64 / clock_ticks_per_second();
    Some(Duration::from_secs_f64(secs))
}

fn cpu_delta_since(start: Option<Duration>) -> Option<Duration> {
    Some(process_cpu_time()?.saturating_sub(start?))
}

fn fmt_cpu(cpu: Option<Duration>, wall: Duration) -> String {
    let Some(cpu) = cpu else {
        return "cpu n/a".to_string();
    };
    let pct = if wall.is_zero() {
        0.0
    } else {
        cpu.as_secs_f64() / wall.as_secs_f64() * 100.0
    };
    format!("cpu {} ({pct:.0}%)", fmt_dur(cpu))
}

async fn enqueue_phase(
    client: &mut engine_client::EngineClient<tonic::transport::Channel>,
    total: usize,
    block_size: u32,
    zeros: u8,
    seed_start: u32,
) -> Duration {
    let task_id = format!("{}:{}", BENCH_NS, BENCH_TASK);
    let start = Instant::now();
    let mut sent = 0usize;
    while sent < total {
        let chunk = (block_size as usize).min(total - sent);
        let tasks: Vec<proto::Task> = (0..chunk)
            .map(|i| {
                let task = PowTask {
                    seed: seed_start.wrapping_add((sent + i) as u32),
                    zeros,
                    nonce: 0,
                };
                proto::Task {
                    id: String::new(),
                    task_id: task_id.clone(),
                    task_payload: task.to_bytes(),
                    payload: Vec::new(),
                }
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

#[derive(Debug)]
struct ProcessReport {
    elapsed: Duration,
    blocks_per_worker: Vec<usize>,
}

fn solve_pow_proto_task(task: proto::Task) -> Result<proto::Task, String> {
    let mut pow = enginelib::api::from_bytes::<PowTask>(&task.task_payload)
        .map_err(|err| format!("invalid PoW payload for {}: {err}", task.id))?;
    pow.run_cpu();
    Ok(proto::Task {
        id: task.id,
        task_id: task.task_id,
        task_payload: pow.to_bytes(),
        payload: Vec::new(),
    })
}

async fn process_phase(
    port: u16,
    total: usize,
    workers: usize,
    block_size: u32,
    acquire_timeout: Duration,
) -> Result<ProcessReport, String> {
    let task_id = format!("{}:{}", BENCH_NS, BENCH_TASK);
    let counter = Arc::new(AtomicUsize::new(0));
    let blocks_per_worker = Arc::new(
        (0..workers)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let (done_tx, done_rx) = watch::channel(false);
    let start = Instant::now();

    let mut handles = JoinSet::new();
    for worker_id in 0..workers {
        let task_id = task_id.clone();
        let counter = counter.clone();
        let blocks_per_worker = blocks_per_worker.clone();
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
                let acquire = client.aquire_task_stream(Request::new(proto::TaskBlockRequest {
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
                let mut task_stream = match resp {
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

                let mut acquired = Vec::with_capacity(block_size as usize);
                loop {
                    match task_stream.message().await {
                        Ok(Some(task)) => acquired.push(task),
                        Ok(None) => break,
                        Err(status) => {
                            return Err(format!("acquire stream failed for {task_id}: {status}"));
                        }
                    }
                }

                if acquired.is_empty() {
                    continue;
                }
                let n = acquired.len();
                blocks_per_worker[worker_id].fetch_add(1, Ordering::Relaxed);
                let solved: Vec<proto::Task> = tokio::task::spawn_blocking(move || {
                    acquired
                        .into_par_iter()
                        .map(solve_pow_proto_task)
                        .collect::<Result<_, String>>()
                })
                .await
                .map_err(|err| format!("PoW worker task failed: {err}"))??;
                match client
                    .publish_task_stream(Request::new(tokio_stream::iter(solved)))
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
    Ok(ProcessReport {
        elapsed: start.elapsed(),
        blocks_per_worker: blocks_per_worker
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect(),
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(args.compute_workers)
        .thread_name(|idx| format!("engine-bench-cpu-{idx}"))
        .build_global();

    println!(
        "engine bench: task={}:{} tasks={} workers={} compute_workers={} block_size={} pow_zeros={} seed_start={}",
        BENCH_NS,
        BENCH_TASK,
        args.tasks,
        args.workers,
        args.compute_workers,
        args.block_size,
        args.pow_zeros,
        args.seed_start
    );

    let port = pick_port().await;
    let _server_api = spawn_server(port, args.block_size).await;
    let mut client = build_client(port).await;

    println!("Phase 1: enqueue");
    let enq_cpu_start = process_cpu_time();
    let enq = enqueue_phase(
        &mut client,
        args.tasks,
        args.block_size,
        args.pow_zeros,
        args.seed_start,
    )
    .await;
    let enq_tps = args.tasks as f64 / enq.as_secs_f64();
    println!(
        "  {} tasks in {} ({}, {})",
        args.tasks,
        fmt_dur(enq),
        fmt_tps(enq_tps),
        fmt_cpu(cpu_delta_since(enq_cpu_start), enq)
    );

    println!("Phase 2: process");
    let proc_cpu_start = process_cpu_time();
    let proc_report = process_phase(
        port,
        args.tasks,
        args.workers,
        args.block_size,
        Duration::from_millis(args.acquire_timeout_ms),
    )
    .await
    .expect("process phase failed");
    let proc = proc_report.elapsed;
    let proc_tps = args.tasks as f64 / proc.as_secs_f64();
    println!(
        "  {} tasks in {} ({}, {})",
        args.tasks,
        fmt_dur(proc),
        fmt_tps(proc_tps),
        fmt_cpu(cpu_delta_since(proc_cpu_start), proc)
    );
    let active_workers = proc_report
        .blocks_per_worker
        .iter()
        .filter(|blocks| **blocks > 0)
        .count();
    let total_blocks: usize = proc_report.blocks_per_worker.iter().sum();
    let min_nonzero_blocks = proc_report
        .blocks_per_worker
        .iter()
        .copied()
        .filter(|blocks| *blocks > 0)
        .min()
        .unwrap_or(0);
    let max_blocks = proc_report
        .blocks_per_worker
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    println!(
        "  worker blocks: total={} active_workers={}/{} min_nonzero={} max={}",
        total_blocks, active_workers, args.workers, min_nonzero_blocks, max_blocks
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
    println!(
        "  - zeros={} PoW difficulty stays representative",
        args.pow_zeros
    );
    println!("  - task_queue_size soft cap doesn't throttle producers");
    println!("  - loopback gRPC ≈ real network (it's typically 2-5x faster than LAN)");
}
