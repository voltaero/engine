//! End-to-end throughput benchmark: an in-process server on loopback TCP, N
//! inserter threads submitting batched `Insert`s of real payloads, and M worker
//! threads that lease, echo each payload back as its result and `Complete`
//! with `then_lease`. Every task is an individual payload with its own
//! verified, stored result.
//!
//! ```text
//! cargo run --release -p engine --bin bench -- --tasks 10000000 --inserters 4 --workers 8
//! ```
//!
//! Flags (all optional): `--tasks`, `--inserters`, `--workers`, `--batch`
//! (payloads per Insert), `--lease` (max_tasks per Lease), `--chunk`
//! (chunk_size), `--payload` (bytes per payload, >= 8), `--db` (store dir;
//! default a fresh temp dir, removed afterwards).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use enginelib::api::ServerAPI;
use enginelib::config::{Config, TaskTypeConfig};
use enginelib::error::ErrorKind;
use enginelib::nucleus::query::scan_tasks;
use enginelib::task::{Task, Verifiable};
use enginelib::transport::client::Client;
use enginelib::transport::server::{
    DEFAULT_MAX_INFLIGHT, DEFAULT_MAX_INFLIGHT_PER_SESSION, Server, ServerOptions,
};
use enginelib::wire::{Auth, Caps, LeaseRef, LeaseReq, Leased, Scope, TaskResult};
use enginelib::{Identifier, Registry};

#[derive(Debug, Clone)]
struct EchoTask;
impl Verifiable for EchoTask {
    fn verify(&self, b: &[u8]) -> bool {
        b.len() >= 8
    }
}
impl Task for EchoTask {
    fn get_id(&self) -> Identifier {
        task_type()
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
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

fn task_type() -> Identifier {
    ("bench".to_string(), "echo".to_string())
}

struct Args {
    tasks: u64,
    inserters: usize,
    workers: usize,
    batch: u64,
    lease: u32,
    chunk: u32,
    payload: usize,
    db: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = Args {
        tasks: 10_000_000,
        inserters: 4,
        workers: 8,
        batch: 8192,
        lease: 4096,
        chunk: 1024,
        payload: 16,
        db: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        let num = || -> u64 {
            value
                .parse()
                .unwrap_or_else(|_| panic!("bad {flag}: {value}"))
        };
        match flag.as_str() {
            "--tasks" => args.tasks = num(),
            "--inserters" => args.inserters = num() as usize,
            "--workers" => args.workers = num() as usize,
            "--batch" => args.batch = num(),
            "--lease" => args.lease = num() as u32,
            "--chunk" => args.chunk = num() as u32,
            "--payload" => args.payload = (num() as usize).max(8),
            "--db" => args.db = Some(PathBuf::from(&value)),
            other => panic!("unknown flag {other}"),
        }
    }
    // Zero would never finish: no batch advances `next`, no thread inserts or
    // completes anything.
    for (flag, value) in [
        ("--batch", args.batch),
        ("--inserters", args.inserters as u64),
        ("--workers", args.workers as u64),
    ] {
        if value == 0 {
            eprintln!("bench: {flag} must be at least 1");
            std::process::exit(2);
        }
    }
    args
}

/// Records the first failure of any bench thread (an error it returned, or a
/// panic) so the main loop stops instead of waiting for a completion count
/// that can no longer be reached.
#[derive(Default)]
struct Abort {
    flag: AtomicBool,
    reason: Mutex<Option<String>>,
}

impl Abort {
    fn set(&self, reason: String) {
        let mut slot = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        slot.get_or_insert(reason);
        self.flag.store(true, Ordering::SeqCst);
    }

    fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn reason(&self) -> Option<String> {
        self.reason
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// Spawns a bench thread whose error or panic aborts the run.
fn spawn_guarded(
    name: String,
    abort: &Arc<Abort>,
    body: impl FnOnce() -> Result<(), String> + Send + 'static,
) -> std::thread::JoinHandle<()> {
    let abort = abort.clone();
    std::thread::spawn(
        move || match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => abort.set(format!("{name}: {err}")),
            Err(_) => abort.set(format!("{name} panicked")),
        },
    )
}

fn auth(uid: &str) -> Auth {
    Auth {
        uid: uid.into(),
        challenge: String::new(),
        admin_token: None,
    }
}

fn results_for(leased: &Leased) -> Vec<TaskResult> {
    leased
        .tasks
        .iter()
        .map(|task| TaskResult {
            lease: LeaseRef {
                task_type: leased.task_type.clone(),
                task_id: task.task_id.clone(),
                token: task.token,
            },
            bytes: task.bytes.clone(),
        })
        .collect()
}

fn rate(n: u64, elapsed: Duration) -> f64 {
    n as f64 / elapsed.as_secs_f64().max(1e-9)
}

fn main() {
    let args = parse_args();
    let (db, temp) = match &args.db {
        Some(db) => (db.clone(), false),
        None => (
            std::env::temp_dir().join(format!("engine-bench-{}", std::process::id())),
            true,
        ),
    };

    let mut cfg = Config::default();
    cfg.config_toml.queue_capacity = (args.chunk as usize * 64).max(65_536);
    cfg.config_toml.tasks.insert(
        "bench:echo".into(),
        TaskTypeConfig {
            chunk_size: Some(args.chunk),
            ..Default::default()
        },
    );
    let mut api = ServerAPI::open(&db, cfg);
    api.task_registry.register(Arc::new(EchoTask), task_type());
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    ServerAPI::spawn_reaper(&api);
    // Leftover pending tasks (an interrupted run in `--db`) would be leased and
    // counted as this run's completions, skewing the rates.
    let (leftover, _) =
        scan_tasks(&api, &task_type(), Scope::Pending, None, 1).expect("scan the store");
    if !leftover.is_empty() {
        eprintln!(
            "bench: {} already holds pending bench tasks; use an empty --db",
            db.display()
        );
        std::process::exit(2);
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = {
        let _guard = rt.enter();
        Server::start(
            api.clone(),
            ServerOptions {
                endpoint: "tcp://127.0.0.1:*".into(),
                server_id: 0,
                advertise: None,
                proxy: None,
                proxy_token: None,
                heartbeat: Duration::from_secs(5),
                max_message_bytes: 256 * 1024 * 1024,
                session_idle: Duration::from_secs(900),
                max_inflight: DEFAULT_MAX_INFLIGHT,
                max_inflight_per_session: DEFAULT_MAX_INFLIGHT_PER_SESSION,
            },
        )
        .expect("server start")
    };
    let endpoint = server.endpoint().to_string();

    println!(
        "bench: tasks={} inserters={} workers={} batch={} lease={} chunk={} payload={}B db={}",
        args.tasks,
        args.inserters,
        args.workers,
        args.batch,
        args.lease,
        args.chunk,
        args.payload,
        db.display()
    );

    let next = Arc::new(AtomicU64::new(0));
    let inserted = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let abort = Arc::new(Abort::default());
    let started = Instant::now();

    let mut inserters = Vec::new();
    for i in 0..args.inserters {
        let (endpoint, next, inserted, failed, done) = (
            endpoint.clone(),
            next.clone(),
            inserted.clone(),
            failed.clone(),
            done.clone(),
        );
        let (total, batch, payload) = (args.tasks, args.batch, args.payload);
        inserters.push(spawn_guarded(format!("inserter {i}"), &abort, move || {
            let mut client = Client::connect(&endpoint, auth(&format!("ins{i}")), Caps::default())
                .map_err(|err| format!("connect: {err}"))?;
            loop {
                let start = next.fetch_add(batch, Ordering::Relaxed);
                if start >= total || done.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let end = (start + batch).min(total);
                let payloads: Vec<Vec<u8>> = (start..end)
                    .map(|n| {
                        let mut p = vec![0u8; payload];
                        p[..8].copy_from_slice(&n.to_le_bytes());
                        p
                    })
                    .collect();
                let ids = client
                    .insert(task_type(), payloads)
                    .map_err(|err| format!("insert: {err}"))?;
                let ok = ids.iter().filter(|id| id.is_ok()).count() as u64;
                failed.fetch_add(ids.len() as u64 - ok, Ordering::Relaxed);
                inserted.fetch_add(ok, Ordering::Relaxed);
            }
        }));
    }

    let mut workers = Vec::new();
    for i in 0..args.workers {
        let (endpoint, completed, failed, done) = (
            endpoint.clone(),
            completed.clone(),
            failed.clone(),
            done.clone(),
        );
        let lease = args.lease;
        workers.push(spawn_guarded(format!("worker {i}"), &abort, move || {
            let mut client = Client::connect(&endpoint, auth(&format!("wrk{i}")), Caps::default())
                .map_err(|err| format!("connect: {err}"))?;
            let mut held: Option<Leased> = None;
            while !done.load(Ordering::Relaxed) {
                let leased = match held.take() {
                    Some(leased) => leased,
                    None => match client.lease(task_type(), lease, Duration::from_millis(100)) {
                        Ok(leased) => leased,
                        // Nothing queued yet: re-check `done` and ask again.
                        Err(err)
                            if err.kind() == ErrorKind::NotFound
                                && err.as_ref() == "QueueEmpty" =>
                        {
                            continue;
                        }
                        // Anything else (server gone, protocol error) stops this
                        // worker, which aborts the run instead of spinning.
                        Err(err) => return Err(format!("lease: {err}")),
                    },
                };
                let (results, next) = client
                    .complete(
                        results_for(&leased),
                        Some(LeaseReq {
                            task_type: task_type(),
                            max_tasks: lease,
                        }),
                    )
                    .map_err(|err| format!("complete: {err}"))?;
                let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
                failed.fetch_add(results.len() as u64 - ok, Ordering::Relaxed);
                completed.fetch_add(ok, Ordering::Relaxed);
                held = next;
            }
            Ok(())
        }));
    }

    // Progress once a second until everything is completed.
    let mut insert_done_at: Option<Duration> = None;
    let mut last = (Instant::now(), 0u64, 0u64);
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (ins, comp) = (
            inserted.load(Ordering::Relaxed),
            completed.load(Ordering::Relaxed),
        );
        if insert_done_at.is_none() && ins >= args.tasks {
            insert_done_at = Some(started.elapsed());
        }
        if last.0.elapsed() >= Duration::from_secs(1) {
            let dt = last.0.elapsed();
            println!(
                "  t={:>6.1}s inserted={:>11} ({:>9.0}/s)  completed={:>11} ({:>9.0}/s)",
                started.elapsed().as_secs_f64(),
                ins,
                rate(ins - last.1, dt),
                comp,
                rate(comp - last.2, dt),
            );
            last = (Instant::now(), ins, comp);
        }
        if comp >= args.tasks || failed.load(Ordering::Relaxed) > 0 || abort.is_set() {
            break;
        }
    }
    let total_elapsed = started.elapsed();
    done.store(true, Ordering::Relaxed);
    for thread in inserters.into_iter().chain(workers) {
        let _ = thread.join();
    }

    if let Some(reason) = abort.reason() {
        eprintln!("bench: aborted: {reason}");
    }
    let mut client = Client::connect(&endpoint, auth("stats"), Caps::default()).expect("connect");
    let stats = client.stats().expect("stats");
    let insert_elapsed = insert_done_at.unwrap_or(total_elapsed);
    println!("results:");
    println!(
        "  insert:   {} tasks in {:.2}s = {:.0} tasks/s",
        args.tasks,
        insert_elapsed.as_secs_f64(),
        rate(args.tasks, insert_elapsed)
    );
    println!(
        "  complete: {} tasks in {:.2}s = {:.0} tasks/s (end to end, insert overlapped)",
        completed.load(Ordering::Relaxed),
        total_elapsed.as_secs_f64(),
        rate(completed.load(Ordering::Relaxed), total_elapsed)
    );
    println!(
        "  failed={} stats: inserted={} leased={} completed={} queued={} active_leases={}",
        failed.load(Ordering::Relaxed),
        stats.inserted,
        stats.leased,
        stats.completed,
        stats.queued,
        stats.active_leases
    );

    drop(client);
    server.shutdown();
    drop(rt);
    drop(api);
    if temp {
        let _ = std::fs::remove_dir_all(&db);
    }
    if abort.is_set() || failed.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}
