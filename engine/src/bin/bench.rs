//! Deployment-faithful throughput benchmark (load generator).
//!
//! Spawns the `bench_server` binary as a **separate OS process** and drives it
//! over real tcp: one submitter connection plus N worker connections, each a real
//! libzmq DEALER. No in-process server, no unbounded channel — the server runs the
//! real bounded pipeline, fed by persistent per-task-type loaders started by the
//! benchmark server (submit writes to the DB; loaders stream records into the
//! lease channel). Stall detection is a safety net: if progress stops for
//! `BENCH_STALL_SECS`, the run reports `stalled` rather than hanging or lying.
//!
//! Usage: bench <n> [submit_batch] [lease_batch] [workers] [port]
//! Emits one JSON line on stdout.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use engine::bench_task::FibTask;
use engine::client::Client;
use enginelib::Identifier;
use enginelib::task::Task;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let n: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let submit_batch: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1000);
    let lease_batch: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(256);
    let workers: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(16);
    let port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(55610);

    let endpoint = format!("tcp://127.0.0.1:{port}");
    let task_type: Identifier = ("engine_mod".to_string(), "fib".to_string());
    let db_path = format!("target/bench_db_{port}_{}", std::process::id());

    // Launch the real server as its own process.
    let server_bin = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("bench_server")))
        .expect("locate bench_server next to bench");
    let mut server = Command::new(&server_bin)
        .arg(port.to_string())
        .arg(&db_path)
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn bench_server");
    // Give it time to bind the ROUTER.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let completed = Arc::new(AtomicU64::new(0));

    // Workers (separate DEALER connections): lease → decode + run → complete.
    let mut worker_handles = Vec::new();
    for _ in 0..workers {
        let endpoint = endpoint.clone();
        let task_type = task_type.clone();
        let completed = completed.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut client = match Client::connect(&endpoint, String::new()) {
                Ok(c) => c,
                Err(_) => return,
            };
            loop {
                let leased = match client
                    .lease(task_type.clone(), "w".into(), lease_batch)
                    .await
                {
                    Ok(v) => v,
                    Err(_) => break,
                };
                if leased.is_empty() {
                    continue;
                }
                let mut results = Vec::with_capacity(leased.len());
                for st in &leased {
                    let mut task: FibTask =
                        enginelib::api::from_bytes(&st.bytes).unwrap_or_default();
                    task.run_cpu();
                    results.push((st.task_id.clone(), task.to_bytes()));
                }
                match client.complete(task_type.clone(), results).await {
                    Ok(ok) => {
                        completed.fetch_add(ok as u64, Ordering::Relaxed);
                    }
                    Err(_) => break,
                }
            }
        }));
    }

    let payload = FibTask {
        iter: 20,
        result: 0,
    }
    .to_bytes();

    // Submitter: submit all N over its own connection, timed.
    let e2e_start = Instant::now();
    let submit_start = Instant::now();
    let submit_task = {
        let endpoint = endpoint.clone();
        let task_type = task_type.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            let mut client = Client::connect(&endpoint, String::new()).expect("submitter connect");
            let mut sent = 0u64;
            while sent < n {
                let this = std::cmp::min(submit_batch as u64, n - sent) as usize;
                if client
                    .submit(task_type.clone(), vec![payload.clone(); this])
                    .await
                    .is_err()
                {
                    break;
                }
                sent += this as u64;
            }
            sent
        })
    };
    let submitted = submit_task.await.unwrap_or(0);
    let submit_secs = submit_start.elapsed().as_secs_f64();

    // Wait for completion. Stall detection is a safety net (e.g. a wedged loader,
    // a crashed worker, or a genuine bug) — it reports `stalled` instead of
    // hanging forever, rather than describing any expected behavior.
    let stall_secs: u64 = std::env::var("BENCH_STALL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let mut last = 0u64;
    let mut last_change = Instant::now();
    let mut stalled = false;
    loop {
        let c = completed.load(Ordering::Relaxed);
        if c >= n {
            break;
        }
        if c != last {
            last = c;
            last_change = Instant::now();
        } else if last_change.elapsed() > Duration::from_secs(stall_secs) {
            stalled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let done = completed.load(Ordering::Relaxed);
    let e2e_secs = e2e_start.elapsed().as_secs_f64();

    for h in worker_handles {
        h.abort();
    }
    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&db_path);

    let submit_tps = submitted as f64 / submit_secs;
    let e2e_tps = done as f64 / e2e_secs;
    println!(
        "{{\"n\":{n},\"submitted\":{submitted},\"completed\":{done},\"stalled\":{stalled},\"workers\":{workers},\"submit_batch\":{submit_batch},\"lease_batch\":{lease_batch},\"submit_secs\":{submit_secs:.4},\"submit_tps\":{submit_tps:.1},\"e2e_secs\":{e2e_secs:.4},\"e2e_tps\":{e2e_tps:.1}}}"
    );
}
