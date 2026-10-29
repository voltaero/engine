//! Experimental worker for sandboxed wasm task mods: leases a mod's tasks and
//! runs them on the GPU (the mod's WGSL kernel) or the CPU (its `ge_run`).
//!
//! ```text
//! # Worker against a running server that loaded the same mod:
//! cargo run --release -p engine --features wasm --bin wasm_worker -- \
//!     --mod mods/wasm_mix.wasm --server tcp://127.0.0.1:50051 --backend auto
//!
//! # Local native vs wasm vs GPU comparison, no server:
//! cargo run --release -p engine --features wasm --bin wasm_worker -- \
//!     --mod mods/wasm_mix.wasm --bench 100000 --words 256
//! ```
//!
//! Sandbox limits and grants (memory, timeouts, files, network) come from the
//! `[wasm]` table of `--config` (default `config.toml`), as on the server; see
//! `docs/wasm.md`.
//!
//! Flags: `--mod` (required), `--config`, `--server`, `--backend` (auto|cpu|gpu),
//! `--lease` (max_tasks per Lease), `--threads` (CPU threads), `--uid`,
//! `--challenge`, `--bench` (task count; runs locally and exits), `--words`
//! (u32 words per bench payload). For `demo:mix` the bench adds a native row
//! built from the example's own source, and reports speedups against it.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use enginelib::error::ErrorKind;
use enginelib::transport::client::Client;
use enginelib::wire::{Auth, Caps, LeaseRef, LeaseReq, Leased, RunnerKind, TaskResult};
use enginewasm::gpu::{Gpu, GpuKernel};
use enginewasm::{WasmConfig, WasmMod};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Backend {
    Auto,
    Cpu,
    Gpu,
}

struct Args {
    module: String,
    config: String,
    server: String,
    backend: Backend,
    lease: u32,
    threads: usize,
    uid: String,
    challenge: String,
    bench: Option<usize>,
    words: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        module: String::new(),
        config: "config.toml".into(),
        server: "tcp://127.0.0.1:50051".into(),
        backend: Backend::Auto,
        lease: 4096,
        threads: std::thread::available_parallelism().map_or(4, |n| n.get()),
        uid: format!("wasm-worker-{}", std::process::id()),
        challenge: String::new(),
        bench: None,
        words: 256,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .unwrap_or_else(|| fail(&format!("missing value for {flag}")));
        let num = || -> usize {
            value
                .parse()
                .unwrap_or_else(|_| fail(&format!("bad {flag}: {value}")))
        };
        match flag.as_str() {
            "--mod" => args.module = value.clone(),
            "--config" => args.config = value.clone(),
            "--server" => args.server = value.clone(),
            "--backend" => {
                args.backend = match value.as_str() {
                    "auto" => Backend::Auto,
                    "cpu" => Backend::Cpu,
                    "gpu" => Backend::Gpu,
                    other => fail(&format!("bad --backend {other}")),
                }
            }
            "--lease" => args.lease = num().max(1) as u32,
            "--threads" => args.threads = num().max(1),
            "--uid" => args.uid = value.clone(),
            "--challenge" => args.challenge = value.clone(),
            "--bench" => args.bench = Some(num().max(1)),
            "--words" => args.words = num().max(1),
            other => fail(&format!("unknown flag {other}")),
        }
    }
    if args.module.is_empty() {
        fail("--mod <file.wasm> is required");
    }
    args
}

fn fail(msg: &str) -> ! {
    eprintln!("wasm_worker: {msg}");
    std::process::exit(2);
}

struct Runner<'a> {
    m: &'a WasmMod,
    gpu: Option<(Gpu, GpuKernel)>,
    threads: usize,
}

impl Runner<'_> {
    fn run(&self, payloads: &[&[u8]]) -> Vec<enginewasm::Result<Vec<u8>>> {
        match &self.gpu {
            Some((gpu, kernel)) => gpu.run_batch(kernel, payloads),
            None => self.m.run_batch(payloads, self.threads),
        }
    }
}

fn open_gpu(m: &WasmMod, backend: Backend) -> Option<(Gpu, GpuKernel)> {
    if backend == Backend::Cpu || m.wgsl().is_none() {
        if backend == Backend::Gpu {
            fail("--backend gpu, but the mod has no GPU kernel");
        }
        return None;
    }
    match Gpu::new().and_then(|gpu| gpu.kernel(m).map(|k| (gpu, k))) {
        Ok((gpu, kernel)) => {
            println!("gpu: {} ({:?})", gpu.info().name, gpu.info().backend);
            Some((gpu, kernel))
        }
        Err(err) if backend == Backend::Gpu => fail(&format!("gpu: {err}")),
        Err(err) => {
            println!("gpu unavailable ({err}); using the CPU");
            None
        }
    }
}

fn main() {
    let args = parse_args();
    let config = WasmConfig::from_file(&args.config)
        .unwrap_or_else(|err| fail(&format!("{}: {err}", args.config)));
    let m = WasmMod::from_file(&args.module, &config)
        .unwrap_or_else(|err| fail(&format!("load {}: {err}", args.module)));
    println!("loaded {:?} from {}", m.task_type(), args.module);
    if let Some(n) = args.bench {
        bench(&m, &args, n);
        return;
    }
    let runner = Runner {
        gpu: open_gpu(&m, args.backend),
        m: &m,
        threads: args.threads,
    };
    work(&runner, &args);
}

/// The no-wasm baseline: `examples/wasm-mix`'s task, compiled natively from
/// the very file the wasm guest is built from. Only used for `demo:mix`.
#[path = "../../../examples/wasm-mix/src/mix.rs"]
mod mix;

fn native_batch(payloads: &[&[u8]], threads: usize) -> Vec<enginewasm::Result<Vec<u8>>> {
    let per = payloads.len().div_ceil(threads.max(1)).max(1);
    std::thread::scope(|scope| {
        let parts: Vec<_> = payloads
            .chunks(per)
            .map(|part| {
                scope.spawn(move || {
                    part.iter()
                        .map(|p| {
                            let mut out = vec![0u8; p.len()];
                            mix::mix_words(p, &mut out);
                            Ok(out)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        parts
            .into_iter()
            .flat_map(|h| h.join().expect("native thread"))
            .collect()
    })
}

/// (label, elapsed, per-task results)
type BenchRow = (String, Duration, Vec<enginewasm::Result<Vec<u8>>>);

fn bench(m: &WasmMod, args: &Args, n: usize) {
    let payloads: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            (0..args.words)
                .flat_map(|w| ((i * args.words + w) as u32).to_le_bytes())
                .collect()
        })
        .collect();
    let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
    let mb = (n * args.words * 4) as f64 / 1e6;
    let mut rows: Vec<BenchRow> = Vec::new();

    if m.task_type() == ("demo".to_string(), "mix".to_string()) {
        let start = Instant::now();
        let out = native_batch(&refs, args.threads);
        rows.push((
            format!("native ({} threads)", args.threads),
            start.elapsed(),
            out,
        ));
    }

    let start = Instant::now();
    let out = m.run_batch(&refs, args.threads);
    let width = if m.is_wasm64() { "wasm64" } else { "wasm32" };
    rows.push((
        format!("{width} ({} threads)", args.threads),
        start.elapsed(),
        out,
    ));

    let backend = if args.backend == Backend::Cpu {
        Backend::Cpu
    } else {
        Backend::Auto
    };
    if let Some((gpu, kernel)) = open_gpu(m, backend) {
        // First dispatch pays for pipeline/driver warm-up.
        let _ = gpu.run_batch(&kernel, &refs[..refs.len().min(64)]);
        let start = Instant::now();
        let out = gpu.run_batch(&kernel, &refs);
        rows.push(("gpu (wgsl)".into(), start.elapsed(), out));
    }

    let base = rows[0].1.as_secs_f64().max(1e-9);
    for (label, took, results) in &rows {
        report(
            label,
            n,
            mb,
            *took,
            results,
            base / took.as_secs_f64().max(1e-9),
        );
    }
    let reference = &rows[0].2;
    let mut differ = false;
    for (label, _, results) in &rows[1..] {
        let mismatches = reference
            .iter()
            .zip(results)
            .filter(|(a, b)| a.as_ref().ok() != b.as_ref().ok())
            .count();
        println!(
            "parity {label} vs {}: {mismatches} / {n} results differ",
            rows[0].0
        );
        differ |= mismatches > 0;
    }
    if differ {
        std::process::exit(1);
    }
}

fn report(
    label: &str,
    n: usize,
    mb: f64,
    took: Duration,
    results: &[enginewasm::Result<Vec<u8>>],
    speedup: f64,
) {
    let failed = results.iter().filter(|r| r.is_err()).count();
    let secs = took.as_secs_f64().max(1e-9);
    println!(
        "{label:<22} {n} tasks in {took:>9.2?} = {:>9.0} tasks/s {:>8.1} MB/s  {speedup:>6.2}x  ({failed} failed)",
        n as f64 / secs,
        mb / secs
    );
}

fn work(runner: &Runner<'_>, args: &Args) {
    let task_type = runner.m.task_type();
    let auth = Auth {
        uid: args.uid.clone(),
        challenge: args.challenge.clone(),
        admin_token: None,
    };
    let caps = Caps {
        runners: vec![RunnerKind::Cpu],
        max_inflight: 1,
    };
    let mut client = Client::connect(&args.server, auth, caps)
        .unwrap_or_else(|err| fail(&format!("connect {}: {err}", args.server)));
    println!("connected to {} as {}", args.server, args.uid);

    let (mut done, mut failed) = (0u64, 0u64);
    let mut report_at = Instant::now();
    let mut held: Option<Leased> = None;
    loop {
        let leased = match held.take() {
            Some(leased) => leased,
            None => match client.lease(task_type.clone(), args.lease, Duration::from_secs(1)) {
                Ok(leased) => leased,
                Err(err) if err.kind() == ErrorKind::NotFound && err.as_ref() == "QueueEmpty" => {
                    continue;
                }
                Err(err) => fail(&format!("lease: {err}")),
            },
        };
        let lease_ref = |task_id: &str, token: u64| LeaseRef {
            task_type: leased.task_type.clone(),
            task_id: task_id.to_owned(),
            token,
        };
        // Derived chunks need a mod-side "derive task from (base, idx)" ABI
        // that doesn't exist yet: hand them back.
        let (derived, plain): (Vec<_>, Vec<_>) =
            leased.tasks.iter().partition(|t| t.range.is_some());
        let payloads: Vec<&[u8]> = plain.iter().map(|t| t.bytes.as_slice()).collect();
        let outputs = runner.run(&payloads);

        let mut results = Vec::with_capacity(plain.len());
        let mut give_back: BTreeSet<(String, u64)> = BTreeSet::new();
        for (task, output) in plain.iter().zip(outputs) {
            match output {
                Ok(bytes) => results.push(TaskResult {
                    lease: lease_ref(&task.task_id, task.token),
                    bytes,
                }),
                Err(err) => {
                    failed += 1;
                    eprintln!("task {} failed: {err}", task.task_id);
                    give_back.insert((task.task_id.clone(), task.token));
                }
            }
        }
        give_back.extend(derived.iter().map(|t| (t.task_id.clone(), t.token)));
        // Only ask for the next lease when there is nothing to hand back
        // first, so a failing chunk is cancelled before it could be re-leased.
        let then_lease = give_back.is_empty().then(|| LeaseReq {
            task_type: task_type.clone(),
            max_tasks: args.lease,
        });
        if !results.is_empty() {
            match client.complete(results, then_lease) {
                Ok((acks, next)) => {
                    done += acks.iter().filter(|r| r.is_ok()).count() as u64;
                    failed += acks.iter().filter(|r| r.is_err()).count() as u64;
                    held = next;
                }
                Err(err) => fail(&format!("complete: {err}")),
            }
        }
        if !give_back.is_empty() {
            let leases = give_back
                .iter()
                .map(|(task_id, token)| lease_ref(task_id, *token))
                .collect();
            if let Err(err) = client.cancel_lease(leases) {
                eprintln!("cancel_lease: {err}");
            }
        }
        if report_at.elapsed() >= Duration::from_secs(5) {
            println!("completed {done}, failed {failed}");
            report_at = Instant::now();
        }
    }
}
