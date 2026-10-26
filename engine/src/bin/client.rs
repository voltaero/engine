//! Worker client: loads the mods in `./mods`, connects to a server (directly or
//! through the proxy) and leases, runs and completes tasks of every type it has
//! a mod for. Each connection (`--conns`, default 1) has one updater thread
//! doing all the I/O and `--threads` compute threads (see `enginelib::worker`).
//!
//! ```text
//! cargo run -p engine --bin client -- --server tcp://[::1]:50051 --threads 8
//! ```
//!
//! Flags (all optional): `--server` (default `advertise`, else `host` from
//! `config.toml`), `--proxy` (resolve the server through a proxy instead),
//! `--uid`, `--challenge` (or `ENGINE_CHALLENGE`), `--threads` (compute threads
//! per connection; default: CPU count), `--conns`, `--types ns:name,...`
//! (default: every type the mods provide), `--max-tasks` (per lease, default
//! 1024), `--hip` (prefer the HIP runner).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use enginelib::Identifier;
use enginelib::api::ServerAPI;
use enginelib::config::Config;
use enginelib::events::Events;
use enginelib::task::Runner;
use enginelib::transport::client::Client;
use enginelib::wire::Auth;
use enginelib::worker::{self, WorkerOptions};
use tracing::{error, info};

struct Args {
    server: Option<String>,
    proxy: Option<String>,
    uid: String,
    challenge: String,
    conns: usize,
    opts: WorkerOptions,
}

fn parse_args() -> Args {
    let mut args = Args {
        server: None,
        proxy: None,
        uid: format!("worker-{}", std::process::id()),
        challenge: std::env::var("ENGINE_CHALLENGE").unwrap_or_default(),
        conns: 1,
        opts: WorkerOptions::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--hip" {
            args.opts.runner = Some(Runner::HIP);
            continue;
        }
        let value = it
            .next()
            .unwrap_or_else(|| usage(&format!("missing value for {flag}")));
        let num = || -> u64 {
            value
                .parse()
                .unwrap_or_else(|_| usage(&format!("bad {flag}: {value}")))
        };
        match flag.as_str() {
            "--server" => args.server = Some(value),
            "--proxy" => args.proxy = Some(value),
            "--uid" => args.uid = value,
            "--challenge" => args.challenge = value,
            "--threads" => args.opts.threads = num() as usize,
            "--conns" => args.conns = num() as usize,
            "--max-tasks" => args.opts.max_tasks = num().min(u32::MAX as u64) as u32,
            "--types" => {
                args.opts.task_types = value
                    .split(',')
                    .map(|name| match name.split_once(':') {
                        Some((ns, name)) => (ns.to_string(), name.to_string()),
                        None => usage(&format!("bad task type {name}, expected ns:name")),
                    })
                    .collect::<Vec<Identifier>>();
            }
            other => usage(&format!("unknown flag {other}")),
        }
    }
    if args.opts.threads == 0 || args.conns == 0 || args.opts.max_tasks == 0 {
        usage("--threads, --conns and --max-tasks must be at least 1");
    }
    args
}

fn usage(message: &str) -> ! {
    eprintln!("client: {message}");
    std::process::exit(2);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = parse_args();

    let api = Arc::new(ServerAPI::client(Config::new()));
    Events::ClientStart(&api);

    let server = args.server.clone().unwrap_or_else(|| {
        let toml = &api.cfg.config_toml;
        toml.advertise.clone().unwrap_or_else(|| toml.host.clone())
    });
    let auth = worker::auth(
        &api,
        Auth {
            uid: args.uid.clone(),
            challenge: args.challenge.clone(),
            admin_token: None,
        },
    );
    let caps = worker::caps(args.opts.runner);
    match &args.proxy {
        Some(proxy) => info!("Client: {} connection(s) via proxy {proxy}", args.conns),
        None => info!("Client: {} connection(s) to {server}", args.conns),
    }

    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut threads = Vec::with_capacity(args.conns);
    for id in 0..args.conns {
        let (api, stop, done_tx) = (api.clone(), stop.clone(), done_tx.clone());
        let (server, proxy, opts) = (server.clone(), args.proxy.clone(), args.opts.clone());
        let (auth, caps) = (auth.clone(), caps.clone());
        let thread = std::thread::Builder::new()
            .name(format!("updater-{id}"))
            .spawn(move || {
                let connect = || match &proxy {
                    Some(proxy) => {
                        Client::connect_via_proxy(proxy, None, auth.clone(), caps.clone())
                    }
                    None => Client::connect(&server, auth.clone(), caps.clone()),
                };
                let result = worker::run(&api, connect, &opts, &stop);
                let _ = done_tx.send(result);
            })
            .expect("Failed to spawn worker thread");
        threads.push(thread);
    }
    drop(done_tx);

    let mut code = 0;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("Shutting down"),
        result = done_rx.recv() => {
            if let Some(Err(err)) = result {
                error!("Worker stopped: {err}");
                code = 1;
            }
        }
    }
    stop.store(true, Ordering::SeqCst);
    // Workers hand back unstarted tasks and complete the finished ones first.
    let _ = tokio::task::spawn_blocking(move || {
        for thread in threads {
            let _ = thread.join();
        }
    })
    .await;
    std::process::exit(code);
}
