//! Worker client: loads the mods in `./mods`, connects to a server (directly or
//! through the proxy) and leases, runs and completes tasks of every type it has
//! a mod for. Each connection has one updater thread doing all the I/O and
//! `threads` compute threads (see `enginelib::worker`).
//!
//! Takes no arguments: everything comes from the `[client]` table of
//! `config.toml` (`enginelib::config::ClientConfig`):
//!
//! ```toml
//! [client]
//! server = "tcp://[::1]:50051"   # default: `advertise`, else `host`
//! # proxy = "tcp://[::1]:50050"  # resolve the server through a proxy instead
//! uid = "worker-1"               # default: worker-<pid>
//! challenge = ""
//! threads = 8                    # compute threads per connection; default: CPU count
//! conns = 1
//! task_types = ["ns:name"]       # default: every type the mods provide
//! max_tasks = 1024               # per lease
//! hip = false                    # prefer the HIP runner
//! ```

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

fn fail(message: &str) -> ! {
    error!("client: {message}");
    std::process::exit(2);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let api = Arc::new(ServerAPI::client(Config::new()));
    let toml = &api.cfg.config_toml;
    let cfg = toml.client.clone();

    let mut opts = WorkerOptions {
        max_tasks: cfg.max_tasks,
        runner: cfg.hip.then_some(Runner::HIP),
        ..Default::default()
    };
    if let Some(threads) = cfg.threads {
        opts.threads = threads;
    }
    opts.task_types = cfg
        .task_types
        .iter()
        .map(|name| match name.split_once(':') {
            Some((ns, name)) => (ns.to_string(), name.to_string()),
            None => fail(&format!(
                "bad task type {name:?} in [client], expected \"ns:name\""
            )),
        })
        .collect::<Vec<Identifier>>();
    if opts.threads == 0 || cfg.conns == 0 || opts.max_tasks == 0 {
        fail("[client] threads, conns and max_tasks must be at least 1");
    }
    let server = cfg
        .server
        .clone()
        .or_else(|| toml.advertise.clone())
        .unwrap_or_else(|| toml.host.clone());

    Events::ClientStart(&api);
    let auth = worker::auth(
        &api,
        Auth {
            uid: cfg
                .uid
                .clone()
                .unwrap_or_else(|| format!("worker-{}", std::process::id())),
            challenge: cfg.challenge.clone(),
            admin_token: None,
        },
    );
    let caps = worker::caps(opts.runner);
    match &cfg.proxy {
        Some(proxy) => info!("Client: {} connection(s) via proxy {proxy}", cfg.conns),
        None => info!("Client: {} connection(s) to {server}", cfg.conns),
    }

    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut threads = Vec::with_capacity(cfg.conns);
    for id in 0..cfg.conns {
        let (api, stop, done_tx) = (api.clone(), stop.clone(), done_tx.clone());
        let (server, proxy, opts) = (server.clone(), cfg.proxy.clone(), opts.clone());
        let (auth, caps) = (auth.clone(), caps.clone());
        let thread = std::thread::Builder::new()
            .name(format!("updater-{id}"))
            .spawn(move || {
                // `home` is the server holding this worker's leases: go back
                // there, even when a proxy would pick another, with the
                // credentials that server accepted (a proxy ticket replaces
                // `challenge`).
                let mut accepted: Option<(String, Auth)> = None;
                let connect = move |home: Option<&str>| {
                    let client = match (home, &proxy) {
                        (Some(home), _) => {
                            let home_auth = match &accepted {
                                Some((endpoint, accepted)) if endpoint == home => accepted,
                                _ => &auth,
                            };
                            Client::connect(home, home_auth.clone(), caps.clone())
                        }
                        (None, Some(proxy)) => {
                            Client::connect_via_proxy(proxy, None, auth.clone(), caps.clone())
                        }
                        (None, None) => Client::connect(&server, auth.clone(), caps.clone()),
                    }?;
                    accepted = Some((client.endpoint().to_string(), client.auth().clone()));
                    Ok(client)
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
