//! Thin CLI over [`engine::client::Client`] for driving the engine by hand.
//!
//! Usage:
//!   client submit   <ns> <name> <payload>          submit one task
//!   client lease    <ns> <name> [max]              lease up to max (default 1)
//!   client complete <ns> <name> <task_id> <result> complete one leased task
//!   client cancel   <ns> <name> <task_id>          cancel (requeue) a lease
//!
//! Endpoint and auth token come from config.toml (same as the server).

use engine::client::Client;
use enginelib::config::Config;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: client <submit|lease|complete|cancel> ...");
        std::process::exit(2);
    }

    let cfg = Config::new();
    let endpoint = format!("tcp://{}", cfg.config_toml.host);
    let auth = cfg.config_toml.auth_token.clone().unwrap_or_default();

    let mut client = match Client::connect(&endpoint, auth) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect failed: {e}");
            std::process::exit(1);
        }
    };

    let result = match args[0].as_str() {
        "submit" if args.len() >= 4 => {
            let task_type = (args[1].clone(), args[2].clone());
            let payload = args[3].clone().into_bytes();
            client
                .submit(task_type, vec![payload])
                .await
                .map(|ids| format!("submitted: {}", ids.join(", ")))
        }
        "lease" if args.len() >= 3 => {
            let task_type = (args[1].clone(), args[2].clone());
            let max: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
            client
                .lease(task_type, "cli".to_string(), max)
                .await
                .map(|tasks| {
                    let lines: Vec<String> = tasks
                        .iter()
                        .map(|t| format!("  {} -> {} bytes", t.task_id, t.bytes.len()))
                        .collect();
                    format!("leased {}:\n{}", tasks.len(), lines.join("\n"))
                })
        }
        "complete" if args.len() >= 5 => {
            let task_type = (args[1].clone(), args[2].clone());
            let results = vec![(args[3].clone(), args[4].clone().into_bytes())];
            client
                .complete(task_type, results)
                .await
                .map(|ok| format!("completed: {ok}"))
        }
        "cancel" if args.len() >= 4 => {
            let task_type = (args[1].clone(), args[2].clone());
            client
                .cancel(task_type, args[3].clone())
                .await
                .map(|()| "cancelled".to_string())
        }
        other => {
            eprintln!("unknown or malformed command: {other}");
            std::process::exit(2);
        }
    };

    match result {
        Ok(msg) => println!("{msg}"),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
