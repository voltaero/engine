use clap::{Args, CommandFactory, Subcommand, ValueEnum, ValueHint};
use clap::{Command, Parser};
use clap_complete::{Generator, Shell, generate};
use colored::*;
use enginelib::events::{Events, ID};
// For coloring the output
use enginelib::Registry;
use enginelib::api::postcard;
use enginelib::prelude::error;
use enginelib::task::{StoredTask, Task, TaskQueue};
use enginelib::{api::EngineAPI, config::Config, event::info};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::io::{self, BufReader, ErrorKind, Read};
use std::path::PathBuf;
use toml::Value;
use tonic::{
    Request,
    metadata::{MetadataKey, MetadataValue},
    transport::Endpoint,
};

pub mod proto {
    tonic::include_proto!("engine");
}

#[derive(Debug)]
struct Entry {
    namespace: String,
    id: String,
    data: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct RawDoc(
    std::collections::BTreeMap<String, Vec<std::collections::BTreeMap<String, toml::Value>>>,
);

fn parse_entries(raw: RawDoc) -> Vec<Entry> {
    let mut result = Vec::new();

    for (compound_key, records) in raw.0 {
        // split on colon: "widget:button" -> ("widget", "button")
        let mut parts = compound_key.splitn(2, ':');
        let namespace = parts.next().unwrap_or("").to_string();
        let id = parts.next().unwrap_or("").to_string();

        for data in records {
            result.push(Entry {
                namespace: namespace.clone(),
                id: id.clone(),
                data,
            });
        }
    }

    result
}

/// A simple CLI application
#[derive(Parser, Debug)]
#[command(name = "packer")]
#[command(version = "1.0")]
#[command(author = "GrandEngineering")]
#[command(about = "A simple CLI app to pack tasks")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    #[arg(long = "generate", value_enum)]
    generator: Option<Shell>,
}

#[derive(Subcommand, Debug, PartialEq)]
enum Commands {
    #[command()]
    Pack(PackArgs),
    #[command()]
    Unpack(PackArgs),
    #[command()]
    Upload(PackArgs),
    #[command()]
    AdminCheck,
    #[command()]
    AdminList(ListArgs),
    #[command()]
    AdminExport(ExportArgs),
    #[command()]
    AdminDelete(DeleteArgs),
    #[command()]
    Schema,
}

#[derive(Args, Debug, PartialEq)]
struct PackArgs {
    #[arg(short, required = true, value_hint = ValueHint::FilePath)]
    input: PathBuf,
    #[arg(long)]
    stream: bool,
}

#[derive(Clone, Debug, ValueEnum, PartialEq)]
enum StateArg {
    Queued,
    Processing,
    Solved,
}

impl StateArg {
    fn to_proto(&self) -> i32 {
        match self {
            StateArg::Queued => proto::TaskState::Queued as i32,
            StateArg::Processing => proto::TaskState::Processing as i32,
            StateArg::Solved => proto::TaskState::Solved as i32,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            StateArg::Queued => "queued",
            StateArg::Processing => "processing",
            StateArg::Solved => "solved",
        }
    }
}

#[derive(Args, Debug, PartialEq)]
struct ListArgs {
    #[arg(long)]
    task_id: Option<String>, // namespace:task
    #[arg(long, value_enum, default_value = "queued")]
    state: StateArg,
    #[arg(long)]
    all_states: bool,
    #[arg(long, default_value_t = 1000)]
    page_size: u32,
}

#[derive(Args, Debug, PartialEq)]
struct ExportArgs {
    #[arg(short = 'o', long, value_hint = ValueHint::FilePath, default_value = "output.rustforge.bin")]
    output: PathBuf,
    #[arg(long)]
    task_id: Option<String>, // namespace:task
    #[arg(long, value_enum, default_value = "queued")]
    state: StateArg,
    #[arg(long)]
    all_states: bool,
    #[arg(long, default_value_t = 1000)]
    page_size: u32,
}

#[derive(Args, Debug, PartialEq)]
struct DeleteArgs {
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    task: String,
    #[arg(long)]
    id: String,
    #[arg(long, value_enum)]
    state: StateArg,
}
fn print_completions<G: Generator>(generator: G, cmd: &mut Command) {
    generate(
        generator,
        cmd,
        cmd.get_name().to_string(),
        &mut io::stdout(),
    );
}

fn build_headers(api: &EngineAPI, admin: bool) -> HashMap<String, String> {
    let headers = std::sync::Arc::new(std::sync::RwLock::new(HashMap::<String, String>::new()));
    Events::ClientAuthPrepare(api, headers.clone());
    let mut prepared_headers = headers.read().map(|h| h.clone()).unwrap_or_default();

    if admin {
        if let Some(token) = api.cfg.config_toml.cgrpc_token.clone() {
            prepared_headers.insert("authorization".to_string(), token);
        }
    }

    prepared_headers
}

const CHUNKED_MAGIC: [u8; 4] = *b"RFPK";
const CHUNKED_VERSION: u8 = 2;
const FRAME_TASK: u8 = 1;
const FRAME_END: u8 = 255;

#[derive(Debug)]
struct ChunkedTaskRecord {
    namespace: String,
    task: String,
    id: String,
    payload: Vec<u8>,
}

fn write_chunked_header<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&CHUNKED_MAGIC)?;
    writer.write_all(&[CHUNKED_VERSION])?;
    Ok(())
}

fn write_chunked_task_record<W: Write>(
    writer: &mut W,
    record: &ChunkedTaskRecord,
) -> io::Result<()> {
    let ns = record.namespace.as_bytes();
    let task = record.task.as_bytes();
    let id = record.id.as_bytes();

    let ns_len: u16 = ns
        .len()
        .try_into()
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "namespace too large"))?;
    let task_len: u16 = task
        .len()
        .try_into()
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "task too large"))?;
    let id_len: u32 = id
        .len()
        .try_into()
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "id too large"))?;
    let payload_len: u64 = record
        .payload
        .len()
        .try_into()
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "payload too large"))?;

    let mut meta = [0u8; 1 + 2 + 2 + 4 + 8];
    meta[0] = FRAME_TASK;
    meta[1..3].copy_from_slice(&ns_len.to_le_bytes());
    meta[3..5].copy_from_slice(&task_len.to_le_bytes());
    meta[5..9].copy_from_slice(&id_len.to_le_bytes());
    meta[9..17].copy_from_slice(&payload_len.to_le_bytes());

    writer.write_all(&meta)?;
    writer.write_all(ns)?;
    writer.write_all(task)?;
    writer.write_all(id)?;
    writer.write_all(&record.payload)?;

    Ok(())
}

fn write_chunked_end<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&[FRAME_END])?;
    Ok(())
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

fn read_chunked_header<R: Read>(reader: &mut R) -> io::Result<()> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != CHUNKED_MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "invalid chunked magic",
        ));
    }

    let mut version = [0u8; 1];
    reader.read_exact(&mut version)?;
    if version[0] != CHUNKED_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("unsupported chunked version {}", version[0]),
        ));
    }

    Ok(())
}

fn read_chunked_next<R: Read>(reader: &mut R) -> io::Result<Option<ChunkedTaskRecord>> {
    let mut frame = [0u8; 1];
    if !read_exact_or_eof(reader, &mut frame)? {
        return Ok(None);
    }

    match frame[0] {
        FRAME_END => Ok(None),
        FRAME_TASK => {
            let mut ns_len = [0u8; 2];
            let mut task_len = [0u8; 2];
            let mut id_len = [0u8; 4];
            let mut payload_len = [0u8; 8];
            reader.read_exact(&mut ns_len)?;
            reader.read_exact(&mut task_len)?;
            reader.read_exact(&mut id_len)?;
            reader.read_exact(&mut payload_len)?;

            let ns_len = u16::from_le_bytes(ns_len) as usize;
            let task_len = u16::from_le_bytes(task_len) as usize;
            let id_len = u32::from_le_bytes(id_len) as usize;
            let payload_len_u64 = u64::from_le_bytes(payload_len);
            if payload_len_u64 > usize::MAX as u64 {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "payload length too large for this platform",
                ));
            }
            let payload_len = payload_len_u64 as usize;

            let mut ns = vec![0u8; ns_len];
            let mut task = vec![0u8; task_len];
            let mut id = vec![0u8; id_len];
            let mut payload = vec![0u8; payload_len];

            reader.read_exact(&mut ns)?;
            reader.read_exact(&mut task)?;
            reader.read_exact(&mut id)?;
            reader.read_exact(&mut payload)?;

            let namespace = String::from_utf8(ns)
                .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid namespace utf8"))?;
            let task = String::from_utf8(task)
                .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid task utf8"))?;
            let id = String::from_utf8(id)
                .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid id utf8"))?;

            Ok(Some(ChunkedTaskRecord {
                namespace,
                task,
                id,
                payload,
            }))
        }
        other => Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("unknown frame type {}", other),
        )),
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Some(generator) = cli.generator {
        let mut cmd = Cli::command();
        eprintln!("Generating completion file for {generator:?}...");
        print_completions(generator, &mut cmd);
    }
    let mut api = EngineAPI::default_client();
    EngineAPI::init_client(&mut api);
    // packer intentionally uses client init path; load config explicitly for host/admin token
    api.cfg = Config::new();
    for (id, tsk) in api.task_registry.tasks.iter() {
        api.task_queue.tasks.entry(id.clone()).or_default();
    }
    if let Some(command) = cli.command {
        match command {
            Commands::Schema => {
                let mut buf: Vec<String> = Vec::new();
                for tsk in api.task_registry.tasks {
                    let unw = tsk.1.to_toml();
                    buf.push(format![r#"[["{}:{}"]]"#, tsk.0.0, tsk.0.1]);
                    buf.push(unw);
                }
                let ns = buf.join("\n");
                match File::create("schema.rustforge.toml") {
                    Ok(mut file) => {
                        if let Err(e) = file.write_all(ns.as_bytes()) {
                            error!("Failed to write schema file: {}", e);
                        } else {
                            info!("Wrote schema.rustforge.toml");
                        }
                    }
                    Err(e) => {
                        error!("Failed to create schema file: {}", e);
                    }
                }
            }
            Commands::Unpack(input) => {
                if input.input.exists() {
                    info!("Unpacking File: {}", input.input.to_string_lossy());
                    let mut buf = Vec::new();

                    // Attempt to open and read the input file. If either step fails,
                    // we do not proceed to deserialization or writing the output file.
                    match File::open(&input.input) {
                        Ok(mut f) => {
                            if let Err(e) = f.read_to_end(&mut buf) {
                                error!(
                                    "Failed to read input file {}: {}",
                                    input.input.display(),
                                    e
                                );
                                // reading failed -> do not proceed to deserialize or write
                                return;
                            }
                        }
                        Err(e) => {
                            error!("Failed to open input file {}: {}", input.input.display(), e);
                            // opening failed -> do not proceed to deserialize or write
                            return;
                        }
                    }

                    // Try to deserialize. Only on successful deserialization do we
                    // process entries and write the output TOML file.
                    let maybe_queue: Option<TaskQueue> =
                        match postcard::from_bytes::<TaskQueue>(&buf) {
                            Ok(k) => Some(k),
                            Err(e) => {
                                error!("Failed to deserialize task queue: {}", e);
                                None
                            }
                        };

                    if let Some(k) = maybe_queue {
                        let mut final_out: Vec<String> = Vec::new();

                        for tasks in k.tasks {
                            match api.task_registry.tasks.get(&tasks.0.clone()) {
                                Some(tt) => {
                                    for task in tasks.1 {
                                        if tt.verify(task.bytes.clone()) {
                                            let tmp_nt = tt.from_bytes(&task.bytes);
                                            final_out.push(format![
                                                r#"[["{}:{}"]]"#,
                                                tasks.0.0.clone(),
                                                tasks.0.1.clone()
                                            ]);
                                            final_out.push(tmp_nt.to_toml());
                                            info!("{:?}", tmp_nt);
                                        }
                                    }
                                }
                                None => {
                                    error!("Unknown template for {}:{}", tasks.0.0, tasks.0.1);
                                }
                            }
                        }

                        let ns = final_out.join("\n");
                        match File::create("output.rustforge.toml") {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(ns.as_bytes()) {
                                    error!("Failed to write output.rustforge.toml: {}", e);
                                } else {
                                    info!("Wrote output.rustforge.toml");
                                }
                            }
                            Err(e) => {
                                error!("Failed to create output.rustforge.toml: {}", e);
                            }
                        }
                    } else {
                        // Deserialization failed; we logged the error above and intentionally do not
                        // create/write the output file to avoid producing an empty output.
                    }
                }
            }
            Commands::Upload(input) => {
                if !input.input.exists() {
                    error!("File does not exist: {}", input.input.to_string_lossy());
                    return;
                }

                info!(
                    "Uploading File: {}{}",
                    input.input.to_string_lossy(),
                    if input.stream { " (stream mode)" } else { "" }
                );

                let prepared_headers = build_headers(&api, false);

                let interceptor = move |mut req: Request<()>| {
                    for (key, value) in prepared_headers.iter() {
                        if let (Ok(key), Ok(value)) = (
                            MetadataKey::from_bytes(key.as_bytes()),
                            MetadataValue::try_from(value.as_str()),
                        ) {
                            req.metadata_mut().insert(key, value);
                        }
                    }
                    Ok(req)
                };

                let endpoint = format!("http://{}", api.cfg.config_toml.host);
                let channel = match Endpoint::from_shared(endpoint.clone()) {
                    Ok(ep) => match ep.connect().await {
                        Ok(ch) => ch,
                        Err(e) => {
                            error!("Failed to connect to server {}: {}", endpoint, e);
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Invalid server endpoint {}: {}", endpoint, e);
                        return;
                    }
                };

                let mut client =
                    proto::engine_client::EngineClient::with_interceptor(channel, interceptor);

                let mut uploaded = 0usize;
                let mut failed = 0usize;

                if input.stream {
                    let file = match File::open(&input.input) {
                        Ok(f) => f,
                        Err(e) => {
                            error!("Failed to open input file {}: {}", input.input.display(), e);
                            return;
                        }
                    };
                    let mut reader = BufReader::new(file);
                    if let Err(e) = read_chunked_header(&mut reader) {
                        error!("Failed to read chunked stream header: {}", e);
                        return;
                    }

                    loop {
                        let record = match read_chunked_next(&mut reader) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to read chunked stream record: {}", e);
                                return;
                            }
                        };

                        let Some(record) = record else {
                            break;
                        };

                        let task_id = format!("{}:{}", record.namespace, record.task);
                        let req = proto::Task {
                            id: record.id,
                            task_id: task_id.clone(),
                            task_payload: record.payload,
                            payload: Vec::new(),
                        };

                        match client.create_task(Request::new(req)).await {
                            Ok(_) => uploaded += 1,
                            Err(e) => {
                                failed += 1;
                                error!("Failed to upload task {}: {}", task_id, e);
                            }
                        }
                    }
                } else {
                    let mut buf = Vec::new();
                    match File::open(&input.input) {
                        Ok(mut f) => {
                            if let Err(e) = f.read_to_end(&mut buf) {
                                error!(
                                    "Failed to read input file {}: {}",
                                    input.input.display(),
                                    e
                                );
                                return;
                            }
                        }
                        Err(e) => {
                            error!("Failed to open input file {}: {}", input.input.display(), e);
                            return;
                        }
                    }

                    let queue: TaskQueue = match postcard::from_bytes::<TaskQueue>(&buf) {
                        Ok(q) => q,
                        Err(e) => {
                            error!("Failed to deserialize task queue: {}", e);
                            return;
                        }
                    };

                    for ((namespace, task), tasks) in queue.tasks {
                        let task_id = format!("{}:{}", namespace, task);
                        for stored in tasks {
                            let req = proto::Task {
                                id: stored.id,
                                task_id: task_id.clone(),
                                task_payload: stored.bytes,
                                payload: Vec::new(),
                            };
                            match client.create_task(Request::new(req)).await {
                                Ok(_) => uploaded += 1,
                                Err(e) => {
                                    failed += 1;
                                    error!("Failed to upload task {}: {}", task_id, e);
                                }
                            }
                        }
                    }
                }

                info!(
                    "Upload complete. uploaded={}, failed={}, mode={}",
                    uploaded,
                    failed,
                    if input.stream { "stream" } else { "legacy" }
                );
            }
            Commands::AdminCheck => {
                let prepared_headers = build_headers(&api, true);
                let interceptor = move |mut req: Request<()>| {
                    for (key, value) in prepared_headers.iter() {
                        if let (Ok(key), Ok(value)) = (
                            MetadataKey::from_bytes(key.as_bytes()),
                            MetadataValue::try_from(value.as_str()),
                        ) {
                            req.metadata_mut().insert(key, value);
                        }
                    }
                    Ok(req)
                };

                let endpoint = format!("http://{}", api.cfg.config_toml.host);
                let channel = match Endpoint::from_shared(endpoint.clone()) {
                    Ok(ep) => match ep.connect().await {
                        Ok(ch) => ch,
                        Err(e) => {
                            error!("Failed to connect to server {}: {}", endpoint, e);
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Invalid server endpoint {}: {}", endpoint, e);
                        return;
                    }
                };

                let mut client =
                    proto::engine_client::EngineClient::with_interceptor(channel, interceptor);
                match client.check_auth(Request::new(proto::Empty {})).await {
                    Ok(_) => info!("Admin auth check: OK"),
                    Err(e) => error!("Admin auth check failed: {}", e),
                }
            }
            Commands::AdminList(args) => {
                let prepared_headers = build_headers(&api, true);
                let interceptor = move |mut req: Request<()>| {
                    for (key, value) in prepared_headers.iter() {
                        if let (Ok(key), Ok(value)) = (
                            MetadataKey::from_bytes(key.as_bytes()),
                            MetadataValue::try_from(value.as_str()),
                        ) {
                            req.metadata_mut().insert(key, value);
                        }
                    }
                    Ok(req)
                };

                let endpoint = format!("http://{}", api.cfg.config_toml.host);
                let channel = match Endpoint::from_shared(endpoint.clone()) {
                    Ok(ep) => match ep.connect().await {
                        Ok(ch) => ch,
                        Err(e) => {
                            error!("Failed to connect to server {}: {}", endpoint, e);
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Invalid server endpoint {}: {}", endpoint, e);
                        return;
                    }
                };

                let mut client =
                    proto::engine_client::EngineClient::with_interceptor(channel, interceptor);

                let task_ids: Vec<String> = if let Some(task_id) = args.task_id.clone() {
                    vec![task_id]
                } else {
                    match client.aquire_task_reg(Request::new(proto::Empty {})).await {
                        Ok(res) => res.into_inner().tasks,
                        Err(e) => {
                            error!("Failed to fetch task registry: {}", e);
                            return;
                        }
                    }
                };

                let states: Vec<StateArg> = if args.all_states {
                    vec![StateArg::Queued, StateArg::Processing, StateArg::Solved]
                } else {
                    vec![args.state.clone()]
                };

                let mut listed = 0usize;
                println!("state\ttask\tid");
                for task_id in task_ids {
                    let Some((namespace, task)) = task_id.split_once(':') else {
                        error!("Invalid task id '{}' (expected namespace:task)", task_id);
                        continue;
                    };

                    for state in &states {
                        let mut page = 0u64;
                        loop {
                            let req = proto::TaskPageRequest {
                                namespace: namespace.to_string(),
                                task: task.to_string(),
                                page,
                                page_size: args.page_size,
                                state: state.to_proto(),
                            };

                            let resp = match client.get_tasks(Request::new(req)).await {
                                Ok(r) => r.into_inner(),
                                Err(e) => {
                                    error!(
                                        "GetTasks failed for {} state {:?}: {}",
                                        task_id, state, e
                                    );
                                    break;
                                }
                            };

                            if resp.tasks.is_empty() {
                                break;
                            }

                            for t in resp.tasks {
                                println!("{}\t{}:{}\t{}", state.as_str(), namespace, task, t.id);
                                listed += 1;
                            }

                            page += 1;
                        }
                    }
                }

                info!("Listed {} task(s)", listed);
            }
            Commands::AdminExport(args) => {
                let prepared_headers = build_headers(&api, true);
                let interceptor = move |mut req: Request<()>| {
                    for (key, value) in prepared_headers.iter() {
                        if let (Ok(key), Ok(value)) = (
                            MetadataKey::from_bytes(key.as_bytes()),
                            MetadataValue::try_from(value.as_str()),
                        ) {
                            req.metadata_mut().insert(key, value);
                        }
                    }
                    Ok(req)
                };

                let endpoint = format!("http://{}", api.cfg.config_toml.host);
                let channel = match Endpoint::from_shared(endpoint.clone()) {
                    Ok(ep) => match ep.connect().await {
                        Ok(ch) => ch,
                        Err(e) => {
                            error!("Failed to connect to server {}: {}", endpoint, e);
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Invalid server endpoint {}: {}", endpoint, e);
                        return;
                    }
                };

                let mut client =
                    proto::engine_client::EngineClient::with_interceptor(channel, interceptor);

                let task_ids: Vec<String> = if let Some(task_id) = args.task_id.clone() {
                    vec![task_id]
                } else {
                    match client.aquire_task_reg(Request::new(proto::Empty {})).await {
                        Ok(res) => res.into_inner().tasks,
                        Err(e) => {
                            error!("Failed to fetch task registry: {}", e);
                            return;
                        }
                    }
                };

                let states: Vec<StateArg> = if args.all_states {
                    vec![StateArg::Queued, StateArg::Processing, StateArg::Solved]
                } else {
                    vec![args.state.clone()]
                };

                let mut out_queue = TaskQueue::default();
                let mut fetched = 0usize;

                for task_id in task_ids {
                    let Some((namespace, task)) = task_id.split_once(':') else {
                        error!("Invalid task id '{}' (expected namespace:task)", task_id);
                        continue;
                    };

                    for state in &states {
                        let mut page = 0u64;
                        loop {
                            let req = proto::TaskPageRequest {
                                namespace: namespace.to_string(),
                                task: task.to_string(),
                                page,
                                page_size: args.page_size,
                                state: state.to_proto(),
                            };

                            let resp = match client.get_tasks(Request::new(req)).await {
                                Ok(r) => r.into_inner(),
                                Err(e) => {
                                    error!(
                                        "GetTasks failed for {} state {:?}: {}",
                                        task_id, state, e
                                    );
                                    break;
                                }
                            };

                            if resp.tasks.is_empty() {
                                break;
                            }

                            let key = ID(namespace, task);
                            let bucket = out_queue.tasks.entry(key).or_default();
                            for t in resp.tasks {
                                bucket.push(StoredTask {
                                    bytes: t.task_payload,
                                    id: t.id,
                                });
                                fetched += 1;
                            }

                            page += 1;
                        }
                    }
                }

                match postcard::to_allocvec(&out_queue) {
                    Ok(data) => match File::create(&args.output) {
                        Ok(mut file) => {
                            if let Err(e) = file.write_all(&data) {
                                error!("Failed to write {}: {}", args.output.display(), e);
                            } else {
                                info!(
                                    "Export complete. wrote {} task(s) to {}",
                                    fetched,
                                    args.output.display()
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to create {}: {}", args.output.display(), e);
                        }
                    },
                    Err(e) => error!("Failed to serialize export: {}", e),
                }
            }
            Commands::AdminDelete(args) => {
                let prepared_headers = build_headers(&api, true);
                let interceptor = move |mut req: Request<()>| {
                    for (key, value) in prepared_headers.iter() {
                        if let (Ok(key), Ok(value)) = (
                            MetadataKey::from_bytes(key.as_bytes()),
                            MetadataValue::try_from(value.as_str()),
                        ) {
                            req.metadata_mut().insert(key, value);
                        }
                    }
                    Ok(req)
                };

                let endpoint = format!("http://{}", api.cfg.config_toml.host);
                let channel = match Endpoint::from_shared(endpoint.clone()) {
                    Ok(ep) => match ep.connect().await {
                        Ok(ch) => ch,
                        Err(e) => {
                            error!("Failed to connect to server {}: {}", endpoint, e);
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Invalid server endpoint {}: {}", endpoint, e);
                        return;
                    }
                };

                let mut client =
                    proto::engine_client::EngineClient::with_interceptor(channel, interceptor);

                let req = proto::TaskSelector {
                    state: args.state.to_proto(),
                    namespace: args.namespace.clone(),
                    task: args.task.clone(),
                    id: args.id.clone(),
                };

                match client.delete_task(Request::new(req)).await {
                    Ok(_) => info!(
                        "Deleted task {} from {}:{} ({:?})",
                        args.id, args.namespace, args.task, args.state
                    ),
                    Err(e) => error!("DeleteTask failed: {}", e),
                }
            }
            Commands::Pack(input) => {
                if input.input.exists() {
                    info!("Packing File: {}", input.input.to_string_lossy());
                    match std::fs::read_to_string(&input.input) {
                        Ok(toml_str) => {
                            match toml::from_str::<RawDoc>(&toml_str) {
                                Ok(raw) => {
                                    let entries = parse_entries(raw);
                                    for entry in entries {
                                        match api
                                            .task_registry
                                            .get(&ID(entry.namespace.as_str(), entry.id.as_str()))
                                        {
                                            Some(template) => {
                                                match toml::to_string(&entry.data) {
                                                    Ok(toml_string) => {
                                                        let t = template.from_toml(toml_string);
                                                        let key = ID(
                                                            entry.namespace.as_str(),
                                                            entry.id.as_str(),
                                                        );
                                                        let mut vec = api
                                                            .task_queue
                                                            .tasks
                                                            .get(&key)
                                                            .cloned()
                                                            .unwrap_or_default();
                                                        vec.push(StoredTask {
                                                            id: "".into(), //ids are minted on the server
                                                            bytes: t.to_bytes(),
                                                        });
                                                        api.task_queue.tasks.insert(key, vec);
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to convert entry data to TOML string: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                            None => {
                                                error!(
                                                    "Template not found for {}:{}",
                                                    entry.namespace, entry.id
                                                );
                                            }
                                        }
                                    }
                                    if input.stream {
                                        match File::create("output.rustforge.bin") {
                                            Ok(mut file) => {
                                                if let Err(e) = write_chunked_header(&mut file) {
                                                    error!(
                                                        "Failed to write chunked output header: {}",
                                                        e
                                                    );
                                                    return;
                                                }

                                                let mut wrote = 0usize;
                                                for ((namespace, task), tasks) in
                                                    &api.task_queue.tasks
                                                {
                                                    for stored in tasks {
                                                        let record = ChunkedTaskRecord {
                                                            namespace: namespace.clone(),
                                                            task: task.clone(),
                                                            id: stored.id.clone(),
                                                            payload: stored.bytes.clone(),
                                                        };
                                                        if let Err(e) = write_chunked_task_record(
                                                            &mut file, &record,
                                                        ) {
                                                            error!(
                                                                "Failed to write chunked output record: {}",
                                                                e
                                                            );
                                                            return;
                                                        }
                                                        wrote += 1;
                                                    }
                                                }

                                                if let Err(e) = write_chunked_end(&mut file) {
                                                    error!(
                                                        "Failed to finalize chunked output: {}",
                                                        e
                                                    );
                                                    return;
                                                }

                                                info!(
                                                    "Wrote output.rustforge.bin (chunked stream, {} task(s))",
                                                    wrote
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Failed to create output.rustforge.bin: {}",
                                                    e
                                                );
                                            }
                                        }
                                    } else {
                                        match postcard::to_allocvec(&api.task_queue) {
                                            Ok(data) => {
                                                match File::create("output.rustforge.bin") {
                                                    Ok(mut file) => {
                                                        if let Err(e) = file.write_all(&data) {
                                                            error!(
                                                                "Failed to write output.rustforge.bin: {}",
                                                                e
                                                            );
                                                        } else {
                                                            info!("Wrote output.rustforge.bin");
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to create output.rustforge.bin: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to serialize task queue: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse input TOML: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to read input file {}: {}", input.input.display(), e);
                        }
                    }
                } else {
                    error!("File does not exist: {}", input.input.to_string_lossy())
                }
            }
        }
    }
}
