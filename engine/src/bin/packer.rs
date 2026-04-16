use clap::{Args, CommandFactory, Subcommand, ValueHint};
use clap::{Command, Parser};
use clap_complete::{Generator, Shell, generate};
use colored::*;
use enginelib::events::ID;
// For coloring the output
use enginelib::Registry;
use enginelib::api::postcard;
use enginelib::prelude::error;
use enginelib::task::{StoredTask, Task, TaskQueue};
use enginelib::{api::EngineAPI, event::info};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::io::{self, Read};
use std::path::PathBuf;
use toml::Value;

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
    Schema,
}
#[derive(Args, Debug, PartialEq)]
struct PackArgs {
    #[arg(short,required=true,value_hint=ValueHint::FilePath)]
    input: PathBuf,
}
fn print_completions<G: Generator>(generator: G, cmd: &mut Command) {
    generate(
        generator,
        cmd,
        cmd.get_name().to_string(),
        &mut io::stdout(),
    );
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
    EngineAPI::init_packer(&mut api);
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
                                    match postcard::to_allocvec(&api.task_queue) {
                                        Ok(data) => match File::create("output.rustforge.bin") {
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
                                        },
                                        Err(e) => {
                                            error!("Failed to serialize task queue: {}", e);
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
