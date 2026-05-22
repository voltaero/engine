use std::{fs, io::Error, u32};

use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

fn default_host() -> String {
    "[::1]:50051".into()
}

fn default_clean_tasks() -> u64 {
    60
}
fn default_task_block_size() -> u32 {
    256
}
fn default_pagination_limit() -> u32 {
    u32::MAX
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigTomlServer {
    #[serde(default)]
    pub cgrpc_token: Option<String>, // Administrator Token, used to invoke cgrpc reqs. If not preset will default to no protection.
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_clean_tasks")]
    pub clean_tasks: u64,
    #[serde(default = "default_pagination_limit")]
    pub pagination_limit: u32,
    #[serde(default = "default_task_block_size")]
    pub task_block_size: u32,
}
impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            cgrpc_token: None,
            host: default_host(),
            clean_tasks: default_clean_tasks(),
            pagination_limit: default_pagination_limit(),
            task_block_size: default_task_block_size(),
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub config_toml: ConfigTomlServer,
}

impl Config {
    #[allow(clippy::new_without_default)]
    #[instrument]
    pub fn new() -> Self {
        let mut content: String = "".to_owned();
        let result: Result<String, Error> = fs::read_to_string("config.toml");
        if result.is_ok() {
            content = result.unwrap();
        };
        let config_toml: ConfigTomlServer = toml::from_str(&content).unwrap_or_else(|err| {
            error!("Failed to parse config file.");
            error!("{:#?}", err);
            ConfigTomlServer::default()
        });
        Self { config_toml }
    }
}
