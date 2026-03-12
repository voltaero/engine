use std::{fmt, fs, io::Error, u32};

use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

fn default_host() -> String {
    "[::1]:50051".into()
}

fn default_clean_tasks() -> u64 {
    60
}

fn default_pagination_limit() -> u32 {
    u32::MAX
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ConfigTomlServer {
    #[serde(default)]
    pub cgrpc_token: Option<String>, // Administrator Token, used to invoke cgrpc reqs. If not preset will default to no protection.
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_clean_tasks")]
    pub clean_tasks: u64,
    #[serde(default = "default_pagination_limit")]
    pub pagination_limit: u32,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub advertise_addr: Option<String>,
    #[serde(default)]
    pub cluster_proxy_addr: Option<String>,
    #[serde(default)]
    pub cluster_token: Option<String>,
    #[serde(default)]
    pub node_tags: Option<Vec<String>>,
}

impl fmt::Debug for ConfigTomlServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigTomlServer")
            .field(
                "cgrpc_token",
                &self.cgrpc_token.as_ref().map(|_| "<redacted>"),
            )
            .field("host", &self.host)
            .field("clean_tasks", &self.clean_tasks)
            .field("pagination_limit", &self.pagination_limit)
            .field("node_id", &self.node_id)
            .field("advertise_addr", &self.advertise_addr)
            .field("cluster_proxy_addr", &self.cluster_proxy_addr)
            .field(
                "cluster_token",
                &self.cluster_token.as_ref().map(|_| "<redacted>"),
            )
            .field("node_tags", &self.node_tags)
            .finish()
    }
}

impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            host: "[::1]:50051".into(),
            cgrpc_token: None,
            clean_tasks: 60,
            pagination_limit: u32::MAX,
            node_id: None,
            advertise_addr: None,
            cluster_proxy_addr: None,
            cluster_token: None,
            node_tags: None,
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
