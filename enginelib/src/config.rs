use std::{fs, io::Error};

use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

fn default_host() -> String {
    "[::1]:50051".into()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigTomlServer {
    #[serde(default = "default_host")]
    pub host: String,

    // Renamed from cgrpc_token; keep the alias so existing configs still authenticate.
    #[serde(alias = "cgrpc_token")]
    pub auth_token: Option<String>,
}
impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            host: default_host(),
            auth_token: Option::None,
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
        if let Ok(file_content) = result {
            content = file_content;
        }
        let config_toml: ConfigTomlServer = toml::from_str(&content).unwrap_or_else(|err| {
            error!("Failed to parse config file.");
            error!("{:#?}", err);
            ConfigTomlServer::default()
        });
        Self { config_toml }
    }
}
