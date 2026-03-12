use std::fs;

use serde::Deserialize;

fn default_listen() -> String {
    "0.0.0.0:50052".into()
}

fn default_node_ttl_seconds() -> u64 {
    15
}

fn default_heartbeat_interval_seconds() -> u64 {
    5
}

fn default_max_acquire_hops() -> u32 {
    3
}

fn default_admin_fanout_limit() -> u32 {
    5000
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RouteRuleToml {
    pub r#match: String,
    #[serde(default)]
    pub require_tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfigToml {
    #[serde(default = "default_listen")]
    pub listen: String,
    pub cluster_token: String,
    #[serde(default = "default_node_ttl_seconds")]
    pub node_ttl_seconds: u64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_max_acquire_hops")]
    pub max_acquire_hops: u32,
    #[serde(default = "default_admin_fanout_limit")]
    pub admin_fanout_limit: u32,
    #[serde(default)]
    pub rules: Vec<RouteRuleToml>,
}

impl Default for ProxyConfigToml {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            cluster_token: String::new(),
            node_ttl_seconds: default_node_ttl_seconds(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
            max_acquire_hops: default_max_acquire_hops(),
            admin_fanout_limit: default_admin_fanout_limit(),
            rules: Vec::new(),
        }
    }
}

impl ProxyConfigToml {
    pub fn load() -> Result<Self, String> {
        let content = match fs::read_to_string("proxy.toml") {
            Ok(content) => content,
            Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
            Err(err) => return Err(format!("Failed to read proxy.toml: {err}")),
        };

        let config = if content.trim().is_empty() {
            Self::default()
        } else {
            toml::from_str(&content).map_err(|err| format!("Failed to parse proxy.toml: {err}"))?
        };

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_token.trim().is_empty() {
            return Err("proxy cluster_token must not be empty".into());
        }
        if self.node_ttl_seconds == 0 {
            return Err("proxy node_ttl_seconds must be greater than zero".into());
        }
        if self.heartbeat_interval_seconds == 0 {
            return Err("proxy heartbeat_interval_seconds must be greater than zero".into());
        }
        if self.max_acquire_hops == 0 {
            return Err("proxy max_acquire_hops must be greater than zero".into());
        }
        if self.admin_fanout_limit == 0 {
            return Err("proxy admin_fanout_limit must be greater than zero".into());
        }
        Ok(())
    }
}

use std::io::ErrorKind;
