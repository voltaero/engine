use std::{fs, io::ErrorKind};

use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

fn default_host() -> String {
    "[::1]:50051".into()
}

/// Resource limits for one transport shard.
///
/// Defaults are deliberately finite. They are starting points that should be
/// tuned with the process benchmark for the deployment's payload distribution.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct TransportConfig {
    pub max_active_requests: usize,
    pub max_active_long_polls: usize,
    pub max_active_mutations: usize,
    pub max_active_queries: usize,
    pub reply_queue_count: usize,
    pub reply_queue_bytes: usize,
    pub max_wire_bytes: usize,
    pub max_reply_bytes: usize,
    pub max_batch_items: usize,
    pub max_batch_bytes: usize,
    pub lease_long_poll_ms: u64,
    pub zmq_sndhwm: i32,
    pub zmq_rcvhwm: i32,
    pub zmq_linger_ms: i32,
    pub shutdown_timeout_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_active_requests: 1024,
            max_active_long_polls: 512,
            max_active_mutations: 384,
            max_active_queries: 128,
            reply_queue_count: 1024,
            reply_queue_bytes: 64 * 1024 * 1024,
            max_wire_bytes: 8 * 1024 * 1024,
            max_reply_bytes: 8 * 1024 * 1024,
            max_batch_items: 4096,
            max_batch_bytes: 4 * 1024 * 1024,
            lease_long_poll_ms: 30_000,
            zmq_sndhwm: 1024,
            zmq_rcvhwm: 1024,
            zmq_linger_ms: 1000,
            shutdown_timeout_ms: 30_000,
        }
    }
}

impl TransportConfig {
    pub fn validate(&self) -> Result<(), String> {
        let nonzero = [
            ("max_active_requests", self.max_active_requests),
            ("max_active_long_polls", self.max_active_long_polls),
            ("max_active_mutations", self.max_active_mutations),
            ("max_active_queries", self.max_active_queries),
            ("reply_queue_count", self.reply_queue_count),
            ("reply_queue_bytes", self.reply_queue_bytes),
            ("max_wire_bytes", self.max_wire_bytes),
            ("max_reply_bytes", self.max_reply_bytes),
            ("max_batch_items", self.max_batch_items),
            ("max_batch_bytes", self.max_batch_bytes),
        ];
        for (name, value) in nonzero {
            if value == 0 {
                return Err(format!("transport.{name} must be greater than zero"));
            }
        }
        if self.max_active_long_polls >= self.max_active_requests {
            return Err(
                "transport.max_active_long_polls must be lower than max_active_requests".into(),
            );
        }
        if self.max_reply_bytes > self.reply_queue_bytes {
            return Err("transport.max_reply_bytes must not exceed reply_queue_bytes".into());
        }
        if self.reply_queue_bytes > u32::MAX as usize {
            return Err("transport.reply_queue_bytes must fit in a u32 semaphore budget".into());
        }
        if self.max_wire_bytes > i64::MAX as usize {
            return Err("transport.max_wire_bytes is too large for ZeroMQ".into());
        }
        if self.max_batch_items > u32::MAX as usize {
            return Err("transport.max_batch_items must fit in u32".into());
        }
        if self.lease_long_poll_ms == 0 || self.shutdown_timeout_ms == 0 {
            return Err("transport timeouts must be greater than zero".into());
        }
        if self.zmq_sndhwm <= 0 || self.zmq_rcvhwm <= 0 || self.zmq_linger_ms < 0 {
            return Err("transport ZMQ HWM values must be positive and linger non-negative".into());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct ConfigTomlServer {
    pub host: String,

    // Renamed from cgrpc_token; keep the alias so existing configs still authenticate.
    #[serde(alias = "cgrpc_token")]
    pub auth_token: Option<String>,

    pub transport: TransportConfig,
}

impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            host: default_host(),
            auth_token: None,
            transport: TransportConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub config_toml: ConfigTomlServer,
}

impl Config {
    /// Load `config.toml`, using defaults only when the file is absent.
    /// Malformed or unsafe production configuration is returned as an error.
    pub fn load_checked() -> Result<Self, String> {
        let config_toml = match fs::read_to_string("config.toml") {
            Ok(content) => toml::from_str::<ConfigTomlServer>(&content)
                .map_err(|err| format!("Failed to parse config.toml: {err}"))?,
            Err(err) if err.kind() == ErrorKind::NotFound => ConfigTomlServer::default(),
            Err(err) => return Err(format!("Failed to read config.toml: {err}")),
        };
        config_toml.transport.validate()?;
        Ok(Self { config_toml })
    }

    /// Compatibility loader for library/CLI callers. The production server uses
    /// [`Config::load_checked`] and fails closed on malformed configuration.
    #[allow(clippy::new_without_default)]
    #[instrument]
    pub fn new() -> Self {
        Self::load_checked().unwrap_or_else(|err| {
            error!("{err}");
            Self::default()
        })
    }
}
