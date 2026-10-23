use std::{collections::HashMap, fs, io::ErrorKind, path::Path};

use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

fn default_host() -> String {
    "[::1]:50051".into()
}

fn default_lease_ttl_secs() -> u64 {
    300
}

fn default_reap_interval_secs() -> u64 {
    15
}

fn default_queue_capacity() -> usize {
    65_536
}

fn default_proxy_host() -> String {
    "[::1]:50050".into()
}

fn default_max_message_bytes() -> i64 {
    64 * 1024 * 1024
}

fn default_session_idle_secs() -> u64 {
    900
}

fn default_heartbeat_secs() -> u64 {
    5
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigTomlServer {
    #[serde(default = "default_host")]
    pub host: String,

    // Renamed from cgrpc_token; keep the alias so existing configs still authenticate.
    #[serde(alias = "cgrpc_token")]
    pub auth_token: Option<String>,

    /// How long a lease stays valid without a `renew`, in seconds. Once it lapses
    /// the reaper drops the lease and the task becomes leasable again.
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u64,

    /// How often the reaper sweeps for expired leases, in seconds. A lease can
    /// outlive its TTL by up to this long.
    #[serde(default = "default_reap_interval_secs")]
    pub reap_interval_secs: u64,

    /// Approximate number of *tasks* each per-type in-memory queue holds. The
    /// queue holds chunks, so its capacity is `queue_capacity / chunk_size`
    /// chunks (at least one); a derived type's chunk counts as one task. The
    /// refill path tops a queue up from RocksDB once it drains below a quarter
    /// of that.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,

    /// Per-type overrides, keyed `"namespace:name"`. They take precedence over
    /// a mod's `Task::limits()` and the engine defaults.
    #[serde(default)]
    pub tasks: HashMap<String, TaskTypeConfig>,

    /// Identifies this server to clients and the proxy.
    #[serde(default)]
    pub server_id: u16,

    /// Endpoint clients should use to reach this server, as announced to the
    /// proxy. Defaults to `host`; set it when binding a wildcard address.
    #[serde(default)]
    pub advertise: Option<String>,

    /// Proxy to register with (heartbeat every `heartbeat_secs`). Unset = none.
    #[serde(default)]
    pub proxy: Option<String>,

    /// Address the `proxy` binary listens on.
    #[serde(default = "default_proxy_host")]
    pub proxy_host: String,

    /// Largest accepted inbound message; bigger ones drop the connection.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: i64,

    /// Sessions idle this long are forgotten; the client must `Hello` again.
    #[serde(default = "default_session_idle_secs")]
    pub session_idle_secs: u64,

    /// Interval of server -> proxy registration heartbeats.
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,

    /// Shared secret servers present when registering with the proxy. The proxy
    /// refuses every registration while it is unset.
    #[serde(default)]
    pub proxy_token: Option<String>,
}
impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            host: default_host(),
            auth_token: Option::None,
            lease_ttl_secs: default_lease_ttl_secs(),
            reap_interval_secs: default_reap_interval_secs(),
            queue_capacity: default_queue_capacity(),
            server_id: 0,
            advertise: None,
            proxy: None,
            proxy_host: default_proxy_host(),
            max_message_bytes: default_max_message_bytes(),
            session_idle_secs: default_session_idle_secs(),
            heartbeat_secs: default_heartbeat_secs(),
            tasks: HashMap::new(),
            proxy_token: None,
        }
    }
}

/// `[tasks."namespace:name"]` table in `config.toml`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskTypeConfig {
    pub chunk_size: Option<u32>,
    pub max_payload: Option<u32>,
    pub max_result: Option<u32>,
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub config_toml: ConfigTomlServer,
}

impl Config {
    #[allow(clippy::new_without_default)]
    #[instrument]
    pub fn new() -> Self {
        Self::from_file("config.toml")
    }

    /// Loads the config from `path`; a missing file falls back to defaults.
    ///
    /// # Panics
    /// If the file exists but can't be read or parsed. Falling back to defaults
    /// there would silently drop the configured `auth_token` (disabling admin
    /// access) and every other setting. See [`Config::try_from_file`].
    #[instrument(skip_all)]
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        Self::try_from_file(path).unwrap_or_else(|err| panic!("{err}"))
    }

    /// Like [`Config::from_file`], but returns a descriptive error instead of
    /// panicking when the file exists but can't be read or parsed.
    pub fn try_from_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
            Err(err) => {
                return Err(format!(
                    "Failed to read config file {}: {err}",
                    path.display()
                ));
            }
        };
        let config_toml: ConfigTomlServer = toml::from_str(&content).map_err(|err| {
            error!("Failed to parse config file {}: {err}", path.display());
            format!("Invalid config file {}: {err}", path.display())
        })?;
        Ok(Self { config_toml })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_file_reads_keys_and_defaults_the_rest() {
        let path =
            std::env::temp_dir().join(format!("enginelib-config-test-{}.toml", std::process::id()));
        fs::write(
            &path,
            "cgrpc_token = \"secret\"\nlease_ttl_secs = 42\n[tasks.\"ns:t\"]\nchunk_size = 7\n",
        )
        .unwrap();
        let cfg = Config::from_file(&path).config_toml;
        let _ = fs::remove_file(&path);

        assert_eq!(cfg.auth_token.as_deref(), Some("secret"));
        assert_eq!(cfg.lease_ttl_secs, 42);
        assert_eq!(cfg.reap_interval_secs, default_reap_interval_secs());
        assert_eq!(cfg.queue_capacity, default_queue_capacity());
        assert_eq!(cfg.host, default_host());
        assert_eq!(cfg.tasks["ns:t"].chunk_size, Some(7));
        assert_eq!(cfg.tasks["ns:t"].max_payload, None);
    }

    #[test]
    fn from_file_missing_falls_back_to_defaults() {
        let cfg = Config::from_file("/nonexistent/enginelib/config.toml").config_toml;
        assert_eq!(cfg.auth_token, None);
        assert_eq!(cfg.lease_ttl_secs, 300);
        assert_eq!(cfg.reap_interval_secs, 15);
    }

    #[test]
    fn invalid_file_fails_closed_instead_of_dropping_the_token() {
        let path = std::env::temp_dir().join(format!(
            "enginelib-config-invalid-{}.toml",
            std::process::id()
        ));
        // A type error in a numeric field must not silently discard auth_token.
        fs::write(
            &path,
            "auth_token = \"secret\"\nqueue_capacity = \"8192\"\n",
        )
        .unwrap();
        let result = Config::try_from_file(&path);
        let panicked = std::panic::catch_unwind(|| Config::from_file(&path)).is_err();
        let _ = fs::remove_file(&path);

        let err = result.unwrap_err();
        assert!(err.contains("queue_capacity"), "{err}");
        assert!(panicked);
    }
}
