use std::{fs, io::ErrorKind, path::Path};

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
    8192
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

    /// Bounded capacity of each per-type in-memory task queue. The refill path
    /// tops a queue up from RocksDB once it drains below a quarter of this.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
}
impl Default for ConfigTomlServer {
    fn default() -> Self {
        Self {
            host: default_host(),
            auth_token: Option::None,
            lease_ttl_secs: default_lease_ttl_secs(),
            reap_interval_secs: default_reap_interval_secs(),
            queue_capacity: default_queue_capacity(),
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
        Self::from_file("config.toml")
    }

    /// Loads the config from `path`; a missing file falls back to defaults.
    ///
    /// # Panics
    /// If the file exists but can't be read or parsed. Falling back to defaults
    /// there would drop the configured `auth_token`, and an unset token makes
    /// admin auth permissive. See [`Config::try_from_file`].
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
        fs::write(&path, "cgrpc_token = \"secret\"\nlease_ttl_secs = 42\n").unwrap();
        let cfg = Config::from_file(&path).config_toml;
        let _ = fs::remove_file(&path);

        assert_eq!(cfg.auth_token.as_deref(), Some("secret"));
        assert_eq!(cfg.lease_ttl_secs, 42);
        assert_eq!(cfg.reap_interval_secs, default_reap_interval_secs());
        assert_eq!(cfg.queue_capacity, default_queue_capacity());
        assert_eq!(cfg.host, default_host());
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
