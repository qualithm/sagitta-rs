//! Configuration for Sagitta.

use std::collections::HashMap;

use serde::Deserialize;
use std::path::Path;

/// Arrow Flight server configuration.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct Config {
    /// Address to listen on (e.g., "0.0.0.0:50051").
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// TLS configuration (optional).
    pub tls: Option<TlsConfig>,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Server configuration.
    #[serde(default)]
    pub server: ServerConfig,

    /// Catalog name for SQL queries.
    #[serde(default = "default_catalog_name")]
    pub catalog_name: String,

    /// Default schema name for SQL queries.
    #[serde(default = "default_schema_name")]
    pub default_schema: String,

    /// Whether to load test fixtures on startup.
    #[serde(default)]
    pub enable_test_fixtures: bool,

    /// Arbitrary key-value options forwarded to the storage backend.
    ///
    /// The meaning of each key is backend-specific; unknown keys are ignored.
    #[serde(default)]
    pub store_options: HashMap<String, String>,
}

/// TLS configuration.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct TlsConfig {
    /// Path to the server certificate.
    pub cert_path: String,
    /// Path to the server private key.
    pub key_path: String,
    /// Path to the CA certificate for client verification (mTLS).
    pub ca_path: Option<String>,
    /// Whether client certificate is optional when mTLS is enabled.
    /// Defaults to false (client cert required when ca_path is set).
    #[serde(default)]
    pub client_auth_optional: bool,
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LoggingConfig {
    /// Log level filter (e.g., "info", "debug", "trace").
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format: "json" or "pretty".
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct ServerConfig {
    /// Graceful shutdown timeout in seconds.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,

    /// TCP keepalive interval in seconds (0 to disable).
    #[serde(default = "default_tcp_keepalive_secs")]
    pub tcp_keepalive_secs: u64,

    /// Maximum concurrent connections (0 for unlimited).
    #[serde(default)]
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            tcp_keepalive_secs: default_tcp_keepalive_secs(),
            max_connections: 0,
        }
    }
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

fn default_tcp_keepalive_secs() -> u64 {
    60
}

fn default_listen_addr() -> String {
    "0.0.0.0:50051".to_string()
}

fn default_catalog_name() -> String {
    "default".to_string()
}

fn default_schema_name() -> String {
    "public".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            tls: None,
            logging: LoggingConfig::default(),
            server: ServerConfig::default(),
            catalog_name: default_catalog_name(),
            default_schema: default_schema_name(),
            enable_test_fixtures: false,
            store_options: HashMap::new(),
        }
    }
}

impl Config {
    /// Load configuration from file or environment.
    ///
    /// Looks for config in the following order:
    /// 1. `SAGITTA_CONFIG` environment variable (if file exists)
    /// 2. `sagitta.toml` in current directory (if file exists)
    /// 3. Default configuration
    ///
    /// # Errors
    ///
    /// Returns an error if a config file is found but cannot be read or parsed.
    pub fn load() -> anyhow::Result<Self> {
        // Check environment variable first
        if let Ok(path) = std::env::var("SAGITTA_CONFIG")
            && Path::new(&path).exists()
        {
            return Self::from_file(&path);
        }

        // Check for config file in current directory
        let default_path = "sagitta.toml";
        if Path::new(default_path).exists() {
            return Self::from_file(default_path);
        }

        // Use defaults
        Ok(Self::default())
    }

    /// Load configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or contains invalid TOML.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Parse configuration from a TOML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the string contains invalid TOML.
    pub fn parse_toml(content: &str) -> anyhow::Result<Self> {
        let config: Config = toml::from_str(content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.listen_addr, "0.0.0.0:50051");
        assert!(config.tls.is_none());
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "pretty");
        assert_eq!(config.server.shutdown_timeout_secs, 30);
        assert_eq!(config.server.tcp_keepalive_secs, 60);
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            listen_addr = "127.0.0.1:8080"
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1:8080");
        assert!(config.tls.is_none());
    }

    #[test]
    fn test_parse_tls_config() {
        let toml = r#"
            listen_addr = "0.0.0.0:50051"

            [tls]
            cert_path = "/path/to/cert.pem"
            key_path = "/path/to/key.pem"
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert!(config.tls.is_some());

        let tls = config.tls.unwrap();
        assert_eq!(tls.cert_path, "/path/to/cert.pem");
        assert_eq!(tls.key_path, "/path/to/key.pem");
        assert!(tls.ca_path.is_none());
    }

    #[test]
    fn test_parse_mtls_config() {
        let toml = r#"
            listen_addr = "0.0.0.0:50051"

            [tls]
            cert_path = "/path/to/cert.pem"
            key_path = "/path/to/key.pem"
            ca_path = "/path/to/ca.pem"
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert!(config.tls.is_some());

        let tls = config.tls.unwrap();
        assert_eq!(tls.cert_path, "/path/to/cert.pem");
        assert_eq!(tls.key_path, "/path/to/key.pem");
        assert_eq!(tls.ca_path, Some("/path/to/ca.pem".to_string()));
    }

    #[test]
    fn test_default_listen_addr_when_omitted() {
        let toml = r#"
            [tls]
            cert_path = "/path/to/cert.pem"
            key_path = "/path/to/key.pem"
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert_eq!(config.listen_addr, "0.0.0.0:50051");
    }

    #[test]
    fn test_parse_logging_config() {
        let toml = r#"
            [logging]
            level = "debug"
            format = "json"
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.format, "json");
    }

    #[test]
    fn test_parse_server_config() {
        let toml = r#"
            [server]
            shutdown_timeout_secs = 60
            tcp_keepalive_secs = 120
            max_connections = 1000
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert_eq!(config.server.shutdown_timeout_secs, 60);
        assert_eq!(config.server.tcp_keepalive_secs, 120);
        assert_eq!(config.server.max_connections, 1000);
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
            listen_addr = "127.0.0.1:9999"

            [logging]
            level = "trace"
            format = "json"

            [server]
            shutdown_timeout_secs = 10
            tcp_keepalive_secs = 30
            max_connections = 500

            [tls]
            cert_path = "/certs/server.pem"
            key_path = "/certs/key.pem"
            ca_path = "/certs/ca.pem"
            client_auth_optional = true
        "#;

        let config = Config::parse_toml(toml).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1:9999");
        assert_eq!(config.logging.level, "trace");
        assert_eq!(config.logging.format, "json");
        assert_eq!(config.server.shutdown_timeout_secs, 10);
        assert_eq!(config.server.tcp_keepalive_secs, 30);
        assert_eq!(config.server.max_connections, 500);

        let tls = config.tls.unwrap();
        assert_eq!(tls.cert_path, "/certs/server.pem");
        assert!(tls.client_auth_optional);
    }
}
