//! Server builder for Sagitta.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::{InMemoryUserStore, UserStore};
use crate::{MemoryStore, Store};
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::info;

use crate::config::Config;
use crate::service::{CustomAction, SagittaService};

/// Builder for configuring and running an Arrow Flight server.
///
/// # Errors
///
/// Returns an error from `serve` if binding or TLS configuration fails.
#[non_exhaustive]
pub struct Sagitta {
    config: Config,
    store: Option<Arc<dyn Store>>,
    user_store: Option<Arc<dyn UserStore>>,
    custom_actions: Vec<Arc<dyn CustomAction>>,
}

impl Sagitta {
    /// Create a new builder with default configuration.
    pub fn builder() -> Self {
        Self {
            config: Config::default(),
            store: None,
            user_store: None,
            custom_actions: Vec::new(),
        }
    }

    /// Set the server configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Set the storage backend.
    pub fn store(mut self, store: Arc<dyn Store>) -> Self {
        self.store = Some(store);
        self
    }

    /// Set the user store for authentication.
    pub fn user_store(mut self, user_store: Arc<dyn UserStore>) -> Self {
        self.user_store = Some(user_store);
        self
    }

    /// Register a custom action handler.
    pub fn action(mut self, action: Arc<dyn CustomAction>) -> Self {
        self.custom_actions.push(action);
        self
    }

    /// Build and start the server, blocking until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the listen address is invalid, TLS configuration
    /// fails, or the server cannot bind.
    pub async fn serve(self) -> anyhow::Result<()> {
        let config = &self.config;
        let addr: SocketAddr = config.listen_addr.parse()?;

        let store: Arc<dyn Store> = self.store.unwrap_or_else(|| {
            if config.enable_test_fixtures {
                Arc::new(MemoryStore::with_test_fixtures())
            } else {
                Arc::new(MemoryStore::new())
            }
        });

        let user_store: Arc<dyn UserStore> = self.user_store.unwrap_or_else(|| {
            if config.enable_test_fixtures {
                Arc::new(InMemoryUserStore::with_test_users())
            } else {
                Arc::new(InMemoryUserStore::new())
            }
        });

        let mut service = SagittaService::build(
            store,
            user_store,
            &config.catalog_name,
            &config.default_schema,
        )
        .await;

        for action in self.custom_actions {
            service = service.register_action(action);
        }

        let mut server = Server::builder();

        if config.server.tcp_keepalive_secs > 0 {
            server =
                server.tcp_keepalive(Some(Duration::from_secs(config.server.tcp_keepalive_secs)));
        }

        if let Some(tls_config) = &config.tls {
            let tls = load_tls_config(tls_config)?;
            server = server.tls_config(tls)?;
            info!(
                address = %addr,
                tls = true,
                mtls = tls_config.ca_path.is_some(),
                "sagitta starting"
            );
        } else {
            info!(address = %addr, tls = false, "sagitta starting");
        }

        let router = server.add_service(FlightServiceServer::new(service));

        let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);

        router
            .serve_with_shutdown(addr, async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install signal handler");
                info!("shutdown signal received, draining connections...");
            })
            .await?;

        tokio::time::sleep(shutdown_timeout).await;
        info!("shutdown complete");

        Ok(())
    }
}

fn load_tls_config(tls: &crate::config::TlsConfig) -> anyhow::Result<ServerTlsConfig> {
    let cert = std::fs::read_to_string(&tls.cert_path)?;
    let key = std::fs::read_to_string(&tls.key_path)?;
    let identity = Identity::from_pem(cert, key);

    let mut config = ServerTlsConfig::new().identity(identity);

    if let Some(ca_path) = &tls.ca_path {
        let ca_cert = std::fs::read_to_string(ca_path)?;
        let ca = tonic::transport::Certificate::from_pem(ca_cert);
        config = config
            .client_ca_root(ca)
            .client_auth_optional(tls.client_auth_optional);
    }

    Ok(config)
}
