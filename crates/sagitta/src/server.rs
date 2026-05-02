//! Server builder for Sagitta.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::{InMemoryUserStore, UserStore};
use crate::{MemoryStore, Store};
use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::info;

use crate::config::Config;
use crate::service::{CustomAction, SagittaService};

// ---------------------------------------------------------------------------
// Connection-limited TCP incoming stream
// ---------------------------------------------------------------------------

/// State machine for [`LimitedIncoming`].
enum LimitedIncomingState {
    /// No accepted connection waiting — poll the listener next.
    Idle,
    /// A connection has been accepted from the OS; waiting for a semaphore permit.
    Accepted {
        stream: TcpStream,
        acquiring: Pin<
            Box<
                dyn Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>> + Send,
            >,
        >,
    },
}

/// A TCP incoming stream that limits the number of concurrent connections.
///
/// When the active connection count reaches `max_connections`, new TCP
/// connections are accepted from the OS but held until a permit becomes
/// available, providing back-pressure without closing connections.
struct LimitedIncoming {
    listener: TcpListener,
    semaphore: Arc<Semaphore>,
    state: LimitedIncomingState,
}

impl LimitedIncoming {
    fn new(listener: TcpListener, max_connections: usize) -> Self {
        Self {
            listener,
            semaphore: Arc::new(Semaphore::new(max_connections)),
            state: LimitedIncomingState::Idle,
        }
    }
}

/// A [`TcpStream`] paired with a semaphore permit.
///
/// Dropping this value releases the permit, decrementing the active connection count.
struct LimitedStream {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl AsyncRead for LimitedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for LimitedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl Connected for LimitedStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl Stream for LimitedIncoming {
    type Item = io::Result<LimitedStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                LimitedIncomingState::Idle => {
                    // Accept the next TCP connection.
                    match this.listener.poll_accept(cx) {
                        Poll::Ready(Ok((stream, _))) => {
                            let sem = Arc::clone(&this.semaphore);
                            this.state = LimitedIncomingState::Accepted {
                                stream,
                                acquiring: Box::pin(async move { sem.acquire_owned().await }),
                            };
                            // Fall through to poll the acquiring future.
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                LimitedIncomingState::Accepted {
                    stream: _,
                    acquiring,
                } => {
                    match acquiring.as_mut().poll(cx) {
                        Poll::Ready(Ok(permit)) => {
                            // Swap state back to Idle and return the stream.
                            let old =
                                std::mem::replace(&mut this.state, LimitedIncomingState::Idle);
                            if let LimitedIncomingState::Accepted { stream, .. } = old {
                                return Poll::Ready(Some(Ok(LimitedStream {
                                    inner: stream,
                                    _permit: permit,
                                })));
                            }
                            unreachable!()
                        }
                        Poll::Ready(Err(_)) => return Poll::Ready(None), // semaphore closed
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

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
        let max_connections = config.server.max_connections;

        let shutdown_signal = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install signal handler");
            info!("shutdown signal received, draining connections...");
        };

        if max_connections > 0 {
            info!(max_connections, "connection limit active");
            let listener = TcpListener::bind(addr).await?;
            let incoming = LimitedIncoming::new(listener, max_connections);
            router
                .serve_with_incoming_shutdown(incoming, shutdown_signal)
                .await?;
        } else {
            router.serve_with_shutdown(addr, shutdown_signal).await?;
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryUserStore, MemoryStore};

    #[test]
    fn test_builder_creates_default_config() {
        let sagitta = Sagitta::builder();
        assert!(sagitta.store.is_none());
        assert!(sagitta.user_store.is_none());
        assert!(sagitta.custom_actions.is_empty());
    }

    #[test]
    fn test_builder_with_config() {
        let config = Config {
            listen_addr: "127.0.0.1:12345".to_string(),
            ..Default::default()
        };
        let sagitta = Sagitta::builder().config(config.clone());
        assert_eq!(sagitta.config.listen_addr, "127.0.0.1:12345");
    }

    #[test]
    fn test_builder_with_store() {
        let store = Arc::new(MemoryStore::new());
        let sagitta = Sagitta::builder().store(store);
        assert!(sagitta.store.is_some());
    }

    #[test]
    fn test_builder_with_user_store() {
        let user_store = Arc::new(InMemoryUserStore::new());
        let sagitta = Sagitta::builder().user_store(user_store);
        assert!(sagitta.user_store.is_some());
    }

    #[test]
    fn test_builder_with_action() {
        use crate::service::CustomAction;
        use bytes::Bytes;
        use tonic::Status;

        struct NoopAction;
        impl CustomAction for NoopAction {
            fn action_type(&self) -> &str {
                "noop"
            }
            fn description(&self) -> &str {
                "does nothing"
            }
            fn execute(&self, _body: Bytes) -> Result<Vec<Bytes>, Status> {
                Ok(vec![])
            }
        }

        let sagitta = Sagitta::builder().action(Arc::new(NoopAction));
        assert_eq!(sagitta.custom_actions.len(), 1);
    }

    #[test]
    fn test_load_tls_config_missing_cert_file() {
        let tls = crate::config::TlsConfig {
            cert_path: "/nonexistent/cert.pem".to_string(),
            key_path: "/nonexistent/key.pem".to_string(),
            ca_path: None,
            client_auth_optional: false,
        };
        let result = load_tls_config(&tls);
        assert!(result.is_err());
    }
}
