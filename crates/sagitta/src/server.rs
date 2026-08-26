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
use tonic::transport::Server;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::info;

use crate::config::Config;
use crate::extension::SharedSessionExtension;
use crate::interceptor::SharedInterceptor;
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
    acquiring:
      Pin<Box<dyn Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>> + Send>>,
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

/// A [`TcpStream`] that may carry a semaphore permit.
///
/// When a permit is present, dropping this value releases it, decrementing the
/// active connection count.
struct LimitedStream {
  inner: TcpStream,
  _permit: Option<OwnedSemaphorePermit>,
}

impl LimitedStream {
  fn from_parts(inner: TcpStream, permit: OwnedSemaphorePermit) -> Self {
    Self {
      inner,
      _permit: Some(permit),
    }
  }

  fn unlimited(inner: TcpStream) -> Self {
    Self {
      inner,
      _permit: None,
    }
  }
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
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
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
              let old = std::mem::replace(&mut this.state, LimitedIncomingState::Idle);
              if let LimitedIncomingState::Accepted { stream, .. } = old {
                return Poll::Ready(Some(Ok(LimitedStream::from_parts(stream, permit))));
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
  interceptor: Option<SharedInterceptor>,
  session_extension: Option<SharedSessionExtension>,
}

impl Sagitta {
  /// Create a new builder with default configuration.
  pub fn builder() -> Self {
    Self {
      config: Config::default(),
      store: None,
      user_store: None,
      custom_actions: Vec::new(),
      interceptor: None,
      session_extension: None,
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

  /// Register a [`StatementInterceptor`](crate::StatementInterceptor) consulted
  /// by the SQL engine before its default statement handling.
  pub fn interceptor(mut self, interceptor: SharedInterceptor) -> Self {
    self.interceptor = Some(interceptor);
    self
  }

  /// Register a [`SessionExtension`](crate::SessionExtension) that applies
  /// embedder DataFusion extensions (`ScalarUDF`s, table functions, optimizer
  /// rules) to the SQL engine's
  /// [`SessionContext`](datafusion::prelude::SessionContext).
  pub fn session_extension(mut self, extension: SharedSessionExtension) -> Self {
    self.session_extension = Some(extension);
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

    if let Some(interceptor) = self.interceptor {
      service = service.with_interceptor(interceptor);
    }

    if let Some(extension) = self.session_extension {
      service = service.with_session_extension(extension);
    }

    for action in self.custom_actions {
      service = service.register_action(action);
    }

    let mut server = Server::builder();

    if config.server.tcp_keepalive_secs > 0 {
      server = server.tcp_keepalive(Some(Duration::from_secs(config.server.tcp_keepalive_secs)));
    }

    let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
    let max_connections = config.server.max_connections;

    let shutdown_signal = async {
      tokio::signal::ctrl_c()
        .await
        .expect("failed to install signal handler");
      info!("shutdown signal received, draining connections...");
    };

    if let Some(tls_config) = &config.tls {
      // The acceptor's certificate resolver re-reads the identity from a
      // shared holder on every new connection, so a cert-manager renewal
      // (kubelet refreshes the mounted files) takes effect with no restart.
      // Established connections keep the identity they negotiated.
      let resolver = crate::tls_reload::ReloadingCertResolver::from_paths(
        &tls_config.cert_path,
        &tls_config.key_path,
      )?;
      let rustls_config = crate::tls_reload::reloading_server_config(tls_config, resolver.clone())?;
      crate::tls_reload::watch_identity(
        resolver,
        tls_config.cert_path.clone(),
        tls_config.key_path.clone(),
        tls_config.cert_reload_interval_secs,
      );
      let acceptor = crate::tls_reload::reloading_acceptor(rustls_config);
      info!(
          address = %addr,
          tls = true,
          mtls = tls_config.ca_path.is_some(),
          "sagitta starting"
      );

      let router = server.add_service(FlightServiceServer::new(service));
      let listener = TcpListener::bind(addr).await?;
      let incoming = TlsIncoming::new(listener, acceptor, max_connections);
      router
        .serve_with_incoming_shutdown(incoming, shutdown_signal)
        .await?;

      tokio::time::sleep(shutdown_timeout).await;
      info!("shutdown complete");
      return Ok(());
    }

    info!(address = %addr, tls = false, "sagitta starting");
    let router = server.add_service(FlightServiceServer::new(service));

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

// ---------------------------------------------------------------------------
// TLS incoming stream with hot-reloaded identity
// ---------------------------------------------------------------------------

/// A [`TcpListener`] incoming stream that completes the TLS handshake per
/// accepted connection using an acceptor whose certificate identity is
/// hot-reloaded from disk.
struct TlsIncoming {
  listener: TcpListener,
  acceptor: tokio_rustls::TlsAcceptor,
  semaphore: Option<Arc<Semaphore>>,
  state: TlsIncomingState,
}

enum TlsIncomingState {
  Idle,
  Handshaking {
    handshake: Pin<
      Box<dyn Future<Output = io::Result<tokio_rustls::server::TlsStream<LimitedStream>>> + Send>,
    >,
  },
}

impl TlsIncoming {
  fn new(
    listener: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    max_connections: usize,
  ) -> Self {
    Self {
      listener,
      acceptor,
      semaphore: (max_connections > 0).then(|| Arc::new(Semaphore::new(max_connections))),
      state: TlsIncomingState::Idle,
    }
  }
}

impl Stream for TlsIncoming {
  type Item = io::Result<tokio_rustls::server::TlsStream<LimitedStream>>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    loop {
      match &mut self.state {
        TlsIncomingState::Idle => match self.listener.poll_accept(cx) {
          Poll::Pending => return Poll::Pending,
          Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
          Poll::Ready(Ok((stream, _))) => {
            // Bound concurrent connections the same way [`LimitedIncoming`]
            // does: without a permit the socket is dropped and the client
            // retries; with one, the permit rides the TLS stream and is
            // released when tonic closes the connection.
            let permit = match &self.semaphore {
              None => None,
              Some(semaphore) => match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => continue,
              },
            };
            let stream = match permit {
              Some(permit) => LimitedStream::from_parts(stream, permit),
              None => LimitedStream::unlimited(stream),
            };
            let acceptor = self.acceptor.clone();
            self.state = TlsIncomingState::Handshaking {
              handshake: Box::pin(async move { acceptor.accept(stream).await }),
            };
          }
        },
        TlsIncomingState::Handshaking { handshake } => {
          let result = std::task::ready!(handshake.as_mut().poll(cx));
          self.state = TlsIncomingState::Idle;
          match result {
            Ok(tls_stream) => return Poll::Ready(Some(Ok(tls_stream))),
            Err(e) => {
              // A failed handshake (e.g. a missing/invalid client cert under
              // mTLS) must not take the listener down: log and keep accepting.
              tracing::debug!(error = %e, "tls handshake failed");
            }
          }
        }
      }
    }
  }
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

    let action = NoopAction;
    assert_eq!(action.action_type(), "noop");
    assert_eq!(action.description(), "does nothing");
    assert!(action.execute(Bytes::new()).unwrap().is_empty());

    let sagitta = Sagitta::builder().action(Arc::new(NoopAction));
    assert_eq!(sagitta.custom_actions.len(), 1);
  }

  #[test]
  fn test_builder_with_interceptor() {
    use crate::interceptor::{QueryInterception, StatementInterceptor};
    use crate::sql::SqlResult;

    struct NoopInterceptor;

    #[async_trait::async_trait]
    impl StatementInterceptor for NoopInterceptor {
      async fn intercept_update(&self, _sql: &str) -> SqlResult<Option<i64>> {
        Ok(None)
      }
      async fn intercept_query(&self, _sql: &str) -> SqlResult<Option<QueryInterception>> {
        Ok(None)
      }
    }

    let sagitta = Sagitta::builder().interceptor(Arc::new(NoopInterceptor));
    assert!(sagitta.interceptor.is_some());
  }

  #[test]
  fn test_builder_with_session_extension() {
    use datafusion::prelude::SessionContext;

    let sagitta = Sagitta::builder().session_extension(Arc::new(|_ctx: &SessionContext| {}));
    assert!(sagitta.session_extension.is_some());
  }

  #[test]
  fn test_tls_reload_missing_cert_file() {
    let result = crate::tls_reload::ReloadingCertResolver::from_paths(
      "/nonexistent/cert.pem",
      "/nonexistent/key.pem",
    );
    assert!(result.is_err());
  }

  /// Serve with TLS, swap the cert/key files, and assert a new connection
  /// presents the new leaf — the cert-manager renewal path with no restart.
  #[tokio::test]
  async fn test_served_certificate_follows_file_rewrite() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    write_self_signed(&cert_path, &key_path, "first");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls = crate::config::TlsConfig {
      cert_path: cert_path.to_str().unwrap().to_string(),
      key_path: key_path.to_str().unwrap().to_string(),
      ca_path: None,
      client_auth_optional: false,
      cert_reload_interval_secs: 0,
    };
    let resolver =
      crate::tls_reload::ReloadingCertResolver::from_paths(&tls.cert_path, &tls.key_path).unwrap();
    let rustls_config = crate::tls_reload::reloading_server_config(&tls, resolver.clone()).unwrap();
    let acceptor = crate::tls_reload::reloading_acceptor(rustls_config);
    let mut incoming = TlsIncoming::new(listener, acceptor, 0);

    // A 1s watcher would keep the test fast enough while still exercising the
    // file-watch path rather than a manual resolver.replace.
    crate::tls_reload::watch_identity(resolver, tls.cert_path.clone(), tls.key_path.clone(), 1);

    let server = tokio::spawn(async move {
      use futures::StreamExt;
      while let Some(conn) = incoming.next().await {
        let mut stream = conn.unwrap();
        // Drain until the client closes; the handshake is what matters.
        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf).await;
        let _ = stream.shutdown().await;
      }
    });

    let before = presented_cert(addr, &cert_path, "first").await;

    // Rotate the files; wait past one watcher tick, then reconnect.
    write_self_signed(&cert_path, &key_path, "second");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let after = presented_cert(addr, &cert_path, "second").await;

    assert_ne!(before, after);

    server.abort();
  }

  /// Connect with a client that trusts whatever `ca_cert_path` currently holds
  /// and return the leaf the server presented.
  async fn presented_cert(
    addr: SocketAddr,
    ca_cert_path: &std::path::Path,
    server_name: &str,
  ) -> rustls::pki_types::CertificateDer<'static> {
    use rustls::pki_types::ServerName;

    let certs = std::fs::read(ca_cert_path).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut certs.as_slice()) {
      roots.add(cert.unwrap()).unwrap();
    }
    let client_config = rustls::ClientConfig::builder()
      .with_root_certificates(roots)
      .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls_stream = connector
      .connect(ServerName::try_from(server_name.to_string()).unwrap(), tcp)
      .await
      .unwrap();
    let (_, session) = tls_stream.get_ref();
    session
      .peer_certificates()
      .unwrap()
      .first()
      .unwrap()
      .clone()
  }

  fn write_self_signed(cert_path: &std::path::Path, key_path: &std::path::Path, cn: &str) {
    let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
      .distinguished_name
      .push(rcgen::DnType::CommonName, cn);
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    std::fs::write(cert_path, cert.pem()).unwrap();
    std::fs::write(key_path, key_pair.serialize_pem()).unwrap();
  }
}
