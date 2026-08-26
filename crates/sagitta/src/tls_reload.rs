//! Hot-reloading server certificate for Flight TLS.
//!
//! cert-manager (and rotation generally) rewrites the cert/key files in place;
//! a rustls [`ServerConfig`] built once at startup would keep serving the old
//! certificate until the process restarts. [`ReloadingCertResolver`] instead
//! reads the active identity from a shared holder on every new connection, and
//! [`watch_identity`] rebuilds that identity whenever the files' mtimes change.
//! Established connections keep the identity they negotiated.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

use crate::config::TlsConfig;

/// A certificate resolver that serves whichever identity was loaded most
/// recently. Cloning shares the holder.
#[derive(Debug, Clone)]
pub struct ReloadingCertResolver {
  active: Arc<RwLock<Arc<CertifiedKey>>>,
}

impl ReloadingCertResolver {
  /// Load the initial identity from `cert_path`/`key_path`.
  ///
  /// # Errors
  ///
  /// Returns an error if the files cannot be read or contain no usable
  /// PEM-encoded certificate chain and private key.
  pub fn from_paths(cert_path: &str, key_path: &str) -> anyhow::Result<Self> {
    let identity = load_certified_key(cert_path, key_path)?;
    Ok(Self {
      active: Arc::new(RwLock::new(Arc::new(identity))),
    })
  }

  /// Swap in a freshly loaded identity.
  fn replace(&self, identity: CertifiedKey) {
    let mut guard = self.active.write().expect("cert resolver lock poisoned");
    *guard = Arc::new(identity);
  }
}

impl ResolvesServerCert for ReloadingCertResolver {
  fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
    Some(
      self
        .active
        .read()
        .expect("cert resolver lock poisoned")
        .clone(),
    )
  }
}

/// Install the ring crypto provider as the process default and return it.
///
/// Tonic's `tls-ring` feature enables the same provider; installing is
/// idempotent, and a conflicting pre-installed provider is a configuration
/// error worth failing loudly on.
fn ring_provider() -> Arc<rustls::crypto::CryptoProvider> {
  if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
    return provider.clone();
  }
  let provider = Arc::new(rustls::crypto::ring::default_provider());
  rustls::crypto::CryptoProvider::install_default(Arc::unwrap_or_clone(provider))
    .unwrap_or_else(|_| panic!("a conflicting crypto provider was installed concurrently"));
  rustls::crypto::CryptoProvider::get_default()
    .expect("ring crypto provider installed above")
    .clone()
}

/// Load a certificate chain and private key from PEM files.
fn load_certified_key(cert_path: &str, key_path: &str) -> anyhow::Result<CertifiedKey> {
  let cert_pem = std::fs::read(cert_path)?;
  let key_pem = std::fs::read(key_path)?;

  let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
    .collect::<Result<Vec<_>, io::Error>>()
    .map_err(|e| anyhow::anyhow!("parsing certificates from {cert_path}: {e}"))?;
  if certs.is_empty() {
    anyhow::bail!("no PEM certificates found in {cert_path}");
  }

  let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_pem.as_slice())
    .map_err(|e| anyhow::anyhow!("parsing private key from {key_path}: {e}"))?
    .ok_or_else(|| anyhow::anyhow!("no PEM private key found in {key_path}"))?;

  CertifiedKey::from_der(certs, key, &ring_provider())
    .map_err(|e| anyhow::anyhow!("certificate/key pair is not usable: {e}"))
}

/// Build a rustls [`ServerConfig`] whose certificate is re-read from a shared
/// holder on every new connection.
///
/// # Errors
///
/// Returns an error if the identity or the client CA root cannot be loaded.
pub fn reloading_server_config(
  tls: &TlsConfig,
  resolver: ReloadingCertResolver,
) -> anyhow::Result<ServerConfig> {
  let builder = ServerConfig::builder();

  let builder = match &tls.ca_path {
    None => builder.with_no_client_auth(),
    Some(ca_path) => {
      let ca_pem = std::fs::read(ca_path)?;
      let mut roots = RootCertStore::empty();
      let (added, _) = roots.add_parsable_certificates(
        rustls_pemfile::certs(&mut ca_pem.as_slice())
          .collect::<Result<Vec<_>, io::Error>>()
          .map_err(|e| anyhow::anyhow!("parsing CA certificates from {ca_path}: {e}"))?,
      );
      if added == 0 {
        anyhow::bail!("no PEM certificates found in {ca_path}");
      }
      let verifier = if tls.client_auth_optional {
        WebPkiClientVerifier::builder(roots.into()).allow_unauthenticated()
      } else {
        WebPkiClientVerifier::builder(roots.into())
      }
      .build()
      .map_err(|e| anyhow::anyhow!("building client certificate verifier: {e}"))?;
      builder.with_client_cert_verifier(verifier)
    }
  };

  let mut config = builder.with_cert_resolver(Arc::new(resolver));
  config.alpn_protocols.push(b"h2".to_vec());
  Ok(config)
}

/// Spawn a task that rebuilds the resolver's identity whenever the cert or key
/// file's mtime changes. A rewrite that fails to load is logged and skipped —
/// the previous identity keeps being served.
pub fn watch_identity(
  resolver: ReloadingCertResolver,
  cert_path: String,
  key_path: String,
  interval_secs: u64,
) {
  if interval_secs == 0 {
    return;
  }

  tokio::spawn(async move {
    let paths = [PathBuf::from(&cert_path), PathBuf::from(&key_path)];
    let mut last = mtimes(&paths);
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await;

    loop {
      ticker.tick().await;
      let current = mtimes(&paths);
      if current == last {
        continue;
      }
      match load_certified_key(&cert_path, &key_path) {
        Ok(identity) => {
          resolver.replace(identity);
          info!(cert_path, key_path, "reloaded TLS identity");
          last = current;
        }
        Err(e) => {
          // A partially-written rotation must not take the listener down.
          error!(cert_path, key_path, error = %e, "TLS identity reload failed; keeping previous certificate");
        }
      }
    }
  });
}

/// Pair of file mtimes; `None` entries compare unequal so a missing file
/// triggers a (logging) reload attempt.
fn mtimes(paths: &[PathBuf]) -> [Option<SystemTime>; 2] {
  [
    std::fs::metadata(&paths[0]).and_then(|m| m.modified()).ok(),
    std::fs::metadata(&paths[1]).and_then(|m| m.modified()).ok(),
  ]
}

/// Build a [`TlsAcceptor`] whose served identity follows `resolver`.
pub fn reloading_acceptor(config: ServerConfig) -> TlsAcceptor {
  TlsAcceptor::from(Arc::new(config))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn resolver_serves_replaced_identity() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");

    write_self_signed(&cert_path, &key_path, "first");
    let resolver =
      ReloadingCertResolver::from_paths(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
        .unwrap();

    let first = resolver
      .active
      .read()
      .expect("cert resolver lock poisoned")
      .clone()
      .cert[0]
      .clone();

    write_self_signed(&cert_path, &key_path, "second");
    let reloaded =
      load_certified_key(cert_path.to_str().unwrap(), key_path.to_str().unwrap()).unwrap();
    resolver.replace(reloaded);

    let second = resolver
      .active
      .read()
      .expect("cert resolver lock poisoned")
      .clone()
      .cert[0]
      .clone();
    assert_ne!(first, second);
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
