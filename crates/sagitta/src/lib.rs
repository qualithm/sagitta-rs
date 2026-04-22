//! # sagitta
//!
//! Rust framework for building analytical data services on Arrow Flight and DataFusion.

mod auth;
mod catalog;
mod config;
mod error;
mod memory;
mod metadata;
mod provider;
mod server;
mod service;
mod sql;
mod store;
mod types;

pub use auth::{AccessLevel, AuthToken, InMemoryUserStore, User, UserStore};
pub use config::{Config, LoggingConfig, ServerConfig, TlsConfig};
pub use error::{Error, Result};
pub use memory::MemoryStore;
pub use server::Sagitta;
pub use service::{CustomAction, SagittaService};
pub use sql::{SqlError, SqlResult};
pub use store::{Dataset, Store};
pub use types::{DataPath, FlightDescriptorExt};

/// Internal implementation details.
///
/// Types in this module are not part of the stable public API and may
/// change between minor versions.
pub mod internals {
    pub use crate::catalog::{StoreCatalog, StoreSchema};
    pub use crate::metadata::{DEFAULT_CATALOG, DEFAULT_SCHEMA, MetadataEngine, MetadataQuery};
    pub use crate::provider::StoreTableProvider;
    pub use crate::sql::{EndSavepoint, IsolationLevel, QueryDataStream, SqlEngine};
}
