//! Example: custom Arrow Flight server with a bespoke store, users, and action.

use std::sync::Arc;

use sagitta::InMemoryUserStore;
use sagitta::{AccessLevel, Config, CustomAction, Sagitta, UserStore};
use sagitta::{MemoryStore, Store};
use tonic::Status;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// ---------------------------------------------------------------------------
// Custom action
// ---------------------------------------------------------------------------

/// Reverses the bytes in the request body and returns the result.
struct ReverseAction;

impl CustomAction for ReverseAction {
    fn action_type(&self) -> &str {
        "reverse"
    }

    fn description(&self) -> &str {
        "Reverse the request body bytes."
    }

    fn execute(&self, body: bytes::Bytes) -> Result<Vec<bytes::Bytes>, Status> {
        let mut reversed = body.to_vec();
        reversed.reverse();
        Ok(vec![bytes::Bytes::from(reversed)])
    }
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logging
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    // Storage — pre-populated MemoryStore with test fixtures.
    let store: Arc<dyn Store> = Arc::new(MemoryStore::with_test_fixtures());

    // Users — one admin and one read-only account.
    let mut users = InMemoryUserStore::new();
    users.add_user("alice", "alice-secret", AccessLevel::FullAccess);
    users.add_user("bob", "bob-readonly", AccessLevel::ReadOnly);
    let user_store: Arc<dyn UserStore> = Arc::new(users);

    // Configuration — override the defaults for this example.
    let config = Config::parse_toml(
        r#"
listen_addr = "0.0.0.0:50051"
catalog_name = "warehouse"
default_schema = "public"
"#,
    )?;

    // Build and run.
    Sagitta::builder()
        .config(config)
        .store(store)
        .user_store(user_store)
        .action(Arc::new(ReverseAction))
        .serve()
        .await
}
