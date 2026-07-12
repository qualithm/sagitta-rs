//! Embedder-registered DataFusion extensions for the SQL engine.

use std::sync::Arc;

use datafusion::prelude::SessionContext;

/// Registers embedder DataFusion extensions on the engine's [`SessionContext`].
///
/// Implementors register `ScalarUDF`s, table functions, or optimizer rules so
/// they participate in engine-planned SQL, complementing the whole-query
/// [`StatementInterceptor`](crate::StatementInterceptor). Any closure of the
/// form `Fn(&SessionContext) + Send + Sync` implements this trait.
pub trait SessionExtension: Send + Sync {
  /// Apply extensions to `ctx`.
  ///
  /// Called once, when the extension is registered on the engine.
  fn apply(&self, ctx: &SessionContext);
}

impl<F> SessionExtension for F
where
  F: Fn(&SessionContext) + Send + Sync,
{
  fn apply(&self, ctx: &SessionContext) {
    self(ctx)
  }
}

/// Shared handle to a registered [`SessionExtension`].
pub type SharedSessionExtension = Arc<dyn SessionExtension>;
