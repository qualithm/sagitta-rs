//! Pluggable statement interception for the SQL engine.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::sql::SqlResult;

/// The materialized result of an intercepted query.
///
/// Returned by [`StatementInterceptor::intercept_query`] to fully satisfy a
/// `SELECT` outside the default DataFusion path (for example, a time-travel
/// read against historical state).
pub struct QueryInterception {
  /// Arrow schema of the returned batches.
  pub schema: SchemaRef,
  /// The rows to stream back to the client.
  pub batches: Vec<RecordBatch>,
}

/// A hook consulted by the SQL engine before its default statement handling.
///
/// An interceptor lets an embedder own dialect extensions and custom execution
/// without forking the engine. Each method returns `Ok(None)` to fall through
/// to the engine's built-in behavior, or `Ok(Some(..))` to short-circuit it.
#[async_trait]
pub trait StatementInterceptor: Send + Sync {
  /// Consulted before default update (DML/DDL) handling.
  ///
  /// Return `Ok(Some(n))` to report `n` affected rows and skip the engine's own
  /// execution, or `Ok(None)` to let the engine handle the statement.
  ///
  /// # Errors
  ///
  /// Returns an error if the interceptor recognized the statement but failed to
  /// execute it.
  async fn intercept_update(&self, sql: &str) -> SqlResult<Option<i64>>;

  /// Consulted before default query (`SELECT`) handling.
  ///
  /// Return `Ok(Some(interception))` to satisfy the query from the interceptor,
  /// or `Ok(None)` to let DataFusion execute it.
  ///
  /// # Errors
  ///
  /// Returns an error if the interceptor recognized the query but failed to
  /// execute it.
  async fn intercept_query(&self, sql: &str) -> SqlResult<Option<QueryInterception>>;
}

/// Shared handle to a registered [`StatementInterceptor`].
pub type SharedInterceptor = Arc<dyn StatementInterceptor>;
