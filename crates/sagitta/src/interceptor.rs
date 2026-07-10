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

/// Request context describing the caller that issued an intercepted statement.
///
/// Passed to the `*_with_context` interceptor methods so an embedder can
/// attribute a statement to the authenticated caller (for example, to record an
/// audit principal). The struct is `#[non_exhaustive]`: construct it with
/// [`InterceptContext::default`] and set the fields you need, so new fields can
/// be added without breaking embedders.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct InterceptContext {
  /// Authenticated principal (username) that issued the statement, if any.
  ///
  /// `None` when the statement was issued anonymously or the engine is invoked
  /// outside an authenticated request.
  pub principal: Option<String>,
  /// Whether the caller is limited to read-only access.
  pub read_only: bool,
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

  /// Context-aware variant of [`intercept_update`](Self::intercept_update).
  ///
  /// The SQL engine calls this method, passing the [`InterceptContext`] for the
  /// request that issued `sql`. The default implementation ignores the context
  /// and delegates to [`intercept_update`](Self::intercept_update), so existing
  /// interceptors keep working unchanged; override it to read the caller.
  ///
  /// # Errors
  ///
  /// Returns an error if the interceptor recognized the statement but failed to
  /// execute it.
  async fn intercept_update_with_context(
    &self,
    ctx: &InterceptContext,
    sql: &str,
  ) -> SqlResult<Option<i64>> {
    let _ = ctx;
    self.intercept_update(sql).await
  }

  /// Context-aware variant of [`intercept_query`](Self::intercept_query).
  ///
  /// The SQL engine calls this method, passing the [`InterceptContext`] for the
  /// request that issued `sql`. The default implementation ignores the context
  /// and delegates to [`intercept_query`](Self::intercept_query), so existing
  /// interceptors keep working unchanged; override it to read the caller.
  ///
  /// # Errors
  ///
  /// Returns an error if the interceptor recognized the query but failed to
  /// execute it.
  async fn intercept_query_with_context(
    &self,
    ctx: &InterceptContext,
    sql: &str,
  ) -> SqlResult<Option<QueryInterception>> {
    let _ = ctx;
    self.intercept_query(sql).await
  }
}

/// Shared handle to a registered [`StatementInterceptor`].
pub type SharedInterceptor = Arc<dyn StatementInterceptor>;
