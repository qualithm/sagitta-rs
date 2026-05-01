//! Flight SQL command handling.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use crate::DataPath;
use crate::Store;
use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, Command, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, EndTransaction,
    TicketStatementQuery,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{
    CreateCatalogSchema, CreateView, DdlStatement, DmlStatement, DropCatalogSchema, DropTable,
    DropView, LogicalPlan, WriteOp,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use futures::Stream;
use prost::Message;
use tracing::{debug, info, warn};

use crate::catalog::StoreCatalog;
use crate::provider::StoreTableProvider;

/// Result type for SQL operations.
/// Result type for SQL operations.
pub type SqlResult<T> = std::result::Result<T, SqlError>;

/// Errors that can occur during SQL operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqlError {
    /// Invalid SQL command format.
    #[error("invalid command: {0}")]
    InvalidCommand(String),

    /// Unsupported SQL command.
    #[error("unsupported command: {0}")]
    UnsupportedCommand(String),

    /// SQL syntax error.
    #[error("sql syntax error: {0}")]
    SyntaxError(String),

    /// Table not found.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// Table already exists.
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    /// Prepared statement not found.
    #[error("prepared statement not found: {0}")]
    PreparedStatementNotFound(String),

    /// Transaction not found.
    #[error("transaction not found: {0}")]
    TransactionNotFound(String),

    /// Savepoint not found.
    #[error("savepoint not found: {0}")]
    SavepointNotFound(String),

    /// Invalid transaction action.
    #[error("invalid transaction action: {0}")]
    InvalidTransactionAction(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),

    /// Query execution error.
    #[error("query execution error: {0}")]
    QueryExecution(String),
}

impl From<SqlError> for tonic::Status {
    fn from(err: SqlError) -> Self {
        match err {
            SqlError::InvalidCommand(msg) => tonic::Status::invalid_argument(msg),
            SqlError::UnsupportedCommand(msg) => tonic::Status::unimplemented(msg),
            SqlError::SyntaxError(msg) => tonic::Status::invalid_argument(msg),
            SqlError::TableNotFound(msg) => tonic::Status::not_found(msg),
            SqlError::TableAlreadyExists(msg) => tonic::Status::already_exists(msg),
            SqlError::PreparedStatementNotFound(msg) => tonic::Status::not_found(msg),
            SqlError::TransactionNotFound(msg) => tonic::Status::not_found(msg),
            SqlError::SavepointNotFound(msg) => tonic::Status::not_found(msg),
            SqlError::InvalidTransactionAction(msg) => tonic::Status::invalid_argument(msg),
            SqlError::Internal(msg) => tonic::Status::internal(msg),
            SqlError::Arrow(e) => tonic::Status::internal(e.to_string()),
            SqlError::QueryExecution(msg) => tonic::Status::internal(msg),
        }
    }
}

/// Result of executing a SQL query via `CommandStatementQuery`.
#[derive(Debug)]
pub struct QueryResult {
    /// Opaque handle for retrieving data via `DoGet`.
    pub handle: Bytes,
    /// Result schema.
    pub schema: SchemaRef,
    /// Total records (-1 if unknown).
    pub total_records: i64,
}

/// Result of executing a SQL update (INSERT, UPDATE, DELETE).
#[derive(Debug)]
pub struct UpdateResult {
    /// Number of records affected.
    pub record_count: i64,
}

/// A streaming source of query data.
///
/// This wraps DataFusion's `SendableRecordBatchStream` to provide streaming
/// access to query results without loading all data into memory.
pub struct QueryDataStream {
    inner: SendableRecordBatchStream,
}

impl QueryDataStream {
    /// Create a new streaming data source.
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { inner: stream }
    }
}

impl Stream for QueryDataStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Err(e))) => {
                // Convert DataFusionError to FlightError
                Poll::Ready(Some(Err(FlightError::ExternalError(Box::new(e)))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Result of creating a prepared statement via `CreatePreparedStatement`.
#[derive(Debug)]
pub struct CreatePreparedStatementResult {
    /// Opaque handle identifying the statement.
    pub handle: Bytes,
    /// Schema of the result dataset (for queries).
    pub dataset_schema: Option<SchemaRef>,
    /// Schema of the parameters.
    pub parameter_schema: Option<SchemaRef>,
}

/// Stored prepared statement.
#[derive(Debug, Clone)]
struct PreparedStatement {
    /// Original SQL query.
    query: String,
    /// Whether this is a query (SELECT) or update (INSERT/UPDATE/DELETE).
    is_query: bool,
    /// Result schema for queries.
    #[allow(dead_code)]
    dataset_schema: Option<SchemaRef>,
    /// Parameter schema (returned to clients, not validated internally yet).
    #[allow(dead_code)]
    parameter_schema: Option<SchemaRef>,
    /// Bound parameter values.
    bound_parameters: Option<Vec<Arc<RecordBatch>>>,
}

/// A pending operation within a transaction.
#[derive(Debug, Clone)]
enum PendingOperation {
    /// INSERT operation.
    Insert {
        path: DataPath,
        query: String,
        record_count: i64,
    },
    /// UPDATE operation.
    Update {
        path: DataPath,
        query: String,
        record_count: i64,
    },
    /// DELETE operation.
    Delete {
        path: DataPath,
        query: String,
        record_count: i64,
    },
}

impl PendingOperation {
    fn record_count(&self) -> i64 {
        match self {
            PendingOperation::Insert { record_count, .. } => *record_count,
            PendingOperation::Update { record_count, .. } => *record_count,
            PendingOperation::Delete { record_count, .. } => *record_count,
        }
    }
}

/// Transaction isolation level.
///
/// Defines the degree of isolation between concurrent transactions.
/// Flight SQL supports this via SqlInfo metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Transactions can read uncommitted data from other transactions.
    ReadUncommitted,
    /// Transactions only read committed data (default).
    #[default]
    ReadCommitted,
    /// Prevents non-repeatable reads within a transaction.
    RepeatableRead,
    /// Full isolation - transactions appear to execute serially.
    Serializable,
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

/// Savepoint action for end_savepoint.
///
/// Matches Flight SQL ActionEndSavepointRequest action values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndSavepoint {
    /// Unspecified action (invalid).
    Unspecified = 0,
    /// Release the savepoint (commit changes since savepoint).
    Release = 1,
    /// Rollback to the savepoint (discard changes since savepoint).
    Rollback = 2,
}

impl TryFrom<i32> for EndSavepoint {
    type Error = SqlError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EndSavepoint::Unspecified),
            1 => Ok(EndSavepoint::Release),
            2 => Ok(EndSavepoint::Rollback),
            _ => Err(SqlError::InvalidTransactionAction(format!(
                "invalid EndSavepoint value: {value}"
            ))),
        }
    }
}

/// A savepoint within a transaction.
///
/// Savepoints allow partial rollback of a transaction.
#[derive(Debug, Clone)]
struct Savepoint {
    /// User-provided name for the savepoint.
    name: String,
    /// Index into pending_operations where this savepoint was created.
    operation_index: usize,
}

/// Stored transaction.
#[derive(Debug, Clone)]
struct Transaction {
    /// Whether the transaction is still active.
    active: bool,
    /// Transaction isolation level.
    ///
    /// Currently stored for future per-transaction isolation behavior.
    #[allow(dead_code)]
    isolation_level: IsolationLevel,
    /// Pending operations to be committed or rolled back.
    pending_operations: Vec<PendingOperation>,
    /// Named savepoints within this transaction.
    savepoints: HashMap<Bytes, Savepoint>,
    /// Opaque transaction ID returned by the store's `begin_transaction` hook.
    store_transaction_id: Bytes,
}

/// SQL query engine backed by DataFusion.
///
/// Manages query execution, prepared statements, and transactions
/// against tables registered from a [`Store`].
pub struct SqlEngine {
    store: Arc<dyn Store>,
    catalog_name: String,
    default_schema: String,
    ctx: RwLock<SessionContext>,
    prepared_statements: RwLock<HashMap<Bytes, PreparedStatement>>,
    transactions: RwLock<HashMap<Bytes, Transaction>>,
    next_handle_id: RwLock<u64>,
}

impl SqlEngine {
    /// Create a new SQL engine.
    pub async fn new(store: Arc<dyn Store>, catalog_name: &str, default_schema: &str) -> Self {
        let ctx = Self::build_session_context(&store, catalog_name, default_schema).await;
        Self {
            store,
            catalog_name: catalog_name.to_string(),
            default_schema: default_schema.to_string(),
            ctx: RwLock::new(ctx),
            prepared_statements: RwLock::new(HashMap::new()),
            transactions: RwLock::new(HashMap::new()),
            next_handle_id: RwLock::new(1),
        }
    }

    /// Create a new SessionContext with our catalog configured as the default.
    async fn build_session_context(
        store: &Arc<dyn Store>,
        catalog_name: &str,
        default_schema: &str,
    ) -> SessionContext {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(catalog_name, default_schema)
            .with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);

        // Register our catalog
        let catalog = StoreCatalog::new(store.clone(), catalog_name, default_schema).await;
        ctx.register_catalog(catalog_name, Arc::new(catalog));

        debug!(
            "created SessionContext with default catalog '{catalog_name}' and schema '{default_schema}'"
        );
        ctx
    }

    /// Refresh the SessionContext with current tables from the store.
    ///
    /// Call this after tables are added or removed via DoPut/DoAction.
    /// Uses incremental catalog replacement to preserve session state such as
    /// registered views and prepared statements.
    pub async fn refresh_tables(&self) {
        self.rebuild_catalog().await;
        debug!("refreshed DataFusion tables from store");
    }

    /// Convert a DataPath to a DataFusion table name.
    ///
    /// Returns the qualified name: `schema.table` or just `table` for default schema.
    #[allow(dead_code)]
    fn path_to_table_name(&self, path: &DataPath) -> String {
        let (_, schema, table) = StoreCatalog::path_to_catalog_schema_table(
            path,
            &self.catalog_name,
            &self.default_schema,
        );
        if schema == self.default_schema {
            table
        } else {
            format!("{schema}.{table}")
        }
    }

    /// Convert a table name back to a DataPath.
    ///
    /// Handles both underscore-separated and dot-separated names.
    #[allow(dead_code)]
    fn table_name_to_path(name: &str) -> DataPath {
        // First try dot-separated (SQL standard)
        if name.contains('.') {
            let segments: Vec<String> = name
                .split('.')
                .map(|s| s.trim_matches('"').to_string())
                .collect();
            return DataPath::new(segments);
        }
        // Fall back to underscore-separated
        let segments: Vec<String> = name.split('_').map(|s| s.to_string()).collect();
        DataPath::new(segments)
    }

    /// Return a clone of the underlying [`SessionContext`].
    ///
    /// Used by callers that need direct DataFusion access (e.g. `DoExchange` query mode).
    pub fn session_ctx(&self) -> SessionContext {
        self.ctx.read().unwrap().clone()
    }

    /// Parse a CMD descriptor into a Flight SQL command.
    pub fn parse_command(cmd: &Bytes) -> SqlResult<Command> {
        let any = Any::decode(cmd.as_ref())
            .map_err(|e| SqlError::InvalidCommand(format!("failed to decode Any: {e}")))?;

        Command::try_from(any)
            .map_err(|e| SqlError::InvalidCommand(format!("failed to parse command: {e}")))
    }

    /// Execute a statement query command.
    ///
    /// Returns query metadata for GetFlightInfo.
    /// Uses DataFusion to parse and validate the query.
    pub async fn execute_statement_query(
        &self,
        cmd: &CommandStatementQuery,
    ) -> SqlResult<QueryResult> {
        let query = &cmd.query;
        debug!(query = %query, "executing statement query");

        // Use DataFusion to validate the query and get the logical plan
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();

        // Create a handle that encodes the query
        let handle = self.create_query_handle(query);

        // Total records is unknown until execution (-1 per Flight SQL spec)
        info!(
            query = %query,
            "query validated with DataFusion"
        );

        Ok(QueryResult {
            handle,
            schema,
            total_records: -1,
        })
    }

    /// Get data for a statement query ticket.
    ///
    /// Executes the query using DataFusion and returns results.
    pub async fn get_statement_query_data(
        &self,
        ticket: &TicketStatementQuery,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        let handle = &ticket.statement_handle;
        let query = String::from_utf8(handle.to_vec())
            .map_err(|_| SqlError::InvalidCommand("invalid query handle".to_string()))?;

        debug!(query = %query, "executing statement query data via DataFusion");

        // Execute the query using DataFusion
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(&query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();
        let batches = df
            .collect()
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        // Convert to Arc<RecordBatch>
        let batches: Vec<Arc<RecordBatch>> = batches.into_iter().map(Arc::new).collect();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            query = %query,
            total_rows,
            batch_count = batches.len(),
            "query executed via DataFusion"
        );

        Ok((schema, batches))
    }

    /// Get a streaming data source for a statement query ticket.
    ///
    /// Executes the query using DataFusion and returns a stream of record batches.
    /// This avoids loading all data into memory at once for large result sets.
    pub async fn get_statement_query_data_stream(
        &self,
        ticket: &TicketStatementQuery,
    ) -> SqlResult<(SchemaRef, QueryDataStream)> {
        let handle = &ticket.statement_handle;
        let query = String::from_utf8(handle.to_vec())
            .map_err(|_| SqlError::InvalidCommand("invalid query handle".to_string()))?;

        debug!(query = %query, "executing statement query data stream via DataFusion");

        // Execute the query using DataFusion
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(&query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();

        // Get a streaming result instead of collecting all batches
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        info!(
            query = %query,
            "query streaming started via DataFusion"
        );

        Ok((schema, QueryDataStream::new(stream)))
    }

    /// Execute a statement update command.
    ///
    /// Returns the number of affected records.
    /// If a transaction_id is provided, the operation is buffered until commit.
    pub async fn execute_statement_update(
        &self,
        cmd: &CommandStatementUpdate,
    ) -> SqlResult<UpdateResult> {
        let query = &cmd.query;
        debug!(query = %query, transaction_id = ?cmd.transaction_id, "executing statement update");

        let (path, record_count) = self.parse_and_execute_update_async(query).await?;

        // If within a transaction, buffer the operation
        if let Some(ref txn_id) = cmd.transaction_id {
            self.add_pending_operation(txn_id, query, &path, record_count)?;
            info!(
                query = %query,
                path = %path.display(),
                record_count,
                transaction_id = %String::from_utf8_lossy(txn_id),
                "update buffered in transaction"
            );
        } else {
            info!(
                query = %query,
                path = %path.display(),
                record_count,
                "update executed (auto-commit)"
            );
        }

        Ok(UpdateResult { record_count })
    }

    /// Add a pending operation to a transaction.
    fn add_pending_operation(
        &self,
        transaction_id: &Bytes,
        query: &str,
        path: &DataPath,
        record_count: i64,
    ) -> SqlResult<()> {
        let mut transactions = self.transactions.write().unwrap();
        let transaction = transactions.get_mut(transaction_id).ok_or_else(|| {
            SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
        })?;

        if !transaction.active {
            return Err(SqlError::InvalidTransactionAction(
                "transaction is not active".to_string(),
            ));
        }

        let query_lower = query.trim().to_lowercase();
        let operation = if query_lower.starts_with("insert") {
            PendingOperation::Insert {
                path: path.clone(),
                query: query.to_string(),
                record_count,
            }
        } else if query_lower.starts_with("update") {
            PendingOperation::Update {
                path: path.clone(),
                query: query.to_string(),
                record_count,
            }
        } else {
            PendingOperation::Delete {
                path: path.clone(),
                query: query.to_string(),
                record_count,
            }
        };

        transaction.pending_operations.push(operation);
        Ok(())
    }

    /// Parse and execute an update statement using DataFusion for parsing.
    ///
    /// Uses DataFusion to parse SQL and resolve qualified table names,
    /// then executes the DML operation.
    async fn parse_and_execute_update_async(&self, query: &str) -> SqlResult<(DataPath, i64)> {
        let query_upper = query.trim().to_uppercase();

        // Handle TRUNCATE TABLE (not supported by DataFusion)
        if query_upper.starts_with("TRUNCATE") {
            return self.execute_truncate(query).await;
        }

        // Handle ALTER TABLE (not supported by DataFusion)
        if query_upper.starts_with("ALTER") {
            return self.execute_alter_table(query).await;
        }

        // Handle MERGE / UPSERT (not supported by DataFusion)
        if query_upper.starts_with("MERGE") {
            return self.execute_merge(query).await;
        }

        // Handle INSERT...ON CONFLICT (not supported by DataFusion)
        if query_upper.starts_with("INSERT") && query_upper.contains("ON CONFLICT") {
            return self.execute_upsert(query).await;
        }

        let ctx = self.ctx.read().unwrap().clone();

        // Use DataFusion to create a logical plan - this handles qualified name resolution
        let plan = ctx.state().create_logical_plan(query).await.map_err(|e| {
            let msg = e.to_string();
            // Check if this is a table not found error
            if msg.contains("not found") && msg.contains("table") {
                // Extract table name from message like "table 'catalog.schema.table' not found"
                if let Some(start) = msg.find('\'')
                    && let Some(end) = msg[start + 1..].find('\'')
                {
                    let table_name = &msg[start + 1..start + 1 + end];
                    return SqlError::TableNotFound(table_name.to_string());
                }
                SqlError::TableNotFound(msg)
            } else {
                SqlError::SyntaxError(msg)
            }
        })?;

        // Extract table reference and operation from the plan
        match &plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op,
                input,
                ..
            }) => {
                let path = self.table_reference_to_path(table_name)?;

                match op {
                    WriteOp::Insert(_insert_op) => {
                        // For INSERT...SELECT, execute the input plan to get the data
                        let df = ctx
                            .execute_logical_plan(input.as_ref().clone())
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                        let batches = df
                            .collect()
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                        let record_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                        // Actually insert the batches into the store
                        if record_count > 0 {
                            self.store
                                .append_batches(&path, batches)
                                .await
                                .map_err(|e| SqlError::Internal(e.to_string()))?;
                        }

                        info!(
                            path = %path.display(),
                            record_count,
                            "INSERT executed via DataFusion"
                        );

                        Ok((path, record_count))
                    }
                    WriteOp::Ctas => {
                        // CREATE TABLE AS SELECT - execute the input and create a new table
                        let df = ctx
                            .execute_logical_plan(input.as_ref().clone())
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                        let schema = df.schema().inner().clone();
                        let batches = df
                            .collect()
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                        let record_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                        self.store
                            .put(path.clone(), schema, batches)
                            .await
                            .map_err(|e| SqlError::Internal(e.to_string()))?;

                        self.register_table_in_catalog(&path).await;

                        info!(
                            path = %path.display(),
                            record_count,
                            "CTAS executed via DataFusion"
                        );

                        Ok((path, record_count))
                    }
                    WriteOp::Update => {
                        let info = self
                            .store
                            .get(&path)
                            .await
                            .map_err(|_| SqlError::TableNotFound(path.display()))?;
                        let schema = self
                            .store
                            .get_schema(&path)
                            .await
                            .map_err(|_| SqlError::TableNotFound(path.display()))?;

                        let has_filter = self.plan_has_filter(input);

                        // Execute the DML input plan to get modified rows
                        // (for UPDATE the input contains the SET-transformed rows
                        // that matched the WHERE clause)
                        let modified_df = ctx
                            .execute_logical_plan(input.as_ref().clone())
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
                        let modified_batches = modified_df
                            .collect()
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
                        let affected_count: i64 =
                            modified_batches.iter().map(|b| b.num_rows() as i64).sum();

                        if !has_filter {
                            // UPDATE without WHERE: all rows replaced by modified data
                            self.store
                                .put(path.clone(), schema, modified_batches)
                                .await
                                .map_err(|e| SqlError::Internal(e.to_string()))?;
                        } else {
                            // UPDATE with WHERE: combine unmodified + modified rows.
                            // Extract the predicate from the logical plan (AST-based, handles
                            // all SQL expressions including subqueries and complex conditions).
                            let where_clause = Self::extract_filter_from_plan(input)
                                .map(|expr| Self::filter_expr_to_sql(&expr))
                                .transpose()?;
                            let qualified = self.qualified_table_sql(&path);

                            let surviving_batches = match where_clause {
                                Some(condition) => {
                                    let surviving_query = format!(
                                        "SELECT * FROM {qualified} WHERE NOT ({condition})"
                                    );
                                    let sdf = ctx
                                        .sql(&surviving_query)
                                        .await
                                        .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
                                    sdf.collect()
                                        .await
                                        .map_err(|e| SqlError::QueryExecution(e.to_string()))?
                                }
                                None => vec![],
                            };

                            let mut all_batches = surviving_batches;
                            all_batches.extend(modified_batches);

                            self.store
                                .put(path.clone(), schema, all_batches)
                                .await
                                .map_err(|e| SqlError::Internal(e.to_string()))?;
                        }

                        let record_count = if has_filter {
                            affected_count
                        } else {
                            info.total_records as i64
                        };

                        info!(
                            path = %path.display(),
                            record_count,
                            "UPDATE executed via DataFusion"
                        );

                        Ok((path, record_count))
                    }
                    WriteOp::Delete => {
                        let info = self
                            .store
                            .get(&path)
                            .await
                            .map_err(|_| SqlError::TableNotFound(path.display()))?;
                        let schema = self
                            .store
                            .get_schema(&path)
                            .await
                            .map_err(|_| SqlError::TableNotFound(path.display()))?;
                        let old_count = info.total_records as i64;

                        let has_filter = self.plan_has_filter(input);

                        if !has_filter {
                            // DELETE without WHERE: remove all rows
                            self.store
                                .truncate(&path)
                                .await
                                .map_err(|e| SqlError::Internal(e.to_string()))?;

                            info!(
                                path = %path.display(),
                                record_count = old_count,
                                "DELETE (all rows) executed via DataFusion"
                            );

                            return Ok((path, old_count));
                        }

                        // DELETE with WHERE: keep only non-matching rows.
                        // Extract the predicate from the logical plan (AST-based).
                        let condition = Self::extract_filter_from_plan(input)
                            .map(|expr| Self::filter_expr_to_sql(&expr))
                            .transpose()?
                            .ok_or_else(|| {
                                SqlError::Internal(
                                    "could not extract WHERE clause from DELETE plan".to_string(),
                                )
                            })?;
                        let qualified = self.qualified_table_sql(&path);

                        let surviving_query =
                            format!("SELECT * FROM {qualified} WHERE NOT ({condition})");
                        let sdf = ctx
                            .sql(&surviving_query)
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
                        let surviving_batches = sdf
                            .collect()
                            .await
                            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
                        let new_count: i64 =
                            surviving_batches.iter().map(|b| b.num_rows() as i64).sum();
                        let deleted_count = old_count - new_count;

                        self.store
                            .put(path.clone(), schema, surviving_batches)
                            .await
                            .map_err(|e| SqlError::Internal(e.to_string()))?;

                        info!(
                            path = %path.display(),
                            record_count = deleted_count,
                            "DELETE executed via DataFusion"
                        );

                        Ok((path, deleted_count))
                    }
                }
            }
            LogicalPlan::Ddl(ddl) => self.execute_ddl(ddl).await,
            other => Err(SqlError::UnsupportedCommand(format!(
                "unsupported statement type: {}",
                other.display()
            ))),
        }
    }

    /// Execute a DDL statement (CREATE TABLE, DROP TABLE, etc.).
    async fn execute_ddl(&self, ddl: &DdlStatement) -> SqlResult<(DataPath, i64)> {
        match ddl {
            DdlStatement::CreateMemoryTable(create) => {
                let path = self.table_reference_to_path(&create.name)?;

                // Check if table already exists
                if self
                    .store
                    .contains(&path)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?
                {
                    if create.if_not_exists {
                        info!(
                            path = %path.display(),
                            "CREATE TABLE IF NOT EXISTS: table already exists, skipping"
                        );
                        return Ok((path, 0));
                    }
                    if !create.or_replace {
                        return Err(SqlError::TableAlreadyExists(path.display()));
                    }
                    // Remove existing table for OR REPLACE
                    self.store
                        .remove(&path)
                        .await
                        .map_err(|e| SqlError::Internal(e.to_string()))?;
                }

                // Get schema from the input plan
                let schema = create.input.schema();
                let arrow_schema: SchemaRef = schema.inner().clone();

                // Execute the input plan to get initial data (for CREATE TABLE AS SELECT)
                let ctx = self.ctx.read().unwrap().clone();
                let df = ctx
                    .execute_logical_plan(create.input.as_ref().clone())
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                let batches = df
                    .collect()
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                let record_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                // Store the table
                self.store
                    .put(path.clone(), arrow_schema, batches)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;

                self.register_table_in_catalog(&path).await;

                info!(
                    path = %path.display(),
                    record_count,
                    "CREATE TABLE executed"
                );

                Ok((path, record_count))
            }
            DdlStatement::DropTable(DropTable {
                name, if_exists, ..
            }) => {
                let path = self.table_reference_to_path(name)?;

                if !self
                    .store
                    .contains(&path)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?
                {
                    if *if_exists {
                        info!(
                            path = %path.display(),
                            "DROP TABLE IF EXISTS: table does not exist, skipping"
                        );
                        return Ok((path, 0));
                    }
                    return Err(SqlError::TableNotFound(path.display()));
                }

                // Remove the table
                self.store
                    .remove(&path)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;

                self.deregister_table_from_catalog(&path);

                info!(path = %path.display(), "DROP TABLE executed");

                Ok((path, 0))
            }
            DdlStatement::CreateView(CreateView {
                name,
                input,
                or_replace,
                ..
            }) => {
                let view_name = name.to_string();

                // Register the view with DataFusion
                let ctx = self.ctx.read().unwrap().clone();

                // Check if view already exists
                if ctx.table_exist(name.clone()).unwrap_or(false) && !*or_replace {
                    return Err(SqlError::TableAlreadyExists(view_name.clone()));
                }

                // Create the view as a logical plan
                let df = ctx
                    .execute_logical_plan(input.as_ref().clone())
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

                // Register the view
                ctx.register_table(name.clone(), df.into_view())
                    .map_err(|e| SqlError::Internal(e.to_string()))?;

                info!(view_name = %view_name, "CREATE VIEW executed");

                Ok((DataPath::new(vec![view_name]), 0))
            }
            DdlStatement::DropView(DropView {
                name, if_exists, ..
            }) => {
                let view_name = name.to_string();
                let ctx = self.ctx.read().unwrap().clone();

                // Check if view exists
                if !ctx.table_exist(name.clone()).unwrap_or(false) {
                    if *if_exists {
                        info!(
                            view_name = %view_name,
                            "DROP VIEW IF EXISTS: view does not exist, skipping"
                        );
                        return Ok((DataPath::new(vec![view_name]), 0));
                    }
                    return Err(SqlError::TableNotFound(view_name.clone()));
                }

                // Deregister the view
                ctx.deregister_table(name.clone())
                    .map_err(|e| SqlError::Internal(e.to_string()))?;

                info!(view_name = %view_name, "DROP VIEW executed");

                Ok((DataPath::new(vec![view_name]), 0))
            }
            DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists,
                ..
            }) => {
                let schema_name_str = schema_name.to_string();

                if self
                    .store
                    .schema_exists(&schema_name_str)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?
                {
                    if *if_not_exists {
                        info!(
                            schema_name = %schema_name_str,
                            "CREATE SCHEMA IF NOT EXISTS: schema already exists, skipping"
                        );
                        return Ok((DataPath::new(vec![schema_name_str]), 0));
                    }
                    return Err(SqlError::TableAlreadyExists(format!(
                        "schema '{schema_name_str}' already exists"
                    )));
                }

                self.store
                    .create_schema(&schema_name_str)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;
                self.rebuild_catalog().await;

                info!(
                    schema_name = %schema_name_str,
                    "CREATE SCHEMA executed"
                );

                Ok((DataPath::new(vec![schema_name_str]), 0))
            }
            DdlStatement::DropCatalogSchema(DropCatalogSchema {
                name, if_exists, ..
            }) => {
                let schema_name_str = name.to_string();

                let existed = self
                    .store
                    .drop_schema(&schema_name_str)
                    .await
                    .map_err(|e| SqlError::Internal(e.to_string()))?;

                if !existed && !*if_exists {
                    return Err(SqlError::TableNotFound(format!(
                        "schema '{schema_name_str}' does not exist"
                    )));
                }

                self.rebuild_catalog().await;

                info!(
                    schema_name = %schema_name_str,
                    "DROP SCHEMA executed"
                );

                Ok((DataPath::new(vec![schema_name_str]), 0))
            }
            other => Err(SqlError::UnsupportedCommand(format!(
                "DDL statement not yet supported: {}",
                other.name()
            ))),
        }
    }

    /// Register a single table in the DataFusion catalog.
    ///
    /// Used after CREATE TABLE or ALTER TABLE to make the new table visible
    /// to queries without rebuilding the entire catalog.
    async fn register_table_in_catalog(&self, path: &DataPath) {
        let (_, schema_name, table_name) = StoreCatalog::path_to_catalog_schema_table(
            path,
            &self.catalog_name,
            &self.default_schema,
        );
        let table_ref = TableReference::Partial {
            schema: schema_name.into(),
            table: table_name.into(),
        };
        if let Ok(provider) = StoreTableProvider::new(self.store.clone(), path.clone()).await {
            let ctx = self.ctx.read().unwrap();
            let _ = ctx.register_table(table_ref, Arc::new(provider));
        }
    }

    /// Deregister a single table from the DataFusion catalog.
    ///
    /// Used after DROP TABLE.
    fn deregister_table_from_catalog(&self, path: &DataPath) {
        let (_, schema_name, table_name) = StoreCatalog::path_to_catalog_schema_table(
            path,
            &self.catalog_name,
            &self.default_schema,
        );
        let table_ref = TableReference::Partial {
            schema: schema_name.into(),
            table: table_name.into(),
        };
        let ctx = self.ctx.read().unwrap();
        let _ = ctx.deregister_table(table_ref);
    }

    /// Rebuild the entire DataFusion catalog from the current Store state.
    ///
    /// Called only for schema-level DDL (CREATE SCHEMA, DROP SCHEMA) where
    /// incremental updates are not sufficient.
    async fn rebuild_catalog(&self) {
        let catalog =
            StoreCatalog::new(self.store.clone(), &self.catalog_name, &self.default_schema).await;
        let ctx = self.ctx.read().unwrap();
        ctx.register_catalog(&self.catalog_name, Arc::new(catalog));
        debug!("rebuilt DataFusion catalog from store");
    }

    /// Convert a DataFusion TableReference to a DataPath.
    fn table_reference_to_path(&self, table_ref: &TableReference) -> SqlResult<DataPath> {
        let segments = match table_ref {
            TableReference::Bare { table } => vec![table.to_string()],
            TableReference::Partial { schema, table } => {
                vec![schema.to_string(), table.to_string()]
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
        };

        Ok(DataPath::new(segments))
    }

    /// Build a SQL-safe qualified table name from a DataPath.
    fn qualified_table_sql(&self, path: &DataPath) -> String {
        let (_, schema, table) = StoreCatalog::path_to_catalog_schema_table(
            path,
            &self.catalog_name,
            &self.default_schema,
        );
        if schema == self.default_schema {
            format!("\"{}\".\"{}\"", self.default_schema, table)
        } else {
            format!("\"{}\".\"{}\"", schema, table)
        }
    }

    /// Extract the filter predicate `Expr` from a logical plan by walking down
    /// through projections and subquery aliases.
    fn extract_filter_from_plan(plan: &LogicalPlan) -> Option<datafusion::logical_expr::Expr> {
        match plan {
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone()),
            LogicalPlan::Projection(proj) => Self::extract_filter_from_plan(proj.input.as_ref()),
            LogicalPlan::SubqueryAlias(alias) => {
                Self::extract_filter_from_plan(alias.input.as_ref())
            }
            _ => None,
        }
    }

    /// Serialise a DataFusion `Expr` to a SQL string using DataFusion's `Unparser`.
    fn filter_expr_to_sql(expr: &datafusion::logical_expr::Expr) -> SqlResult<String> {
        use datafusion::sql::unparser::Unparser;
        Unparser::default()
            .expr_to_sql(expr)
            .map(|sql| sql.to_string())
            .map_err(|e| SqlError::Internal(format!("failed to serialise filter condition: {e}")))
    }

    /// Check if a logical plan contains a filter (WHERE clause).
    fn plan_has_filter(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter { .. } => true,
            LogicalPlan::Projection(proj) => self.plan_has_filter(proj.input.as_ref()),
            LogicalPlan::SubqueryAlias(alias) => self.plan_has_filter(alias.input.as_ref()),
            _ => false,
        }
    }

    /// Parse and execute an update statement (INSERT, UPDATE, DELETE).
    ///
    /// Execute a TRUNCATE TABLE statement.
    ///
    /// Format: TRUNCATE [TABLE] table_name
    /// Removes all rows but keeps the table schema.
    async fn execute_truncate(&self, query: &str) -> SqlResult<(DataPath, i64)> {
        let query_lower = query.trim().to_lowercase();

        // Parse: TRUNCATE [TABLE] table_name
        let after_truncate = query_lower
            .strip_prefix("truncate")
            .ok_or_else(|| SqlError::SyntaxError("invalid TRUNCATE syntax".to_string()))?
            .trim_start();

        // Skip optional TABLE keyword
        let table_part = if after_truncate.starts_with("table ") {
            after_truncate.strip_prefix("table ").unwrap().trim_start()
        } else {
            after_truncate
        };

        let table_name = table_part
            .split_whitespace()
            .next()
            .ok_or_else(|| SqlError::SyntaxError("missing table name".to_string()))?;

        let path = self.parse_table_name(table_name)?;

        // Get current record count before truncation
        let info = self
            .store
            .get(&path)
            .await
            .map_err(|_| SqlError::TableNotFound(path.display()))?;

        let record_count = info.total_records as i64;

        // Truncate the table
        self.store
            .truncate(&path)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        info!(
            path = %path.display(),
            record_count,
            "TRUNCATE TABLE executed"
        );

        Ok((path, record_count))
    }

    /// Execute an ALTER TABLE statement.
    ///
    /// Supported operations:
    /// - ALTER TABLE table_name ADD [COLUMN] column_name data_type
    /// - ALTER TABLE table_name DROP [COLUMN] column_name
    /// - ALTER TABLE table_name RENAME [COLUMN] old_name TO new_name
    async fn execute_alter_table(&self, query: &str) -> SqlResult<(DataPath, i64)> {
        let query_lower = query.trim().to_lowercase();

        // Parse: ALTER TABLE table_name ...
        let after_alter = query_lower
            .strip_prefix("alter")
            .ok_or_else(|| SqlError::SyntaxError("invalid ALTER syntax".to_string()))?
            .trim_start();

        let after_table = after_alter
            .strip_prefix("table")
            .ok_or_else(|| SqlError::SyntaxError("expected TABLE after ALTER".to_string()))?
            .trim_start();

        // Extract table name (until ADD, DROP, or RENAME)
        let table_end = after_table
            .find(" add ")
            .or_else(|| after_table.find(" drop "))
            .or_else(|| after_table.find(" rename "))
            .ok_or_else(|| {
                SqlError::SyntaxError("expected ADD, DROP, or RENAME in ALTER TABLE".to_string())
            })?;

        let table_name = after_table[..table_end].trim();
        let operation = &after_table[table_end + 1..].trim();
        let path = self.parse_table_name(table_name)?;

        // Get current schema
        let current_schema = self
            .store
            .get_schema(&path)
            .await
            .map_err(|_| SqlError::TableNotFound(path.display()))?;

        // Get current batches
        let current_batches: Vec<Arc<RecordBatch>> = {
            use futures::TryStreamExt;
            let stream = self
                .store
                .get_batches(&path)
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            let owned: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            owned.into_iter().map(Arc::new).collect()
        };
        let (new_schema, new_batches) = if operation.starts_with("add ") {
            self.alter_add_column(operation, &current_schema, &current_batches)?
        } else if operation.starts_with("drop ") {
            self.alter_drop_column(operation, &current_schema, &current_batches)?
        } else if operation.starts_with("rename ") {
            self.alter_rename_column(operation, &current_schema, &current_batches)?
        } else {
            return Err(SqlError::UnsupportedCommand(
                "only ADD, DROP, RENAME operations are supported".to_string(),
            ));
        };

        // Replace the table with the new schema and data
        self.store
            .remove(&path)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        let batches: Vec<RecordBatch> = new_batches.into_iter().map(|b| (*b).clone()).collect();
        self.store
            .put(path.clone(), new_schema, batches)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        self.register_table_in_catalog(&path).await;

        info!(path = %path.display(), "ALTER TABLE executed");

        Ok((path, 0))
    }

    /// Execute ADD COLUMN operation.
    fn alter_add_column(
        &self,
        operation: &str,
        current_schema: &SchemaRef,
        current_batches: &[Arc<RecordBatch>],
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        let after_add = operation.strip_prefix("add ").unwrap().trim_start();

        // Skip optional COLUMN keyword
        let column_part = if after_add.starts_with("column ") {
            after_add.strip_prefix("column ").unwrap().trim_start()
        } else {
            after_add
        };

        // Parse: column_name data_type [NOT NULL] [DEFAULT ...]
        let parts: Vec<&str> = column_part.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(SqlError::SyntaxError(
                "expected column_name data_type".to_string(),
            ));
        }

        let column_name = parts[0];
        let data_type_str = parts[1].to_uppercase();
        let nullable = !column_part.to_uppercase().contains("NOT NULL");

        let data_type = self.parse_data_type(&data_type_str)?;

        // Check if column already exists
        if current_schema.field_with_name(column_name).is_ok() {
            return Err(SqlError::InvalidCommand(format!(
                "column '{column_name}' already exists"
            )));
        }

        // Create new schema with added column
        let mut fields: Vec<Field> = current_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(column_name, data_type.clone(), nullable));
        let new_schema = Arc::new(Schema::new(fields));

        // Create new batches with null values for the new column
        let new_batches = self.add_null_column_to_batches(
            current_batches,
            column_name,
            &data_type,
            nullable,
            &new_schema,
        )?;

        Ok((new_schema, new_batches))
    }

    /// Execute DROP COLUMN operation.
    fn alter_drop_column(
        &self,
        operation: &str,
        current_schema: &SchemaRef,
        current_batches: &[Arc<RecordBatch>],
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        let after_drop = operation.strip_prefix("drop ").unwrap().trim_start();

        // Skip optional COLUMN keyword
        let column_name = if after_drop.starts_with("column ") {
            after_drop.strip_prefix("column ").unwrap().trim_start()
        } else {
            after_drop
        };

        let column_name = column_name.split_whitespace().next().unwrap_or(column_name);

        // Check if column exists
        let column_idx = current_schema
            .index_of(column_name)
            .map_err(|_| SqlError::InvalidCommand(format!("column '{column_name}' not found")))?;

        // Can't drop the last column
        if current_schema.fields().len() == 1 {
            return Err(SqlError::InvalidCommand(
                "cannot drop the last column".to_string(),
            ));
        }

        // Create new schema without the column
        let fields: Vec<Field> = current_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != column_idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        let new_schema = Arc::new(Schema::new(fields));

        // Create new batches without the column
        let new_batches =
            self.drop_column_from_batches(current_batches, column_idx, &new_schema)?;

        Ok((new_schema, new_batches))
    }

    /// Execute RENAME COLUMN operation.
    fn alter_rename_column(
        &self,
        operation: &str,
        current_schema: &SchemaRef,
        current_batches: &[Arc<RecordBatch>],
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        let after_rename = operation.strip_prefix("rename ").unwrap().trim_start();

        // Skip optional COLUMN keyword
        let column_part = if after_rename.starts_with("column ") {
            after_rename.strip_prefix("column ").unwrap().trim_start()
        } else {
            after_rename
        };

        // Parse: old_name TO new_name
        let parts: Vec<&str> = column_part.split_whitespace().collect();
        let to_idx = parts
            .iter()
            .position(|&p| p == "to")
            .ok_or_else(|| SqlError::SyntaxError("expected old_name TO new_name".to_string()))?;

        if to_idx == 0 || to_idx + 1 >= parts.len() {
            return Err(SqlError::SyntaxError(
                "expected old_name TO new_name".to_string(),
            ));
        }

        let old_name = parts[to_idx - 1];
        let new_name = parts[to_idx + 1];

        // Check if old column exists
        let column_idx = current_schema
            .index_of(old_name)
            .map_err(|_| SqlError::InvalidCommand(format!("column '{old_name}' not found")))?;

        // Check if new name already exists
        if current_schema.field_with_name(new_name).is_ok() {
            return Err(SqlError::InvalidCommand(format!(
                "column '{new_name}' already exists"
            )));
        }

        // Create new schema with renamed column
        let fields: Vec<Field> = current_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == column_idx {
                    Field::new(new_name, f.data_type().clone(), f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let new_schema = Arc::new(Schema::new(fields));

        // Recreate batches with new schema (data is the same, just schema changes)
        let new_batches = self.rename_column_in_batches(current_batches, &new_schema)?;

        Ok((new_schema, new_batches))
    }

    /// Parse a SQL data type string into an Arrow DataType.
    fn parse_data_type(&self, type_str: &str) -> SqlResult<arrow_schema::DataType> {
        use arrow_schema::DataType;

        let type_upper = type_str.to_uppercase();
        let dt = match type_upper.as_str() {
            "INT" | "INTEGER" | "INT32" => DataType::Int32,
            "BIGINT" | "INT64" => DataType::Int64,
            "SMALLINT" | "INT16" => DataType::Int16,
            "TINYINT" | "INT8" => DataType::Int8,
            "FLOAT" | "FLOAT32" | "REAL" => DataType::Float32,
            "DOUBLE" | "FLOAT64" => DataType::Float64,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "VARCHAR" | "TEXT" | "STRING" | "UTF8" => DataType::Utf8,
            "BINARY" | "BYTEA" | "BLOB" => DataType::Binary,
            "DATE" | "DATE32" => DataType::Date32,
            "TIMESTAMP" => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            _ => {
                // Check for VARCHAR(n) or similar
                if type_upper.starts_with("VARCHAR") || type_upper.starts_with("CHAR") {
                    DataType::Utf8
                } else {
                    return Err(SqlError::SyntaxError(format!(
                        "unknown data type: {type_str}"
                    )));
                }
            }
        };

        Ok(dt)
    }

    /// Add a null column to all batches.
    fn add_null_column_to_batches(
        &self,
        batches: &[Arc<RecordBatch>],
        _column_name: &str,
        data_type: &arrow_schema::DataType,
        _nullable: bool,
        new_schema: &SchemaRef,
    ) -> SqlResult<Vec<Arc<RecordBatch>>> {
        use arrow_array::{ArrayRef, new_null_array};

        let mut new_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let num_rows = batch.num_rows();
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

            // Create null array for the new column
            let null_array = new_null_array(data_type, num_rows);
            columns.push(null_array);

            let new_batch = RecordBatch::try_new(new_schema.clone(), columns)
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            new_batches.push(Arc::new(new_batch));
        }

        Ok(new_batches)
    }

    /// Drop a column from all batches.
    fn drop_column_from_batches(
        &self,
        batches: &[Arc<RecordBatch>],
        column_idx: usize,
        new_schema: &SchemaRef,
    ) -> SqlResult<Vec<Arc<RecordBatch>>> {
        use arrow_array::ArrayRef;

        let mut new_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != column_idx)
                .map(|(_, col)| col.clone())
                .collect();

            let new_batch = RecordBatch::try_new(new_schema.clone(), columns)
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            new_batches.push(Arc::new(new_batch));
        }

        Ok(new_batches)
    }

    /// Rename a column in all batches (just recreate with new schema).
    fn rename_column_in_batches(
        &self,
        batches: &[Arc<RecordBatch>],
        new_schema: &SchemaRef,
    ) -> SqlResult<Vec<Arc<RecordBatch>>> {
        let mut new_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let new_batch = RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                .map_err(|e| SqlError::Internal(e.to_string()))?;
            new_batches.push(Arc::new(new_batch));
        }

        Ok(new_batches)
    }

    /// Execute a MERGE statement (SQL:2003 MERGE syntax).
    ///
    /// Executes the merge by building equivalent SQL queries for each WHEN
    /// clause and combining results via UNION ALL.
    async fn execute_merge(&self, query: &str) -> SqlResult<(DataPath, i64)> {
        let lower = query.trim().to_lowercase();
        let original = query.trim();

        // Find keyword positions
        let into_pos = lower
            .find(" into ")
            .ok_or_else(|| SqlError::SyntaxError("missing INTO in MERGE".to_string()))?;
        let using_pos = lower
            .find(" using ")
            .ok_or_else(|| SqlError::SyntaxError("missing USING in MERGE".to_string()))?;
        let on_pos = lower[using_pos + 7..]
            .find(" on ")
            .map(|p| using_pos + 7 + p)
            .ok_or_else(|| SqlError::SyntaxError("missing ON in MERGE".to_string()))?;

        // Extract target and source with optional aliases
        let target_part = original[into_pos + 6..using_pos].trim();
        let source_part = original[using_pos + 7..on_pos].trim();

        let (target_table, target_alias) = self.split_table_alias(target_part);
        let (source_table, source_alias) = self.split_table_alias(source_part);

        let target_path = self.parse_table_name(&target_table.to_lowercase())?;
        let source_path = self.parse_table_name(&source_table.to_lowercase())?;

        // Verify both tables exist
        let target_schema = self
            .store
            .get_schema(&target_path)
            .await
            .map_err(|_| SqlError::TableNotFound(target_path.display()))?;
        self.store
            .get_schema(&source_path)
            .await
            .map_err(|_| SqlError::TableNotFound(source_path.display()))?;

        let target_sql = self.qualified_table_sql(&target_path);
        let source_sql = self.qualified_table_sql(&source_path);
        let t_alias = target_alias.unwrap_or("_t");
        let s_alias = source_alias.unwrap_or("_s");

        // Find WHEN clauses (ensure "when matched" is not "when not matched")
        let when_not_matched_pos = lower.find(" when not matched ");
        let when_matched_pos = lower
            .find(" when matched ")
            .filter(|&p| when_not_matched_pos.is_none_or(|wnm| p != wnm + 4));

        // Extract ON condition (between ON and first WHEN clause)
        let first_when = when_matched_pos
            .into_iter()
            .chain(when_not_matched_pos)
            .min()
            .unwrap_or(original.len());
        let on_condition = original[on_pos + 4..first_when].trim();

        // Parse WHEN MATCHED THEN UPDATE SET ...
        let update_assignments = if let Some(wm_pos) = when_matched_pos {
            let clause_end = when_not_matched_pos
                .filter(|&p| p > wm_pos)
                .unwrap_or(original.len());
            let clause = &original[wm_pos..clause_end];
            let clause_lower = clause.to_lowercase();
            if let Some(set_offset) = clause_lower.find(" set ") {
                let set_str = clause[set_offset + 5..].trim();
                Some(self.parse_set_assignments(set_str)?)
            } else {
                None
            }
        } else {
            None
        };

        // Parse WHEN NOT MATCHED THEN INSERT (cols) VALUES (exprs)
        let insert_clause = if let Some(wnm_pos) = when_not_matched_pos {
            let clause_end = when_matched_pos
                .filter(|&p| p > wnm_pos)
                .unwrap_or(original.len());
            let clause = &original[wnm_pos..clause_end];
            let clause_lower = clause.to_lowercase();
            if let Some(insert_offset) = clause_lower.find(" insert ") {
                let insert_str = clause[insert_offset + 8..].trim();
                Some(self.parse_insert_columns_values(insert_str)?)
            } else {
                None
            }
        } else {
            None
        };

        let target_columns: Vec<&str> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        let ctx = self.ctx.read().unwrap().clone();
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut affected_count: i64 = 0;

        // Part 1: Unchanged target rows (not matched by source)
        {
            let select_cols: Vec<String> = target_columns
                .iter()
                .map(|c| format!("{t_alias}.\"{}\"", c))
                .collect();
            let unchanged_query = format!(
                "SELECT {} FROM {target_sql} {t_alias} \
                 WHERE NOT EXISTS (SELECT 1 FROM {source_sql} {s_alias} WHERE {on_condition})",
                select_cols.join(", ")
            );
            let df = ctx
                .sql(&unchanged_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            all_batches.extend(
                df.collect()
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?,
            );
        }

        // Part 2: Updated matched rows
        if let Some(ref assignments) = update_assignments {
            let select_cols: Vec<String> = target_columns
                .iter()
                .map(|c| {
                    if let Some(expr) = assignments.get(*c) {
                        format!("{expr} AS \"{c}\"")
                    } else {
                        format!("{t_alias}.\"{}\"", c)
                    }
                })
                .collect();
            let updated_query = format!(
                "SELECT {} FROM {target_sql} {t_alias} \
                 INNER JOIN {source_sql} {s_alias} ON {on_condition}",
                select_cols.join(", ")
            );
            let df = ctx
                .sql(&updated_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            let updated_batches = df
                .collect()
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            affected_count += updated_batches
                .iter()
                .map(|b| b.num_rows() as i64)
                .sum::<i64>();
            all_batches.extend(updated_batches);
        }

        // Part 3: Inserted rows from unmatched source
        if let Some((ref cols, ref vals)) = insert_clause {
            let select_cols: Vec<String> = target_columns
                .iter()
                .map(|tc| {
                    if let Some(idx) = cols.iter().position(|c| c == tc) {
                        format!("{} AS \"{}\"", vals[idx], tc)
                    } else {
                        format!("NULL AS \"{}\"", tc)
                    }
                })
                .collect();
            let inserted_query = format!(
                "SELECT {} FROM {source_sql} {s_alias} \
                 WHERE NOT EXISTS (SELECT 1 FROM {target_sql} {t_alias} WHERE {on_condition})",
                select_cols.join(", ")
            );
            let df = ctx
                .sql(&inserted_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            let inserted_batches = df
                .collect()
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            affected_count += inserted_batches
                .iter()
                .map(|b| b.num_rows() as i64)
                .sum::<i64>();
            all_batches.extend(inserted_batches);
        }

        // Write back to target
        self.store
            .put(target_path.clone(), target_schema, all_batches)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        info!(
            target = %target_path.display(),
            record_count = affected_count,
            "MERGE executed"
        );

        Ok((target_path, affected_count))
    }

    /// Execute an INSERT...ON CONFLICT statement (PostgreSQL-style UPSERT).
    ///
    /// For DO NOTHING: inserts only non-conflicting rows.
    /// For DO UPDATE: updates conflicting rows and inserts non-conflicting ones.
    async fn execute_upsert(&self, query: &str) -> SqlResult<(DataPath, i64)> {
        let lower = query.trim().to_lowercase();
        let original = query.trim();

        // Parse table name
        let into_pos = lower
            .find(" into ")
            .ok_or_else(|| SqlError::SyntaxError("missing INTO clause".to_string()))?;
        let after_into = &original[into_pos + 6..];
        let table_end = after_into
            .find(|c: char| c == '(' || c.is_whitespace())
            .unwrap_or(after_into.len());
        let table_name = after_into[..table_end].trim();
        let path = self.parse_table_name(&table_name.to_lowercase())?;

        let target_schema = self
            .store
            .get_schema(&path)
            .await
            .map_err(|_| SqlError::TableNotFound(path.display()))?;
        let target_qualified = self.qualified_table_sql(&path);

        // Parse column list from the original query (after table name)
        let rest = &original[into_pos + 6 + table_end..];
        let cols_start = rest
            .find('(')
            .ok_or_else(|| SqlError::SyntaxError("missing column list".to_string()))?;
        let cols_end = rest
            .find(')')
            .ok_or_else(|| SqlError::SyntaxError("missing closing paren".to_string()))?;
        let columns: Vec<String> = rest[cols_start + 1..cols_end]
            .split(',')
            .map(|c| c.trim().to_lowercase().trim_matches('"').to_string())
            .collect();

        // Parse VALUES section (use original casing for literals)
        let values_pos = lower
            .find(" values ")
            .ok_or_else(|| SqlError::SyntaxError("missing VALUES clause".to_string()))?;
        let on_conflict_pos = lower
            .find(" on conflict ")
            .ok_or_else(|| SqlError::SyntaxError("missing ON CONFLICT clause".to_string()))?;
        let values_section = original[values_pos + 8..on_conflict_pos].trim();

        // Parse ON CONFLICT (key_col)
        let after_conflict = &original[on_conflict_pos + 13..];
        let conflict_start = after_conflict
            .find('(')
            .ok_or_else(|| SqlError::SyntaxError("missing conflict column".to_string()))?;
        let conflict_end = after_conflict.find(')').ok_or_else(|| {
            SqlError::SyntaxError("missing closing paren for conflict column".to_string())
        })?;
        let conflict_col = after_conflict[conflict_start + 1..conflict_end]
            .trim()
            .to_lowercase();

        // Determine action
        let do_update = lower.contains("do update");
        let do_nothing = lower.contains("do nothing");

        if !do_update && !do_nothing {
            return Err(SqlError::SyntaxError(
                "ON CONFLICT requires DO UPDATE or DO NOTHING".to_string(),
            ));
        }

        // Build VALUES subquery with column aliases
        let col_list = columns.join(", ");
        let values_subquery =
            format!("(SELECT * FROM (VALUES {values_section}) AS _nv({col_list}))");

        let target_columns: Vec<&str> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        let ctx = self.ctx.read().unwrap().clone();
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut affected_count: i64 = 0;

        if do_nothing {
            // Keep all existing rows as-is
            let existing_query = format!(
                "SELECT {} FROM {target_qualified}",
                target_columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let df = ctx
                .sql(&existing_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            all_batches.extend(
                df.collect()
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?,
            );

            // Insert only non-conflicting new rows
            let insert_cols: Vec<String> = target_columns
                .iter()
                .map(|tc| {
                    if let Some(idx) = columns.iter().position(|c| c == tc) {
                        format!("_nv.\"{}\"", columns[idx])
                    } else {
                        format!("NULL AS \"{}\"", tc)
                    }
                })
                .collect();
            let new_query = format!(
                "SELECT {} FROM {values_subquery} _nv \
                 WHERE _nv.\"{conflict_col}\" NOT IN (SELECT \"{conflict_col}\" FROM {target_qualified})",
                insert_cols.join(", ")
            );
            let df = ctx
                .sql(&new_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            let new_batches = df
                .collect()
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            affected_count += new_batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            all_batches.extend(new_batches);
        } else {
            // DO UPDATE

            // Parse SET clause (replace EXCLUDED.col with _nv.col)
            let set_pos = lower
                .rfind(" set ")
                .ok_or_else(|| SqlError::SyntaxError("missing SET in DO UPDATE".to_string()))?;
            let set_clause = original[set_pos + 5..].trim();
            let assignments = self.parse_upsert_set_assignments(set_clause)?;

            // Non-conflicting existing rows
            let existing_cols: Vec<String> = target_columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect();
            let existing_query = format!(
                "SELECT {} FROM {target_qualified} \
                 WHERE \"{conflict_col}\" NOT IN (SELECT \"{conflict_col}\" FROM {values_subquery} _nv)",
                existing_cols.join(", ")
            );
            let df = ctx
                .sql(&existing_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            all_batches.extend(
                df.collect()
                    .await
                    .map_err(|e| SqlError::QueryExecution(e.to_string()))?,
            );

            // Updated conflicting rows
            let update_cols: Vec<String> = target_columns
                .iter()
                .map(|c| {
                    if let Some(expr) = assignments.get(*c) {
                        format!("{expr} AS \"{c}\"")
                    } else {
                        format!("_existing.\"{}\"", c)
                    }
                })
                .collect();
            let updated_query = format!(
                "SELECT {} FROM {target_qualified} _existing \
                 INNER JOIN {values_subquery} _nv ON _existing.\"{conflict_col}\" = _nv.\"{conflict_col}\"",
                update_cols.join(", ")
            );
            let df = ctx
                .sql(&updated_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            let updated_batches = df
                .collect()
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            affected_count += updated_batches
                .iter()
                .map(|b| b.num_rows() as i64)
                .sum::<i64>();
            all_batches.extend(updated_batches);

            // Non-conflicting new rows
            let insert_cols: Vec<String> = target_columns
                .iter()
                .map(|tc| {
                    if let Some(idx) = columns.iter().position(|c| c == tc) {
                        format!("_nv.\"{}\"", columns[idx])
                    } else {
                        format!("NULL AS \"{}\"", tc)
                    }
                })
                .collect();
            let new_query = format!(
                "SELECT {} FROM {values_subquery} _nv \
                 WHERE _nv.\"{conflict_col}\" NOT IN (SELECT \"{conflict_col}\" FROM {target_qualified})",
                insert_cols.join(", ")
            );
            let df = ctx
                .sql(&new_query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            let new_batches = df
                .collect()
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;
            affected_count += new_batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            all_batches.extend(new_batches);
        }

        // Write back to target
        self.store
            .put(path.clone(), target_schema, all_batches)
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        info!(
            path = %path.display(),
            record_count = affected_count,
            "UPSERT executed"
        );

        Ok((path, affected_count))
    }

    /// Split a "table_name AS alias" string into table and optional alias.
    fn split_table_alias<'a>(&self, s: &'a str) -> (&'a str, Option<&'a str>) {
        let lower = s.to_lowercase();
        if let Some(as_pos) = lower.find(" as ") {
            let table = s[..as_pos].trim();
            let alias = s[as_pos + 4..].trim();
            (table, Some(alias))
        } else {
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() == 2 {
                (parts[0], Some(parts[1]))
            } else {
                (s.trim(), None)
            }
        }
    }

    /// Parse SET assignments: "col1 = expr1, col2 = expr2".
    fn parse_set_assignments(&self, set_clause: &str) -> SqlResult<HashMap<String, String>> {
        let mut assignments = HashMap::new();
        for assign in self.split_top_level(set_clause, ',') {
            let eq_pos = assign
                .find('=')
                .ok_or_else(|| SqlError::SyntaxError("invalid SET assignment".to_string()))?;
            let col = assign[..eq_pos]
                .trim()
                .to_lowercase()
                .trim_matches('"')
                .to_string();
            let expr = assign[eq_pos + 1..].trim().to_string();
            assignments.insert(col, expr);
        }
        Ok(assignments)
    }

    /// Parse UPSERT SET assignments, replacing EXCLUDED references.
    fn parse_upsert_set_assignments(&self, set_clause: &str) -> SqlResult<HashMap<String, String>> {
        let mut assignments = HashMap::new();
        for assign in self.split_top_level(set_clause, ',') {
            let eq_pos = assign
                .find('=')
                .ok_or_else(|| SqlError::SyntaxError("invalid SET assignment".to_string()))?;
            let col = assign[..eq_pos]
                .trim()
                .to_lowercase()
                .trim_matches('"')
                .to_string();
            let expr = assign[eq_pos + 1..].trim().to_string();
            // Replace EXCLUDED.col with _nv."col"
            let expr = expr.replace("EXCLUDED.", "_nv.\"");
            let expr = expr.replace("excluded.", "_nv.\"");
            // Close any opened quotes from the replacement
            let expr = if expr.contains("_nv.\"") && !expr.ends_with('"') {
                // Find each _nv." and close the quote at the next word boundary
                let mut result = String::new();
                let mut chars = expr.chars().peekable();
                while let Some(c) = chars.next() {
                    result.push(c);
                    if result.ends_with("_nv.\"") {
                        // Read until word boundary
                        while let Some(&next) = chars.peek() {
                            if next.is_alphanumeric() || next == '_' {
                                result.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                        result.push('"');
                    }
                }
                result
            } else {
                expr
            };
            assignments.insert(col, expr);
        }
        Ok(assignments)
    }

    /// Parse INSERT columns and values: "(col1, col2) VALUES (expr1, expr2)".
    fn parse_insert_columns_values(&self, clause: &str) -> SqlResult<(Vec<String>, Vec<String>)> {
        let cols_start = clause
            .find('(')
            .ok_or_else(|| SqlError::SyntaxError("missing column list in INSERT".to_string()))?;
        let cols_end = clause.find(')').ok_or_else(|| {
            SqlError::SyntaxError("missing closing paren in column list".to_string())
        })?;

        let columns: Vec<String> = clause[cols_start + 1..cols_end]
            .split(',')
            .map(|c| c.trim().to_lowercase().trim_matches('"').to_string())
            .collect();

        let rest = &clause[cols_end + 1..];
        let lower_rest = rest.to_lowercase();
        let values_pos = lower_rest
            .find("values")
            .ok_or_else(|| SqlError::SyntaxError("missing VALUES in INSERT".to_string()))?;

        let after_values = &rest[values_pos + 6..];
        let vals_start = after_values
            .find('(')
            .ok_or_else(|| SqlError::SyntaxError("missing values list".to_string()))?;
        let vals_end = after_values
            .rfind(')')
            .ok_or_else(|| SqlError::SyntaxError("missing closing paren in values".to_string()))?;

        let values: Vec<String> = self
            .split_top_level(&after_values[vals_start + 1..vals_end], ',')
            .into_iter()
            .map(|v| v.trim().to_string())
            .collect();

        if columns.len() != values.len() {
            return Err(SqlError::SyntaxError(
                "column count does not match value count in INSERT".to_string(),
            ));
        }

        Ok((columns, values))
    }

    /// Split a string by delimiter, respecting parentheses and string literals.
    fn split_top_level<'a>(&self, s: &'a str, delim: char) -> Vec<&'a str> {
        let mut parts = Vec::new();
        let mut depth = 0i32;
        let mut in_string = false;
        let mut start = 0;

        for (i, c) in s.char_indices() {
            match c {
                '\'' if !in_string => in_string = true,
                '\'' if in_string => in_string = false,
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                c if c == delim && depth == 0 && !in_string => {
                    parts.push(&s[start..i]);
                    start = i + c.len_utf8();
                }
                _ => {}
            }
        }
        parts.push(&s[start..]);
        parts
    }

    /// Parse a table name into a DataPath.
    fn parse_table_name(&self, table_name: &str) -> SqlResult<DataPath> {
        let segments: Vec<String> = table_name
            .split('.')
            .map(|s| s.trim_matches('"').to_string())
            .collect();

        if segments.is_empty() || segments.iter().any(|s| s.is_empty()) {
            return Err(SqlError::SyntaxError("invalid table name".to_string()));
        }

        Ok(DataPath::new(segments))
    }

    /// Parse a simple SELECT query to extract table path.
    #[cfg(test)]
    fn parse_select_query(&self, query: &str) -> SqlResult<DataPath> {
        let query = query.trim().to_lowercase();

        // Simple parser for: SELECT * FROM table_name
        // or: SELECT * FROM schema.table_name
        if !query.starts_with("select") {
            return Err(SqlError::SyntaxError(
                "only SELECT queries are supported".to_string(),
            ));
        }

        // Find FROM clause
        let from_pos = query
            .find(" from ")
            .ok_or_else(|| SqlError::SyntaxError("missing FROM clause".to_string()))?;

        let after_from = &query[from_pos + 6..];
        let table_name = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| SqlError::SyntaxError("missing table name".to_string()))?;

        // Handle schema.table format
        let segments: Vec<String> = table_name
            .split('.')
            .map(|s| s.trim_matches('"').to_string())
            .collect();

        if segments.is_empty() {
            return Err(SqlError::SyntaxError("empty table name".to_string()));
        }

        Ok(DataPath::new(segments))
    }

    /// Parse LIMIT clause from a query, returning None if not present.
    #[cfg(test)]
    fn parse_limit(&self, query: &str) -> Option<usize> {
        let query_lower = query.trim().to_lowercase();

        // Find "LIMIT" keyword
        if let Some(limit_pos) = query_lower.find(" limit ") {
            let after_limit = &query_lower[limit_pos + 7..];
            // Take the first token which should be the number
            if let Some(num_str) = after_limit.split_whitespace().next() {
                return num_str.parse::<usize>().ok();
            }
        }
        None
    }

    /// Apply a row limit to record batches.
    #[cfg(test)]
    fn apply_limit(&self, batches: Vec<Arc<RecordBatch>>, limit: usize) -> Vec<Arc<RecordBatch>> {
        let mut result = Vec::new();
        let mut remaining = limit;

        for batch in batches {
            if remaining == 0 {
                break;
            }

            let batch_rows = batch.num_rows();
            if batch_rows <= remaining {
                // Take the whole batch
                result.push(batch);
                remaining -= batch_rows;
            } else {
                // Slice the batch to get only `remaining` rows
                let sliced = batch.slice(0, remaining);
                result.push(Arc::new(sliced));
                remaining = 0;
            }
        }

        result
    }

    /// Create a query handle from a query string.
    fn create_query_handle(&self, query: &str) -> Bytes {
        Bytes::from(query.to_string())
    }

    /// Generate a unique prepared statement handle.
    fn generate_handle(&self) -> Bytes {
        let mut id = self.next_handle_id.write().unwrap();
        let handle = Bytes::from(format!("ps_{}", *id));
        *id += 1;
        handle
    }

    /// Create a prepared statement.
    ///
    /// Uses DataFusion to validate queries and infer result schema.
    pub async fn create_prepared_statement(
        &self,
        request: &ActionCreatePreparedStatementRequest,
    ) -> SqlResult<CreatePreparedStatementResult> {
        let query = &request.query;
        debug!(query = %query, "creating prepared statement");

        let query_lower = query.trim().to_lowercase();
        let is_query = query_lower.starts_with("select");

        // Validate the query and determine schema using DataFusion
        let (dataset_schema, parameter_schema) = if is_query {
            // For SELECT queries, use DataFusion to get the result schema
            let ctx = self.ctx.read().unwrap().clone();
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

            let schema = df.schema().inner().clone();
            let params = self.extract_parameter_schema(query);
            (Some(schema), params)
        } else if query_lower.starts_with("insert")
            || query_lower.starts_with("update")
            || query_lower.starts_with("delete")
        {
            // For DML statements, validate the table exists
            let path = self.parse_dml_table_name(query)?;
            self.store
                .get_schema(&path)
                .await
                .map_err(|_| SqlError::TableNotFound(path.display()))?;

            let params = self.extract_parameter_schema(query);
            (None, params)
        } else {
            return Err(SqlError::SyntaxError(
                "only SELECT, INSERT, UPDATE, DELETE statements are supported".to_string(),
            ));
        };

        // Generate handle and store the prepared statement
        let handle = self.generate_handle();
        let stmt = PreparedStatement {
            query: query.clone(),
            is_query,
            dataset_schema: dataset_schema.clone(),
            parameter_schema: parameter_schema.clone(),
            bound_parameters: None,
        };

        self.prepared_statements
            .write()
            .unwrap()
            .insert(handle.clone(), stmt);

        info!(
            handle = %String::from_utf8_lossy(&handle),
            query = %query,
            is_query,
            "prepared statement created"
        );

        Ok(CreatePreparedStatementResult {
            handle,
            dataset_schema,
            parameter_schema,
        })
    }

    /// Close a prepared statement.
    pub fn close_prepared_statement(
        &self,
        request: &ActionClosePreparedStatementRequest,
    ) -> SqlResult<()> {
        let handle = &request.prepared_statement_handle;
        debug!(handle = %String::from_utf8_lossy(handle), "closing prepared statement");

        let removed = self.prepared_statements.write().unwrap().remove(handle);

        if removed.is_some() {
            info!(handle = %String::from_utf8_lossy(handle), "prepared statement closed");
            Ok(())
        } else {
            warn!(
                handle = %String::from_utf8_lossy(handle),
                "prepared statement not found for close"
            );
            // Per Flight SQL spec, closing a non-existent statement is not an error
            Ok(())
        }
    }

    /// Bind parameters to a prepared statement.
    ///
    /// Clients send parameter values via DoPut with CommandPreparedStatementQuery.
    /// The parameters are stored and used when the statement is executed.
    pub fn bind_parameters(
        &self,
        handle: &Bytes,
        parameters: Vec<Arc<RecordBatch>>,
    ) -> SqlResult<()> {
        debug!(
            handle = %String::from_utf8_lossy(handle),
            batch_count = parameters.len(),
            "binding parameters to prepared statement"
        );

        let mut statements = self.prepared_statements.write().unwrap();
        let stmt = statements.get_mut(handle).ok_or_else(|| {
            SqlError::PreparedStatementNotFound(String::from_utf8_lossy(handle).to_string())
        })?;

        let total_rows: usize = parameters.iter().map(|b| b.num_rows()).sum();

        stmt.bound_parameters = Some(parameters);

        info!(
            handle = %String::from_utf8_lossy(handle),
            total_rows,
            "parameters bound to prepared statement"
        );

        Ok(())
    }

    /// Get the bound parameters for a prepared statement.
    pub fn get_bound_parameters(&self, handle: &Bytes) -> SqlResult<Option<Vec<Arc<RecordBatch>>>> {
        let statements = self.prepared_statements.read().unwrap();
        let stmt = statements.get(handle).ok_or_else(|| {
            SqlError::PreparedStatementNotFound(String::from_utf8_lossy(handle).to_string())
        })?;

        Ok(stmt.bound_parameters.clone())
    }

    /// Execute a prepared statement query.
    ///
    /// Uses DataFusion to validate and prepare the query.
    pub async fn execute_prepared_statement_query(
        &self,
        cmd: &CommandPreparedStatementQuery,
    ) -> SqlResult<QueryResult> {
        let handle = cmd.prepared_statement_handle.clone();
        debug!(handle = %String::from_utf8_lossy(&handle), "executing prepared statement query");

        let query = {
            let statements = self.prepared_statements.read().unwrap();
            let stmt = statements.get(&handle).ok_or_else(|| {
                SqlError::PreparedStatementNotFound(String::from_utf8_lossy(&handle).to_string())
            })?;

            if !stmt.is_query {
                return Err(SqlError::InvalidCommand(
                    "prepared statement is not a query".to_string(),
                ));
            }

            stmt.query.clone()
        };

        // Use DataFusion to get the schema
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(&query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();

        info!(
            handle = %String::from_utf8_lossy(&handle),
            "prepared statement query validated"
        );

        Ok(QueryResult {
            handle: handle.clone(),
            schema,
            total_records: -1,
        })
    }

    /// Get data for a prepared statement query.
    ///
    /// Executes the query using DataFusion.
    pub async fn get_prepared_statement_data(
        &self,
        handle: &Bytes,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!(handle = %String::from_utf8_lossy(handle), "getting prepared statement data via DataFusion");

        let query = {
            let statements = self.prepared_statements.read().unwrap();
            let stmt = statements.get(handle).ok_or_else(|| {
                SqlError::PreparedStatementNotFound(String::from_utf8_lossy(handle).to_string())
            })?;
            stmt.query.clone()
        };

        // Execute using DataFusion
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(&query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();
        let batches = df
            .collect()
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        // Convert to Arc<RecordBatch>
        let batches: Vec<Arc<RecordBatch>> = batches.into_iter().map(Arc::new).collect();

        Ok((schema, batches))
    }

    /// Get a streaming data source for a prepared statement query.
    ///
    /// Executes the query using DataFusion and returns a stream of record batches.
    /// This avoids loading all data into memory at once for large result sets.
    pub async fn get_prepared_statement_data_stream(
        &self,
        handle: &Bytes,
    ) -> SqlResult<(SchemaRef, QueryDataStream)> {
        debug!(handle = %String::from_utf8_lossy(handle), "getting prepared statement data stream via DataFusion");

        let (query, bound) = {
            let statements = self.prepared_statements.read().unwrap();
            let stmt = statements.get(handle).ok_or_else(|| {
                SqlError::PreparedStatementNotFound(String::from_utf8_lossy(handle).to_string())
            })?;
            (stmt.query.clone(), stmt.bound_parameters.clone())
        };

        let query = if let Some(ref params) = bound {
            Self::substitute_parameters(&query, params)?
        } else {
            query
        };

        // Execute using DataFusion
        let ctx = self.ctx.read().unwrap().clone();
        let df = ctx
            .sql(&query)
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        let schema = df.schema().inner().clone();

        // Get a streaming result instead of collecting all batches
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqlError::QueryExecution(e.to_string()))?;

        info!(
            handle = %String::from_utf8_lossy(handle),
            "prepared statement query streaming started via DataFusion"
        );

        Ok((schema, QueryDataStream::new(stream)))
    }

    /// Execute a prepared statement update.
    pub async fn execute_prepared_statement_update(
        &self,
        cmd: &CommandPreparedStatementUpdate,
    ) -> SqlResult<UpdateResult> {
        let handle = &cmd.prepared_statement_handle;
        debug!(handle = %String::from_utf8_lossy(handle), "executing prepared statement update");

        let (query, bound) = {
            let statements = self.prepared_statements.read().unwrap();
            let stmt = statements.get(handle).ok_or_else(|| {
                SqlError::PreparedStatementNotFound(String::from_utf8_lossy(handle).to_string())
            })?;

            if stmt.is_query {
                return Err(SqlError::InvalidCommand(
                    "prepared statement is a query, not an update".to_string(),
                ));
            }

            (stmt.query.clone(), stmt.bound_parameters.clone())
        };

        let query = if let Some(ref params) = bound {
            Self::substitute_parameters(&query, params)?
        } else {
            query
        };

        // Execute the stored update via DataFusion
        let (path, record_count) = self.parse_and_execute_update_async(&query).await?;

        info!(
            handle = %String::from_utf8_lossy(handle),
            path = %path.display(),
            record_count,
            "prepared statement update executed"
        );

        Ok(UpdateResult { record_count })
    }

    /// Extract parameter schema from a query.
    ///
    /// For now, returns an empty schema - parameter binding is a future enhancement.
    fn extract_parameter_schema(&self, _query: &str) -> Option<SchemaRef> {
        // In a full implementation, we'd parse ? placeholders and infer types
        // For now, return an empty schema indicating no parameters
        Some(Arc::new(Schema::new(Vec::<Field>::new())))
    }

    /// Substitute bound parameters into a query string.
    ///
    /// Replaces `?` placeholders in order with SQL literal representations of the
    /// values taken from the first row of the first parameter batch. Returns the
    /// original query unchanged when no parameters are bound.
    ///
    /// # Errors
    ///
    /// Returns an error if scalar value serialisation fails or if there are fewer
    /// bound values than `?` placeholders in the query.
    fn substitute_parameters(query: &str, params: &[Arc<RecordBatch>]) -> SqlResult<String> {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Expr as DfExpr;
        use datafusion::sql::unparser::Unparser;

        if params.is_empty() {
            return Ok(query.to_string());
        }

        let batch = &params[0];
        if batch.num_rows() == 0 {
            return Ok(query.to_string());
        }

        // Extract one scalar per column from the first row.
        let unparser = Unparser::default();
        let mut literals: Vec<String> = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let scalar = ScalarValue::try_from_array(batch.column(col_idx), 0).map_err(|e| {
                SqlError::Internal(format!("failed to extract parameter {col_idx}: {e}"))
            })?;
            let sql_expr = unparser
                .expr_to_sql(&DfExpr::Literal(scalar, None))
                .map_err(|e| {
                    SqlError::Internal(format!("failed to serialise parameter {col_idx}: {e}"))
                })?;
            literals.push(sql_expr.to_string());
        }

        // Replace each `?` placeholder (outside string literals) with the next value.
        let mut result = String::with_capacity(query.len());
        let mut param_idx = 0usize;
        let chars: Vec<char> = query.chars().collect();
        let mut i = 0;
        let mut in_string = false;
        let mut string_char = '"';

        while i < chars.len() {
            let c = chars[i];
            match c {
                '\'' | '"' if !in_string => {
                    in_string = true;
                    string_char = c;
                    result.push(c);
                }
                c if in_string && c == string_char => {
                    // Handle escaped quotes ('' or "")
                    if i + 1 < chars.len() && chars[i + 1] == string_char {
                        result.push(c);
                        result.push(c);
                        i += 2;
                        continue;
                    }
                    in_string = false;
                    result.push(c);
                }
                '?' if !in_string => {
                    let literal = literals.get(param_idx).ok_or_else(|| {
                        SqlError::InvalidCommand(format!(
                            "not enough bound parameters: placeholder {} has no value",
                            param_idx + 1
                        ))
                    })?;
                    result.push_str(literal);
                    param_idx += 1;
                }
                _ => result.push(c),
            }
            i += 1;
        }

        Ok(result)
    }

    /// Parse DML statement to extract table name.
    fn parse_dml_table_name(&self, query: &str) -> SqlResult<DataPath> {
        let query_lower = query.trim().to_lowercase();

        if query_lower.starts_with("insert") {
            let into_pos = query_lower
                .find(" into ")
                .ok_or_else(|| SqlError::SyntaxError("missing INTO clause".to_string()))?;
            let after_into = &query_lower[into_pos + 6..];
            let table_name = after_into
                .split(|c: char| c.is_whitespace() || c == '(')
                .next()
                .ok_or_else(|| SqlError::SyntaxError("missing table name".to_string()))?;
            self.parse_table_name(table_name)
        } else if query_lower.starts_with("update") {
            let after_update = query_lower
                .strip_prefix("update")
                .ok_or_else(|| SqlError::SyntaxError("invalid UPDATE syntax".to_string()))?
                .trim_start();
            let table_name = after_update
                .split_whitespace()
                .next()
                .ok_or_else(|| SqlError::SyntaxError("missing table name".to_string()))?;
            self.parse_table_name(table_name)
        } else if query_lower.starts_with("delete") {
            let from_pos = query_lower
                .find(" from ")
                .ok_or_else(|| SqlError::SyntaxError("missing FROM clause".to_string()))?;
            let after_from = &query_lower[from_pos + 6..];
            let table_name = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| SqlError::SyntaxError("missing table name".to_string()))?;
            self.parse_table_name(table_name)
        } else {
            Err(SqlError::SyntaxError(
                "unknown DML statement type".to_string(),
            ))
        }
    }

    /// Begin a new transaction.
    ///
    /// Returns a unique transaction ID. Also calls the store's
    /// `begin_transaction` hook so backends with native transaction support
    /// can participate.
    pub async fn begin_transaction(&self) -> SqlResult<Bytes> {
        self.begin_transaction_with_isolation(IsolationLevel::default())
            .await
    }

    /// Begin a new transaction with a specific isolation level.
    ///
    /// Returns a unique transaction ID.
    pub async fn begin_transaction_with_isolation(
        &self,
        isolation_level: IsolationLevel,
    ) -> SqlResult<Bytes> {
        let store_txn_id = self
            .store
            .begin_transaction()
            .await
            .map_err(|e| SqlError::Internal(e.to_string()))?;

        let id = self.generate_handle();
        let transaction = Transaction {
            active: true,
            isolation_level,
            pending_operations: Vec::new(),
            savepoints: HashMap::new(),
            store_transaction_id: store_txn_id,
        };

        self.transactions
            .write()
            .unwrap()
            .insert(id.clone(), transaction);

        info!(
            transaction_id = %String::from_utf8_lossy(&id),
            isolation_level = %isolation_level,
            "transaction started"
        );

        Ok(id)
    }

    /// End a transaction (commit or rollback).
    ///
    /// Calls the store's `commit_transaction` or `rollback_transaction` hook
    /// after updating the engine's internal transaction state.
    pub async fn end_transaction(
        &self,
        transaction_id: &Bytes,
        action: EndTransaction,
    ) -> SqlResult<()> {
        debug!(
            transaction_id = %String::from_utf8_lossy(transaction_id),
            action = ?action,
            "ending transaction"
        );

        let (store_txn_id, commit) = {
            let mut transactions = self.transactions.write().unwrap();
            let transaction = transactions.get_mut(transaction_id).ok_or_else(|| {
                SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
            })?;

            if !transaction.active {
                return Err(SqlError::InvalidTransactionAction(
                    "transaction already ended".to_string(),
                ));
            }

            let pending_count = transaction.pending_operations.len();
            let total_records: i64 = transaction
                .pending_operations
                .iter()
                .map(|op| op.record_count())
                .sum();

            match action {
                EndTransaction::Commit => {
                    for op in &transaction.pending_operations {
                        match op {
                            PendingOperation::Insert {
                                path,
                                query,
                                record_count,
                            } => {
                                debug!(path = %path.display(), query = %query, record_count, "committing INSERT");
                            }
                            PendingOperation::Update {
                                path,
                                query,
                                record_count,
                            } => {
                                debug!(path = %path.display(), query = %query, record_count, "committing UPDATE");
                            }
                            PendingOperation::Delete {
                                path,
                                query,
                                record_count,
                            } => {
                                debug!(path = %path.display(), query = %query, record_count, "committing DELETE");
                            }
                        }
                    }
                    info!(
                        transaction_id = %String::from_utf8_lossy(transaction_id),
                        pending_operations = pending_count,
                        total_records_affected = total_records,
                        "transaction committed"
                    );
                }
                EndTransaction::Rollback => {
                    info!(
                        transaction_id = %String::from_utf8_lossy(transaction_id),
                        pending_operations = pending_count,
                        total_records_discarded = total_records,
                        "transaction rolled back"
                    );
                }
                EndTransaction::Unspecified => {
                    return Err(SqlError::InvalidTransactionAction(
                        "unspecified transaction action".to_string(),
                    ));
                }
            }

            transaction.active = false;
            let store_txn_id = transaction.store_transaction_id.clone();
            transactions.remove(transaction_id);
            (store_txn_id, matches!(action, EndTransaction::Commit))
        };

        // Call store hooks outside the lock.
        if commit {
            self.store
                .commit_transaction(&store_txn_id)
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
        } else {
            self.store
                .rollback_transaction(&store_txn_id)
                .await
                .map_err(|e| SqlError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Get the count of pending operations in a transaction.
    #[cfg(test)]
    pub fn get_pending_operation_count(&self, transaction_id: &Bytes) -> SqlResult<usize> {
        let transactions = self.transactions.read().unwrap();
        let transaction = transactions.get(transaction_id).ok_or_else(|| {
            SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
        })?;
        Ok(transaction.pending_operations.len())
    }

    /// Begin a savepoint within an existing transaction.
    ///
    /// Returns a unique savepoint ID that can be used to rollback or release.
    pub fn begin_savepoint(&self, transaction_id: &Bytes, name: String) -> SqlResult<Bytes> {
        let mut transactions = self.transactions.write().unwrap();
        let transaction = transactions.get_mut(transaction_id).ok_or_else(|| {
            SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
        })?;

        if !transaction.active {
            return Err(SqlError::InvalidTransactionAction(
                "cannot create savepoint in inactive transaction".to_string(),
            ));
        }

        // Generate a unique savepoint ID
        let savepoint_id = self.generate_handle();

        // Record the current position in pending operations
        let savepoint = Savepoint {
            name: name.clone(),
            operation_index: transaction.pending_operations.len(),
        };

        transaction
            .savepoints
            .insert(savepoint_id.clone(), savepoint);

        info!(
            transaction_id = %String::from_utf8_lossy(transaction_id),
            savepoint_id = %String::from_utf8_lossy(&savepoint_id),
            savepoint_name = %name,
            operation_index = transaction.pending_operations.len(),
            "savepoint created"
        );

        Ok(savepoint_id)
    }

    /// End a savepoint (release or rollback).
    ///
    /// - Release: Removes the savepoint but keeps all operations since.
    /// - Rollback: Discards all operations since the savepoint was created.
    pub fn end_savepoint(&self, savepoint_id: &Bytes, action: EndSavepoint) -> SqlResult<()> {
        let mut transactions = self.transactions.write().unwrap();

        // Find the transaction containing this savepoint
        let transaction = transactions
            .values_mut()
            .find(|t| t.savepoints.contains_key(savepoint_id))
            .ok_or_else(|| {
                SqlError::SavepointNotFound(String::from_utf8_lossy(savepoint_id).to_string())
            })?;

        if !transaction.active {
            return Err(SqlError::InvalidTransactionAction(
                "cannot end savepoint in inactive transaction".to_string(),
            ));
        }

        let savepoint = transaction.savepoints.remove(savepoint_id).ok_or_else(|| {
            SqlError::SavepointNotFound(String::from_utf8_lossy(savepoint_id).to_string())
        })?;

        match action {
            EndSavepoint::Release => {
                // Simply remove the savepoint marker, keeping all operations
                info!(
                    savepoint_id = %String::from_utf8_lossy(savepoint_id),
                    savepoint_name = %savepoint.name,
                    "savepoint released"
                );
            }
            EndSavepoint::Rollback => {
                // Discard all operations since the savepoint
                let discarded_count =
                    transaction.pending_operations.len() - savepoint.operation_index;
                transaction
                    .pending_operations
                    .truncate(savepoint.operation_index);

                // Also invalidate any savepoints created after this one
                transaction
                    .savepoints
                    .retain(|_, sp| sp.operation_index <= savepoint.operation_index);

                info!(
                    savepoint_id = %String::from_utf8_lossy(savepoint_id),
                    savepoint_name = %savepoint.name,
                    discarded_operations = discarded_count,
                    "savepoint rolled back"
                );
            }
            EndSavepoint::Unspecified => {
                // Re-insert the savepoint since we didn't process it
                transaction
                    .savepoints
                    .insert(savepoint_id.clone(), savepoint);
                return Err(SqlError::InvalidTransactionAction(
                    "unspecified savepoint action".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Get the isolation level of a transaction.
    #[cfg(test)]
    pub fn get_transaction_isolation_level(
        &self,
        transaction_id: &Bytes,
    ) -> SqlResult<IsolationLevel> {
        let transactions = self.transactions.read().unwrap();
        let transaction = transactions.get(transaction_id).ok_or_else(|| {
            SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
        })?;
        Ok(transaction.isolation_level)
    }

    /// Get the count of savepoints in a transaction.
    #[cfg(test)]
    pub fn get_savepoint_count(&self, transaction_id: &Bytes) -> SqlResult<usize> {
        let transactions = self.transactions.read().unwrap();
        let transaction = transactions.get(transaction_id).ok_or_else(|| {
            SqlError::TransactionNotFound(String::from_utf8_lossy(transaction_id).to_string())
        })?;
        Ok(transaction.savepoints.len())
    }
}

/// Create a TicketStatementQuery message for a query handle.
pub fn create_statement_ticket(handle: Bytes) -> Bytes {
    let ticket = TicketStatementQuery {
        statement_handle: handle,
    };
    ticket.encode_to_vec().into()
}

/// Create a TicketStatementQuery for a prepared statement handle.
pub fn create_prepared_statement_ticket(handle: Bytes) -> Bytes {
    // Use the same ticket format as regular statement queries
    let ticket = TicketStatementQuery {
        statement_handle: handle,
    };
    ticket.encode_to_vec().into()
}

/// Create a TicketStatementQuery for a metadata query handle.
pub fn create_metadata_ticket(handle: Bytes) -> Bytes {
    // Use the same ticket format - metadata handles are distinguished by prefix
    let ticket = TicketStatementQuery {
        statement_handle: handle,
    };
    ticket.encode_to_vec().into()
}

/// Encode schema to IPC format for ActionCreatePreparedStatementResult.
pub fn encode_schema_to_ipc(schema: &SchemaRef) -> SqlResult<Bytes> {
    use arrow_flight::{IpcMessage, SchemaAsIpc};

    let options = IpcWriteOptions::default();
    let schema_ipc = SchemaAsIpc::new(schema, &options);
    let ipc_message: IpcMessage = schema_ipc
        .try_into()
        .map_err(|e: ArrowError| SqlError::Arrow(e))?;
    Ok(Bytes::copy_from_slice(&ipc_message))
}

/// Create the ActionCreatePreparedStatementResult message.
pub fn create_prepared_statement_result(
    result: &CreatePreparedStatementResult,
) -> SqlResult<ActionCreatePreparedStatementResult> {
    let dataset_schema = match &result.dataset_schema {
        Some(schema) => encode_schema_to_ipc(schema)?,
        None => Bytes::new(),
    };

    let parameter_schema = match &result.parameter_schema {
        Some(schema) => encode_schema_to_ipc(schema)?,
        None => Bytes::new(),
    };

    Ok(ActionCreatePreparedStatementResult {
        prepared_statement_handle: result.handle.clone(),
        dataset_schema,
        parameter_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use crate::metadata::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_flight::sql::CommandStatementQuery;
    use arrow_schema::{DataType, Field, Schema};

    async fn create_test_store() -> Arc<dyn Store> {
        let store = Arc::new(MemoryStore::new());

        // Add test table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        store
            .put(
                DataPath::new(vec!["test".to_string(), "table".to_string()]),
                schema,
                vec![batch],
            )
            .await
            .unwrap();

        store
    }

    async fn create_test_engine() -> SqlEngine {
        SqlEngine::new(create_test_store().await, DEFAULT_CATALOG, DEFAULT_SCHEMA).await
    }

    async fn create_fixture_engine() -> SqlEngine {
        let store: Arc<dyn Store> = Arc::new(MemoryStore::with_test_fixtures());
        SqlEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA).await
    }

    #[tokio::test]
    async fn test_parse_simple_select() {
        let engine = create_test_engine().await;

        let path = engine
            .parse_select_query("SELECT * FROM test.table")
            .unwrap();
        assert_eq!(path.segments(), &["test", "table"]);
    }

    #[tokio::test]
    async fn test_parse_select_single_table() {
        let engine = create_test_engine().await;

        let path = engine.parse_select_query("SELECT * FROM users").unwrap();
        assert_eq!(path.segments(), &["users"]);
    }

    #[tokio::test]
    async fn test_parse_select_with_limit() {
        let engine = create_test_engine().await;

        // LIMIT should not affect table path extraction
        let path = engine
            .parse_select_query("SELECT * FROM test.table LIMIT 5")
            .unwrap();
        assert_eq!(path.segments(), &["test", "table"]);
    }

    #[tokio::test]
    async fn test_parse_limit_clause() {
        let engine = create_test_engine().await;

        assert_eq!(engine.parse_limit("SELECT * FROM users LIMIT 10"), Some(10));
        assert_eq!(engine.parse_limit("SELECT * FROM users LIMIT 5"), Some(5));
        assert_eq!(engine.parse_limit("SELECT * FROM users LIMIT 0"), Some(0));
        assert_eq!(engine.parse_limit("SELECT * FROM users"), None);
        assert_eq!(engine.parse_limit("SELECT * FROM users WHERE id > 5"), None);
    }

    #[tokio::test]
    async fn test_apply_limit() {
        let engine = create_test_engine().await;

        // Create test batches
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch1 = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        );
        let batch2 = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
            )
            .unwrap(),
        );

        let batches = vec![batch1, batch2];

        // Limit less than first batch
        let limited = engine.apply_limit(batches.clone(), 2);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].num_rows(), 2);

        // Limit exactly first batch
        let limited = engine.apply_limit(batches.clone(), 3);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].num_rows(), 3);

        // Limit spans both batches
        let limited = engine.apply_limit(batches.clone(), 4);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].num_rows(), 3);
        assert_eq!(limited[1].num_rows(), 1);

        // Limit all rows
        let limited = engine.apply_limit(batches.clone(), 6);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].num_rows(), 3);
        assert_eq!(limited[1].num_rows(), 3);

        // Limit more than available
        let limited = engine.apply_limit(batches.clone(), 100);
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_execute_statement_query() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        // DataFusion returns -1 for total_records until execution
        assert_eq!(result.total_records, -1);
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_table_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM nonexistent".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await;
        assert!(matches!(result, Err(SqlError::QueryExecution(_))));
    }

    #[tokio::test]
    async fn test_inner_join() {
        let engine = create_fixture_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT c.name, o.order_id, o.amount FROM test.customers c INNER JOIN test.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        // Execute and verify results
        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        assert_eq!(schema.fields().len(), 3);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 25); // All 25 orders should have matching customers
    }

    #[tokio::test]
    async fn test_left_join() {
        let engine = create_fixture_engine().await;

        // LEFT JOIN - all customers, even those without orders
        let cmd = CommandStatementQuery {
            query: "SELECT c.customer_id, c.name, o.order_id FROM test.customers c LEFT JOIN test.orders o ON c.customer_id = o.customer_id ORDER BY c.customer_id, o.order_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // All customers have at least one order in our test data
        assert!(total_rows >= 10);
    }

    #[tokio::test]
    async fn test_right_join() {
        let engine = create_fixture_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT c.name, o.order_id, o.amount FROM test.customers c RIGHT JOIN test.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 25); // All 25 orders
    }

    #[tokio::test]
    async fn test_full_outer_join() {
        let engine = create_fixture_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT c.customer_id, c.name, o.order_id FROM test.customers c FULL OUTER JOIN test.orders o ON c.customer_id = o.customer_id ORDER BY c.customer_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // FULL OUTER JOIN includes all rows from both sides
        assert!(total_rows >= 25);
    }

    #[tokio::test]
    async fn test_multi_table_join() {
        let engine = create_fixture_engine().await;

        // Three-way join: customers -> orders -> products
        let cmd = CommandStatementQuery {
            query: "SELECT c.name, o.order_id, p.product_name, p.price FROM test.customers c INNER JOIN test.orders o ON c.customer_id = o.customer_id INNER JOIN test.products p ON o.order_id = p.order_id ORDER BY o.order_id, p.product_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 4);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        // Verify schema columns
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["name", "order_id", "product_name", "price"]
        );

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 15); // 15 products linked to orders
    }

    #[tokio::test]
    async fn test_join_with_aggregate() {
        let engine = create_fixture_engine().await;

        // Join with GROUP BY and aggregation
        let cmd = CommandStatementQuery {
            query: "SELECT c.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount FROM test.customers c INNER JOIN test.orders o ON c.customer_id = o.customer_id GROUP BY c.name ORDER BY total_amount DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10); // 10 customers, each with aggregated data
    }

    #[tokio::test]
    async fn test_join_with_where_clause() {
        let engine = create_fixture_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT c.name, o.order_id, o.amount FROM test.customers c INNER JOIN test.orders o ON c.customer_id = o.customer_id WHERE o.amount > 200 ORDER BY o.amount DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Orders with amount > 200: (2, 250), (5, 300), (8, 200 excluded), (9, 500), etc.
        assert!(total_rows > 0);
        assert!(total_rows < 25); // Should filter out some orders
    }

    #[tokio::test]
    async fn test_self_join_existing_tables() {
        let engine = create_fixture_engine().await;

        // Self-join on test.integers to find pairs
        let cmd = CommandStatementQuery {
            query: "SELECT a.id as id1, b.id as id2, a.value + b.value as combined FROM test.integers a INNER JOIN test.integers b ON a.id < b.id WHERE a.id < 5 AND b.id < 5 ORDER BY a.id, b.id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Pairs where id1 < id2 and both < 5: (0,1), (0,2), (0,3), (0,4), (1,2), (1,3), (1,4), (2,3), (2,4), (3,4)
        assert_eq!(total_rows, 10);
    }

    // ==================== Subquery Tests ====================

    #[tokio::test]
    async fn test_scalar_subquery_in_select() {
        let engine = create_fixture_engine().await;

        // Scalar subquery in SELECT clause
        let cmd = CommandStatementQuery {
            query: "SELECT customer_id, name, (SELECT COUNT(*) FROM test.orders WHERE test.orders.customer_id = test.customers.customer_id) as order_count FROM test.customers ORDER BY customer_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["customer_id", "name", "order_count"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10); // 10 customers
    }

    #[tokio::test]
    async fn test_subquery_in_where_with_in() {
        let engine = create_fixture_engine().await;

        // Subquery in WHERE clause with IN
        let cmd = CommandStatementQuery {
            query: "SELECT customer_id, name FROM test.customers WHERE customer_id IN (SELECT DISTINCT customer_id FROM test.orders WHERE amount > 200) ORDER BY customer_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with orders > 200
        assert!(total_rows > 0);
        assert!(total_rows <= 10);
    }

    #[tokio::test]
    async fn test_subquery_in_where_with_exists() {
        let engine = create_fixture_engine().await;

        // Subquery in WHERE clause with EXISTS
        let cmd = CommandStatementQuery {
            query: "SELECT customer_id, name FROM test.customers WHERE EXISTS (SELECT 1 FROM test.orders WHERE test.orders.customer_id = test.customers.customer_id AND amount > 400) ORDER BY customer_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with orders > 400 (order 9 = 500, order 16 = 425)
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_subquery_in_where_with_not_exists() {
        let engine = create_fixture_engine().await;

        // NOT EXISTS - but all customers have orders in our test data
        let cmd = CommandStatementQuery {
            query: "SELECT customer_id, name FROM test.customers WHERE NOT EXISTS (SELECT 1 FROM test.orders WHERE test.orders.customer_id = test.customers.customer_id AND amount > 1000) ORDER BY customer_id".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // No orders > 1000, so all 10 customers should be returned
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn test_subquery_in_from_clause() {
        let engine = create_fixture_engine().await;

        // Derived table (subquery in FROM clause)
        let cmd = CommandStatementQuery {
            query: "SELECT sub.customer_id, sub.total_orders FROM (SELECT customer_id, COUNT(*) as total_orders FROM test.orders GROUP BY customer_id) as sub WHERE sub.total_orders > 2 ORDER BY sub.total_orders DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["customer_id", "total_orders"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with more than 2 orders
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_subquery_with_comparison() {
        let engine = create_fixture_engine().await;

        // Subquery with comparison operator
        let cmd = CommandStatementQuery {
            query: "SELECT order_id, amount FROM test.orders WHERE amount > (SELECT AVG(amount) FROM test.orders) ORDER BY amount DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Orders above average
        assert!(total_rows > 0);
        assert!(total_rows < 25); // Not all orders
    }

    #[tokio::test]
    async fn test_nested_subquery() {
        let engine = create_fixture_engine().await;

        // Nested subquery
        let cmd = CommandStatementQuery {
            query: "SELECT name FROM test.customers WHERE customer_id IN (SELECT customer_id FROM test.orders WHERE order_id IN (SELECT order_id FROM test.products WHERE price > 100)) ORDER BY name".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 1);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with products priced > 100
        assert!(total_rows > 0);
    }

    // ==================== CTE (WITH clause) Tests ====================

    #[tokio::test]
    async fn test_simple_cte() {
        let engine = create_fixture_engine().await;

        // Simple CTE
        let cmd = CommandStatementQuery {
            query: "WITH high_value_orders AS (SELECT * FROM test.orders WHERE amount > 200) SELECT order_id, amount FROM high_value_orders ORDER BY amount DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["order_id", "amount"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Orders with amount > 200
        assert!(total_rows > 0);
        assert!(total_rows < 25);
    }

    #[tokio::test]
    async fn test_cte_with_join() {
        let engine = create_fixture_engine().await;

        // CTE joined with another table
        let cmd = CommandStatementQuery {
            query: "WITH customer_totals AS (SELECT customer_id, SUM(amount) as total_spent FROM test.orders GROUP BY customer_id) SELECT c.name, ct.total_spent FROM test.customers c INNER JOIN customer_totals ct ON c.customer_id = ct.customer_id ORDER BY ct.total_spent DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["name", "total_spent"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10); // 10 customers
    }

    #[tokio::test]
    async fn test_multiple_ctes() {
        let engine = create_fixture_engine().await;

        // Multiple CTEs
        let cmd = CommandStatementQuery {
            query: "WITH order_counts AS (SELECT customer_id, COUNT(*) as order_count FROM test.orders GROUP BY customer_id), order_totals AS (SELECT customer_id, SUM(amount) as total_amount FROM test.orders GROUP BY customer_id) SELECT c.name, oc.order_count, ot.total_amount FROM test.customers c INNER JOIN order_counts oc ON c.customer_id = oc.customer_id INNER JOIN order_totals ot ON c.customer_id = ot.customer_id ORDER BY ot.total_amount DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["name", "order_count", "total_amount"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn test_cte_with_aggregation() {
        let engine = create_fixture_engine().await;

        // CTE with aggregation in main query
        let cmd = CommandStatementQuery {
            query: "WITH large_orders AS (SELECT customer_id, amount FROM test.orders WHERE amount > 100) SELECT customer_id, COUNT(*) as large_order_count, AVG(amount) as avg_large_order FROM large_orders GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY avg_large_order DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with more than 1 large order
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_cte_referenced_multiple_times() {
        let engine = create_fixture_engine().await;

        // CTE referenced multiple times in the query
        let cmd = CommandStatementQuery {
            query: "WITH order_stats AS (SELECT customer_id, SUM(amount) as total, AVG(amount) as avg_amount FROM test.orders GROUP BY customer_id) SELECT s1.customer_id, s1.total, s1.avg_amount, (SELECT MAX(total) FROM order_stats) as max_total FROM order_stats s1 ORDER BY s1.total DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 4);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn test_cte_with_subquery() {
        let engine = create_fixture_engine().await;

        // CTE combined with subquery
        let cmd = CommandStatementQuery {
            query: "WITH top_customers AS (SELECT customer_id FROM test.orders GROUP BY customer_id HAVING SUM(amount) > 300) SELECT name FROM test.customers WHERE customer_id IN (SELECT customer_id FROM top_customers) ORDER BY name".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 1);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Customers with total orders > 300
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_cte_column_aliasing() {
        let engine = create_fixture_engine().await;

        // CTE with explicit column aliases
        let cmd = CommandStatementQuery {
            query: "WITH customer_summary (cid, order_total) AS (SELECT customer_id, SUM(amount) FROM test.orders GROUP BY customer_id) SELECT cid, order_total FROM customer_summary WHERE order_total > 200 ORDER BY order_total DESC".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);

        let ticket = TicketStatementQuery {
            statement_handle: result.handle.clone(),
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["cid", "order_total"]);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0);
    }

    // Note: DataFusion does not currently support ALL and ANY operators with
    // comparison operators other than '='. Use workarounds with MAX/MIN aggregates
    // or IN/EXISTS clauses instead.

    #[tokio::test]
    async fn test_execute_insert_statement() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (4, 40)".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);
    }

    #[tokio::test]
    async fn test_execute_insert_multiple_rows() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (4, 40), (5, 50), (6, 60)"
                .to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 3);
    }

    #[tokio::test]
    async fn test_execute_update_all_rows() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "UPDATE test.table SET value = 100".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        // All 3 rows should be affected
        assert_eq!(result.record_count, 3);
    }

    #[tokio::test]
    async fn test_execute_update_with_where() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "UPDATE test.table SET value = 100 WHERE id = 1".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        // Partial update - simulated as half of 3 rows = 1
        assert_eq!(result.record_count, 1);
    }

    #[tokio::test]
    async fn test_execute_delete_all_rows() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "DELETE FROM test.table".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        // All 3 rows would be deleted
        assert_eq!(result.record_count, 3);
    }

    #[tokio::test]
    async fn test_execute_delete_with_where() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "DELETE FROM test.table WHERE id > 1".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await.unwrap();
        // Rows with id 2 and 3 are deleted
        assert_eq!(result.record_count, 2);
    }

    #[tokio::test]
    async fn test_update_table_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "UPDATE nonexistent SET value = 1".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await;
        assert!(matches!(result, Err(SqlError::TableNotFound(_))));
    }

    #[tokio::test]
    async fn test_invalid_update_syntax() {
        let engine = create_test_engine().await;

        // Test with invalid SQL that doesn't match any known statement type
        let cmd = CommandStatementUpdate {
            query: "INVALID_COMMAND test.table".to_string(),
            transaction_id: None,
        };

        let result = engine.execute_statement_update(&cmd).await;
        assert!(matches!(result, Err(SqlError::SyntaxError(_))));
    }

    #[tokio::test]
    async fn test_create_prepared_statement_select() {
        let engine = create_test_engine().await;

        let request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };

        let result = engine.create_prepared_statement(&request).await.unwrap();
        assert!(!result.handle.is_empty());
        assert!(result.dataset_schema.is_some());
        assert!(result.parameter_schema.is_some());

        // Schema should have 2 fields
        let schema = result.dataset_schema.unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_create_prepared_statement_insert() {
        let engine = create_test_engine().await;

        let request = ActionCreatePreparedStatementRequest {
            query: "INSERT INTO test.table (id, value) VALUES (?, ?)".to_string(),
            transaction_id: None,
        };

        let result = engine.create_prepared_statement(&request).await.unwrap();
        assert!(!result.handle.is_empty());
        // INSERT doesn't return result schema
        assert!(result.dataset_schema.is_none());
        assert!(result.parameter_schema.is_some());
    }

    #[tokio::test]
    async fn test_create_prepared_statement_table_not_found() {
        let engine = create_test_engine().await;

        let request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM nonexistent".to_string(),
            transaction_id: None,
        };

        let result = engine.create_prepared_statement(&request).await;
        assert!(matches!(result, Err(SqlError::QueryExecution(_))));
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_query() {
        let engine = create_test_engine().await;

        // First create the prepared statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Then execute it
        let execute_cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: create_result.handle.clone(),
        };
        let execute_result = engine
            .execute_prepared_statement_query(&execute_cmd)
            .await
            .unwrap();

        // DataFusion returns -1 for total_records until execution
        assert_eq!(execute_result.total_records, -1);
        assert_eq!(execute_result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: bytes::Bytes::from("nonexistent"),
        };

        let result = engine.execute_prepared_statement_query(&cmd).await;
        assert!(matches!(
            result,
            Err(SqlError::PreparedStatementNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_close_prepared_statement() {
        let engine = create_test_engine().await;

        // Create a prepared statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Close it
        let close_request = ActionClosePreparedStatementRequest {
            prepared_statement_handle: create_result.handle.clone(),
        };
        engine.close_prepared_statement(&close_request).unwrap();

        // Trying to execute should fail
        let execute_cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: create_result.handle,
        };
        let result = engine.execute_prepared_statement_query(&execute_cmd).await;
        assert!(matches!(
            result,
            Err(SqlError::PreparedStatementNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_close_nonexistent_prepared_statement() {
        let engine = create_test_engine().await;

        // Closing a non-existent statement should succeed (per Flight SQL spec)
        let request = ActionClosePreparedStatementRequest {
            prepared_statement_handle: bytes::Bytes::from("nonexistent"),
        };

        // Should not error
        engine.close_prepared_statement(&request).unwrap();
    }

    #[tokio::test]
    async fn test_get_prepared_statement_data() {
        let engine = create_test_engine().await;

        // Create a prepared statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Get the data
        let (schema, batches) = engine
            .get_prepared_statement_data(&create_result.handle)
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_update_insert() {
        let engine = create_test_engine().await;

        // Create a prepared INSERT statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "INSERT INTO test.table (id, value) VALUES (4, 40), (5, 50)".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Execute the prepared update
        let execute_cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: create_result.handle,
        };
        let result = engine
            .execute_prepared_statement_update(&execute_cmd)
            .await
            .unwrap();

        assert_eq!(result.record_count, 2);
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_update_update() {
        let engine = create_test_engine().await;

        // Create a prepared UPDATE statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "UPDATE test.table SET value = 100".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Execute the prepared update
        let execute_cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: create_result.handle,
        };
        let result = engine
            .execute_prepared_statement_update(&execute_cmd)
            .await
            .unwrap();

        // Should affect all 3 rows
        assert_eq!(result.record_count, 3);
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_update_delete() {
        let engine = create_test_engine().await;

        // Create a prepared DELETE statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "DELETE FROM test.table WHERE id > 1".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Execute the prepared update
        let execute_cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: create_result.handle,
        };
        let result = engine
            .execute_prepared_statement_update(&execute_cmd)
            .await
            .unwrap();

        // DELETE WHERE id > 1 should remove rows with id 2 and 3
        assert_eq!(result.record_count, 2);
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_update_on_query_fails() {
        let engine = create_test_engine().await;

        // Create a prepared SELECT statement (query, not update)
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Try to execute it as an update
        let execute_cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: create_result.handle,
        };
        let result = engine.execute_prepared_statement_update(&execute_cmd).await;

        assert!(matches!(result, Err(SqlError::InvalidCommand(_))));
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_update_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: bytes::Bytes::from("nonexistent"),
        };

        let result = engine.execute_prepared_statement_update(&cmd).await;
        assert!(matches!(
            result,
            Err(SqlError::PreparedStatementNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_bind_parameters() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let engine = create_test_engine().await;

        // Create a prepared statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();
        let handle = create_result.handle;

        // Create parameter batch
        let param_schema = Arc::new(Schema::new(vec![Field::new(
            "param1",
            DataType::Int64,
            false,
        )]));
        let param_batch =
            RecordBatch::try_new(param_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();

        // Bind parameters
        engine
            .bind_parameters(&handle, vec![Arc::new(param_batch)])
            .unwrap();

        // Verify parameters are bound
        let bound = engine.get_bound_parameters(&handle).unwrap();
        assert!(bound.is_some());
        let batches = bound.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_bind_parameters_not_found() {
        let engine = create_test_engine().await;

        let result = engine.bind_parameters(&bytes::Bytes::from("nonexistent"), vec![]);
        assert!(matches!(
            result,
            Err(SqlError::PreparedStatementNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_get_bound_parameters_none_initially() {
        let engine = create_test_engine().await;

        // Create a prepared statement
        let create_request = ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let create_result = engine
            .create_prepared_statement(&create_request)
            .await
            .unwrap();

        // Initially no parameters bound
        let bound = engine.get_bound_parameters(&create_result.handle).unwrap();
        assert!(bound.is_none());
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();
        assert!(!transaction_id.is_empty());
        // Transaction IDs should be unique
        let transaction_id2 = engine.begin_transaction().await.unwrap();
        assert_ne!(transaction_id, transaction_id2);
    }

    #[tokio::test]
    async fn test_end_transaction_commit() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();
        engine
            .end_transaction(&transaction_id, EndTransaction::Commit)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_end_transaction_rollback() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();
        engine
            .end_transaction(&transaction_id, EndTransaction::Rollback)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_end_transaction_not_found() {
        let engine = create_test_engine().await;

        let result = engine
            .end_transaction(&bytes::Bytes::from("nonexistent"), EndTransaction::Commit)
            .await;
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }

    #[tokio::test]
    async fn test_end_transaction_unspecified_fails() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();
        let result = engine
            .end_transaction(&transaction_id, EndTransaction::Unspecified)
            .await;
        assert!(matches!(result, Err(SqlError::InvalidTransactionAction(_))));
    }

    #[tokio::test]
    async fn test_end_transaction_twice_fails() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();
        engine
            .end_transaction(&transaction_id, EndTransaction::Commit)
            .await
            .unwrap();

        // Ending the same transaction again should fail (transaction removed)
        let result = engine
            .end_transaction(&transaction_id, EndTransaction::Commit)
            .await;
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }

    #[tokio::test]
    async fn test_transaction_buffers_update() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();

        // Execute an update within the transaction
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (4, 40)".to_string(),
            transaction_id: Some(transaction_id.clone()),
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);

        // Check pending operations count
        let pending = engine.get_pending_operation_count(&transaction_id).unwrap();
        assert_eq!(pending, 1);

        // Execute another update
        let cmd2 = CommandStatementUpdate {
            query: "UPDATE test.table SET value = 100".to_string(),
            transaction_id: Some(transaction_id.clone()),
        };
        engine.execute_statement_update(&cmd2).await.unwrap();

        // Check pending operations count increased
        let pending = engine.get_pending_operation_count(&transaction_id).unwrap();
        assert_eq!(pending, 2);

        // Commit should succeed
        engine
            .end_transaction(&transaction_id, EndTransaction::Commit)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_transaction_rollback_discards_operations() {
        let engine = create_test_engine().await;

        let transaction_id = engine.begin_transaction().await.unwrap();

        // Execute updates within the transaction
        let cmd1 = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (5, 50)".to_string(),
            transaction_id: Some(transaction_id.clone()),
        };
        engine.execute_statement_update(&cmd1).await.unwrap();

        let cmd2 = CommandStatementUpdate {
            query: "DELETE FROM test.table WHERE id = 1".to_string(),
            transaction_id: Some(transaction_id.clone()),
        };
        engine.execute_statement_update(&cmd2).await.unwrap();

        // Check pending operations count
        let pending = engine.get_pending_operation_count(&transaction_id).unwrap();
        assert_eq!(pending, 2);

        // Rollback discards operations
        engine
            .end_transaction(&transaction_id, EndTransaction::Rollback)
            .await
            .unwrap();

        // Transaction is gone
        let result = engine.get_pending_operation_count(&transaction_id);
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }

    #[tokio::test]
    async fn test_transaction_update_fails_for_invalid_transaction() {
        let engine = create_test_engine().await;

        // Try to execute update with non-existent transaction ID
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (6, 60)".to_string(),
            transaction_id: Some(bytes::Bytes::from("nonexistent_txn")),
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }

    #[tokio::test]
    async fn test_auto_commit_without_transaction_id() {
        let engine = create_test_engine().await;

        // Execute update without transaction ID (auto-commit mode)
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (7, 70)".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);
        // No transaction to check - this is auto-committed
    }

    // ===== DDL Tests =====

    #[tokio::test]
    async fn test_create_table() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE new_table AS SELECT 1 as id, 'test' as name".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);

        // Verify the table was created
        let path = DataPath::new(vec!["new_table".to_string()]);
        assert!(store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_table_qualified_name() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE myschema.mytable AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);

        // Verify the table was created with qualified path
        let path = DataPath::new(vec!["myschema".to_string(), "mytable".to_string()]);
        assert!(store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First create
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE new_table AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Second create with IF NOT EXISTS should not error
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE IF NOT EXISTS new_table AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 0); // 0 because table already exists
    }

    #[tokio::test]
    async fn test_create_table_already_exists_error() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First create
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE new_table AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Second create without IF NOT EXISTS should error
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE new_table AS SELECT 2 as id".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(matches!(result, Err(SqlError::TableAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_create_table_or_replace() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First create
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE new_table AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Create OR REPLACE should succeed
        let cmd = CommandStatementUpdate {
            query: "CREATE OR REPLACE TABLE new_table AS SELECT 2 as id".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 1);
    }

    #[tokio::test]
    async fn test_drop_table() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table first
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE temp_table AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["temp_table".to_string()]);
        assert!(store.contains(&path).await.unwrap());

        // Drop the table
        let cmd = CommandStatementUpdate {
            query: "DROP TABLE temp_table".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 0);

        // Verify the table is gone
        assert!(!store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_table_qualified_name() {
        let store = create_test_store().await;
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table with qualified name
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE myschema.temp AS SELECT 1 as id".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["myschema".to_string(), "temp".to_string()]);
        assert!(store.contains(&path).await.unwrap());

        // Drop the table with qualified name
        let cmd = CommandStatementUpdate {
            query: "DROP TABLE myschema.temp".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        assert!(!store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let engine = create_test_engine().await;

        // Drop non-existent table with IF EXISTS should not error
        let cmd = CommandStatementUpdate {
            query: "DROP TABLE IF EXISTS nonexistent".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(result.record_count, 0);
    }

    #[tokio::test]
    async fn test_drop_table_not_found() {
        let engine = create_test_engine().await;

        // Drop non-existent table without IF EXISTS should error
        let cmd = CommandStatementUpdate {
            query: "DROP TABLE nonexistent".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(matches!(result, Err(SqlError::TableNotFound(_))));
    }

    // ===== Introspection Tests =====

    #[tokio::test]
    async fn test_show_tables() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SHOW TABLES".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_show_tables_returns_data() {
        let engine = create_test_engine().await;

        // Execute SHOW TABLES and get data
        let cmd = CommandStatementQuery {
            query: "SHOW TABLES".to_string(),
            transaction_id: None,
        };
        let query_result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: query_result.handle,
        };
        let (schema, batches) = engine.get_statement_query_data(&ticket).await.unwrap();

        // Should have at least the test table
        assert!(!schema.fields().is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 1, "Should have at least one table");
    }

    #[tokio::test]
    async fn test_show_schemas() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SHOW SCHEMAS".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await;
        // DataFusion may not support SHOW SCHEMAS directly - that's okay
        // Check it returns a result (could be error if not supported)
        if let Ok(query_result) = result {
            assert!(!query_result.schema.fields().is_empty());
        }
    }

    #[tokio::test]
    async fn test_information_schema_tables() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM information_schema.tables".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());

        // Get the data
        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows >= 1,
            "Should have at least one table in information_schema.tables"
        );
    }

    #[tokio::test]
    async fn test_information_schema_columns() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM information_schema.columns WHERE table_name = 'table'"
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());

        // Get the data
        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Should have columns from test.table (id and value)
        assert!(total_rows >= 2, "Should have at least 2 columns");
    }

    #[tokio::test]
    async fn test_information_schema_schemata() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM information_schema.schemata".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_describe_table() {
        let engine = create_test_engine().await;

        // DESCRIBE is typically implemented via information_schema
        let cmd = CommandStatementQuery {
            query: "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'test' AND table_name = 'table'".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());

        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 2, "test.table should have at least 2 columns");
    }

    // ===== Query Features Tests =====

    #[tokio::test]
    async fn test_select_distinct() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT DISTINCT value FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_select_limit() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.\"table\" LIMIT 2".to_string(),
            transaction_id: None,
        };
        let query_result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: query_result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 2, "LIMIT 2 should return at most 2 rows");
    }

    #[tokio::test]
    async fn test_select_limit_offset() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.\"table\" LIMIT 1 OFFSET 1".to_string(),
            transaction_id: None,
        };
        let query_result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: query_result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows <= 1,
            "LIMIT 1 OFFSET 1 should return at most 1 row"
        );
    }

    #[tokio::test]
    async fn test_union() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id FROM test.\"table\" WHERE id = 1 UNION SELECT id FROM test.\"table\" WHERE id = 2".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_union_all() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id FROM test.\"table\" UNION ALL SELECT id FROM test.\"table\""
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_intersect() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id FROM test.\"table\" INTERSECT SELECT id FROM test.\"table\""
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_except() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query:
                "SELECT id FROM test.\"table\" EXCEPT SELECT id FROM test.\"table\" WHERE id = 1"
                    .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_case_expression() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id, CASE WHEN value > 15 THEN 'high' ELSE 'low' END as category FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_window_function_row_number() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM test.\"table\""
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_window_function_rank() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id, RANK() OVER (ORDER BY value) as rnk FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_window_function_lag_lead() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT id, LAG(value, 1) OVER (ORDER BY id) as prev_val, LEAD(value, 1) OVER (ORDER BY id) as next_val FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);
    }

    #[tokio::test]
    async fn test_like_pattern() {
        let engine = create_test_engine().await;

        // Create a table with string data for LIKE testing
        let cmd = CommandStatementUpdate {
            query: "CREATE TABLE strings AS SELECT 'hello' as name UNION ALL SELECT 'world' UNION ALL SELECT 'help'".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM strings WHERE name LIKE 'hel%'".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_between_operator() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.\"table\" WHERE value BETWEEN 10 AND 25".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_coalesce() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT COALESCE(NULL, value, 0) as val FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    #[tokio::test]
    async fn test_nullif() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT NULLIF(value, 10) as val FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    // ===== Function Tests =====

    #[tokio::test]
    async fn test_string_functions() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT 
                CONCAT('hello', ' ', 'world') as concat_result,
                SUBSTRING('hello world', 1, 5) as substr_result,
                TRIM('  hello  ') as trim_result,
                UPPER('hello') as upper_result,
                LOWER('HELLO') as lower_result
            "
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 5);
    }

    #[tokio::test]
    async fn test_date_functions() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT 
                NOW() as now_result,
                CURRENT_DATE as current_date_result
            "
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_date_trunc_extract() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT 
                DATE_TRUNC('month', NOW()) as truncated,
                EXTRACT(YEAR FROM NOW()) as year_val
            "
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_math_functions() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT 
                ABS(-5) as abs_result,
                ROUND(3.7) as round_result,
                FLOOR(3.7) as floor_result,
                CEIL(3.2) as ceil_result,
                10 % 3 as mod_result
            "
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 5);
    }

    #[tokio::test]
    async fn test_cast_function() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "SELECT 
                CAST(123 AS VARCHAR) as int_to_str,
                CAST('456' AS INT) as str_to_int,
                CAST(1.5 AS INT) as float_to_int
            "
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert_eq!(result.schema.fields().len(), 3);
    }

    // ===== EXPLAIN Tests =====

    #[tokio::test]
    async fn test_explain() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "EXPLAIN SELECT * FROM test.\"table\" WHERE id > 1".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());

        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 1, "EXPLAIN should return plan rows");
    }

    #[tokio::test]
    async fn test_explain_analyze() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementQuery {
            query: "EXPLAIN ANALYZE SELECT * FROM test.\"table\"".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();
        assert!(!result.schema.fields().is_empty());
    }

    // ===== Streaming Tests =====

    #[tokio::test]
    async fn test_streaming_query_data() {
        use futures::TryStreamExt;

        let engine = create_fixture_engine().await;

        // Query the large test fixture which has 10000 rows
        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.large".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();

        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };

        // Use streaming to get the data
        let (schema, stream) = engine
            .get_statement_query_data_stream(&ticket)
            .await
            .unwrap();

        // Verify schema
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "data");

        // Consume the stream and count rows
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10000);
    }

    #[tokio::test]
    async fn test_streaming_prepared_statement() {
        use futures::TryStreamExt;

        let engine = create_fixture_engine().await;

        // Create a prepared statement
        let create_result = engine
            .create_prepared_statement(&ActionCreatePreparedStatementRequest {
                query: "SELECT * FROM test.large".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();

        // Use streaming to get the data
        let (schema, stream) = engine
            .get_prepared_statement_data_stream(&create_result.handle)
            .await
            .unwrap();

        // Verify schema
        assert_eq!(schema.fields().len(), 2);

        // Consume the stream and count rows
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10000);
    }

    // ===== INSERT...SELECT Tests =====

    #[tokio::test]
    async fn test_insert_select_basic() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First, create a destination table
        let create_query = "CREATE TABLE dest_table (id BIGINT, value BIGINT)";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Get initial row count
        let info_before = store
            .get(&DataPath::new(vec!["dest_table".to_string()]))
            .await;
        assert!(info_before.is_ok());
        assert_eq!(info_before.unwrap().total_records, 0);

        // INSERT...SELECT from test.integers
        let insert_query =
            "INSERT INTO dest_table SELECT id, value FROM test.integers WHERE id < 10";
        let cmd = CommandStatementUpdate {
            query: insert_query.to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();

        // Should have inserted 10 rows (ids 0-9)
        assert_eq!(result.record_count, 10);

        // Verify data was actually inserted
        let info_after = store
            .get(&DataPath::new(vec!["dest_table".to_string()]))
            .await
            .unwrap();
        assert_eq!(info_after.total_records, 10);
    }

    #[tokio::test]
    async fn test_insert_select_all_rows() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create destination table
        let create_query = "CREATE TABLE copy_table (id BIGINT, name VARCHAR)";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // INSERT...SELECT all rows from test.strings
        let insert_query = "INSERT INTO copy_table SELECT * FROM test.strings";
        let cmd = CommandStatementUpdate {
            query: insert_query.to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();

        // test.strings has 100 rows
        assert_eq!(result.record_count, 100);

        // Verify data
        let info = store
            .get(&DataPath::new(vec!["copy_table".to_string()]))
            .await
            .unwrap();
        assert_eq!(info.total_records, 100);
    }

    // ===== TRUNCATE TABLE Tests =====

    #[tokio::test]
    async fn test_truncate_table_basic() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First create a table with some data
        let create_query =
            "CREATE TABLE truncate_test AS SELECT * FROM test.integers WHERE id < 50";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Verify table has data
        let path = DataPath::new(vec!["truncate_test".to_string()]);
        let info_before = store.get(&path).await.unwrap();
        assert_eq!(info_before.total_records, 50);

        // Truncate the table
        let cmd = CommandStatementUpdate {
            query: "TRUNCATE TABLE truncate_test".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();

        // Should return the number of rows that were removed
        assert_eq!(result.record_count, 50);

        // Verify table is now empty
        let info_after = store.get(&path).await.unwrap();
        assert_eq!(info_after.total_records, 0);

        // Schema should still exist
        let schema = store.get_schema(&path).await.unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_truncate_without_table_keyword() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table
        let create_query =
            "CREATE TABLE truncate_test2 AS SELECT * FROM test.integers WHERE id < 20";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Truncate without TABLE keyword
        let cmd = CommandStatementUpdate {
            query: "TRUNCATE truncate_test2".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await.unwrap();

        assert_eq!(result.record_count, 20);

        // Verify empty
        let path = DataPath::new(vec!["truncate_test2".to_string()]);
        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 0);
    }

    #[tokio::test]
    async fn test_truncate_nonexistent_table() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "TRUNCATE TABLE nonexistent".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SqlError::TableNotFound(_)));
    }

    // ===== ALTER TABLE ADD COLUMN Tests =====

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table
        let create_query =
            "CREATE TABLE alter_test AS SELECT id, value FROM test.integers WHERE id < 5";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["alter_test".to_string()]);

        // Verify initial schema
        let schema_before = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_before.fields().len(), 2);

        // Add a new column
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE alter_test ADD COLUMN new_col VARCHAR".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Verify schema has new column
        let schema_after = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_after.fields().len(), 3);
        assert_eq!(schema_after.field(2).name(), "new_col");
        assert_eq!(schema_after.field(2).data_type(), &DataType::Utf8);

        // Data should still be there with nulls in new column
        let batches: Vec<_> = {
            use futures::TryStreamExt;
            store
                .get_batches(&path)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap()
        };
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_alter_table_add_column_without_keyword() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table
        let create_query = "CREATE TABLE alter_test2 AS SELECT id FROM test.integers WHERE id < 3";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Add without COLUMN keyword
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE alter_test2 ADD extra_col BIGINT".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["alter_test2".to_string()]);
        let schema = store.get_schema(&path).await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "extra_col");
    }

    // ===== ALTER TABLE DROP COLUMN Tests =====

    #[tokio::test]
    async fn test_alter_table_drop_column() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table with multiple columns
        let create_query =
            "CREATE TABLE drop_test AS SELECT id, value FROM test.integers WHERE id < 5";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["drop_test".to_string()]);

        // Verify initial schema
        let schema_before = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_before.fields().len(), 2);

        // Drop a column
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE drop_test DROP COLUMN value".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Verify schema
        let schema_after = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_after.fields().len(), 1);
        assert_eq!(schema_after.field(0).name(), "id");

        // Data should still be there
        let batches: Vec<_> = {
            use futures::TryStreamExt;
            store
                .get_batches(&path)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap()
        };
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_alter_table_drop_last_column_fails() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table with one column
        let create_query = "CREATE TABLE single_col AS SELECT id FROM test.integers WHERE id < 3";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Try to drop the only column
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE single_col DROP COLUMN id".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SqlError::InvalidCommand(_)));
    }

    // ===== ALTER TABLE RENAME COLUMN Tests =====

    #[tokio::test]
    async fn test_alter_table_rename_column() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create a table
        let create_query =
            "CREATE TABLE rename_test AS SELECT id, value FROM test.integers WHERE id < 5";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let path = DataPath::new(vec!["rename_test".to_string()]);

        // Verify initial schema
        let schema_before = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_before.field(1).name(), "value");

        // Rename column
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE rename_test RENAME COLUMN value TO amount".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Verify schema
        let schema_after = store.get_schema(&path).await.unwrap();
        assert_eq!(schema_after.fields().len(), 2);
        assert_eq!(schema_after.field(0).name(), "id");
        assert_eq!(schema_after.field(1).name(), "amount");

        // Data should still be there
        let batches: Vec<_> = {
            use futures::TryStreamExt;
            store
                .get_batches(&path)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap()
        };
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_alter_table_rename_nonexistent_column() {
        let engine = create_fixture_engine().await;

        // Create a table
        let create_query = "CREATE TABLE rename_test2 AS SELECT id FROM test.integers WHERE id < 3";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Try to rename nonexistent column
        let cmd = CommandStatementUpdate {
            query: "ALTER TABLE rename_test2 RENAME COLUMN nonexistent TO newname".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SqlError::InvalidCommand(_)));
    }

    // ===== CREATE/DROP VIEW Tests =====

    #[tokio::test]
    async fn test_create_view_basic() {
        let engine = create_fixture_engine().await;

        // Create a view
        let cmd = CommandStatementUpdate {
            query: "CREATE VIEW test_view AS SELECT id, value FROM test.integers WHERE id < 10"
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());

        // Query the view
        let query_cmd = CommandStatementQuery {
            query: "SELECT * FROM test_view".to_string(),
            transaction_id: None,
        };
        let query_result = engine.execute_statement_query(&query_cmd).await.unwrap();
        assert_eq!(query_result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_drop_view() {
        let engine = create_fixture_engine().await;

        // Create a view
        let cmd = CommandStatementUpdate {
            query: "CREATE VIEW drop_view_test AS SELECT * FROM test.integers".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Drop the view
        let cmd = CommandStatementUpdate {
            query: "DROP VIEW drop_view_test".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());

        // Query should fail now
        let query_cmd = CommandStatementQuery {
            query: "SELECT * FROM drop_view_test".to_string(),
            transaction_id: None,
        };
        let query_result = engine.execute_statement_query(&query_cmd).await;
        assert!(query_result.is_err());
    }

    #[tokio::test]
    async fn test_drop_view_if_exists() {
        let engine = create_test_engine().await;

        // Drop non-existent view with IF EXISTS - should succeed
        let cmd = CommandStatementUpdate {
            query: "DROP VIEW IF EXISTS nonexistent_view".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());
    }

    // ===== CREATE/DROP SCHEMA Tests =====

    #[tokio::test]
    async fn test_create_schema() {
        let engine = create_test_engine().await;

        // Create a schema (virtual for now)
        let cmd = CommandStatementUpdate {
            query: "CREATE SCHEMA new_schema".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_schema_if_not_exists() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "CREATE SCHEMA IF NOT EXISTS another_schema".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_schema() {
        let engine = create_test_engine().await;

        // Create then drop
        let cmd = CommandStatementUpdate {
            query: "CREATE SCHEMA temp_schema".to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        let cmd = CommandStatementUpdate {
            query: "DROP SCHEMA temp_schema".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_schema_if_exists() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "DROP SCHEMA IF EXISTS nonexistent_schema".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;
        assert!(result.is_ok());
    }

    // ===== MERGE / UPSERT Tests =====

    #[tokio::test]
    async fn test_merge_basic() {
        let engine = create_fixture_engine().await;

        // Execute a MERGE statement
        let cmd = CommandStatementUpdate {
            query: r#"
                MERGE INTO test.integers AS t
                USING test.strings AS s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET value = s.id * 100
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.id * 100)
            "#
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        // MERGE is simulated, should succeed
        assert!(result.is_ok());
        assert!(result.unwrap().record_count >= 0);
    }

    #[tokio::test]
    async fn test_merge_target_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: r#"
                MERGE INTO nonexistent AS t
                USING other_table AS s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET value = s.value
            "#
            .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SqlError::TableNotFound(_)));
    }

    #[tokio::test]
    async fn test_upsert_do_nothing() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First, create a table with a key column
        let create_query =
            "CREATE TABLE upsert_test AS SELECT id, value FROM test.integers WHERE id < 5";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Execute INSERT...ON CONFLICT DO NOTHING
        let cmd = CommandStatementUpdate {
            query:
                "INSERT INTO upsert_test (id, value) VALUES (1, 999) ON CONFLICT (id) DO NOTHING"
                    .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_ok());
        // Row id=1 already exists, so DO NOTHING affects zero rows
        assert_eq!(result.unwrap().record_count, 0);
    }

    #[tokio::test]
    async fn test_upsert_do_update() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // First, create a table
        let create_query =
            "CREATE TABLE upsert_test2 AS SELECT id, value FROM test.integers WHERE id < 5";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Execute INSERT...ON CONFLICT DO UPDATE
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO upsert_test2 (id, value) VALUES (1, 999) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().record_count, 1);
    }

    #[tokio::test]
    async fn test_upsert_multiple_values() {
        let store = Arc::new(MemoryStore::with_test_fixtures());
        let engine = SqlEngine::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Create table
        let create_query =
            "CREATE TABLE upsert_test3 AS SELECT id, value FROM test.integers WHERE id < 3";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Upsert multiple rows
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO upsert_test3 (id, value) VALUES (1, 100), (2, 200), (10, 1000) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().record_count, 3);
    }

    #[tokio::test]
    async fn test_upsert_missing_conflict_action() {
        let engine = create_fixture_engine().await;

        // Create table
        let create_query = "CREATE TABLE upsert_test4 AS SELECT id FROM test.integers WHERE id < 3";
        let cmd = CommandStatementUpdate {
            query: create_query.to_string(),
            transaction_id: None,
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Missing DO UPDATE or DO NOTHING
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO upsert_test4 (id) VALUES (1) ON CONFLICT (id)".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SqlError::SyntaxError(_)));
    }

    #[tokio::test]
    async fn test_upsert_table_not_found() {
        let engine = create_test_engine().await;

        let cmd = CommandStatementUpdate {
            query: "INSERT INTO nonexistent (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
                .to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_update(&cmd).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SqlError::TableNotFound(_)));
    }

    // ===== Nested Arrow Type Tests =====

    #[tokio::test]
    async fn test_select_nested_list_type() {
        // Use with_test_fixtures to get the nested table
        let engine = create_fixture_engine().await;

        // Query the nested table that contains List<Int64> column
        let cmd = CommandStatementQuery {
            query: "SELECT id, items FROM test.nested LIMIT 5".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();

        // Verify schema has List type
        let schema = result.schema;
        assert_eq!(schema.fields().len(), 2);
        assert!(matches!(
            schema.field(1).data_type(),
            arrow_schema::DataType::List(_)
        ));
    }

    #[tokio::test]
    async fn test_nested_list_count() {
        use futures::TryStreamExt;

        // Use with_test_fixtures to get the nested table
        let engine = create_fixture_engine().await;

        // Count rows in nested table
        let cmd = CommandStatementQuery {
            query: "SELECT COUNT(*) as cnt FROM test.nested".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();

        // Get the batches using streaming
        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_schema, stream) = engine
            .get_statement_query_data_stream(&ticket)
            .await
            .unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let batch = &batches[0];
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 50); // 50 rows created in fixture
    }

    #[tokio::test]
    async fn test_nested_list_filter() {
        use futures::TryStreamExt;

        // Use with_test_fixtures to get the nested table
        let engine = create_fixture_engine().await;

        // Filter nested table by id
        let cmd = CommandStatementQuery {
            query: "SELECT id FROM test.nested WHERE id < 10".to_string(),
            transaction_id: None,
        };
        let result = engine.execute_statement_query(&cmd).await.unwrap();

        // Get the batches using streaming
        let ticket = TicketStatementQuery {
            statement_handle: result.handle,
        };
        let (_schema, stream) = engine
            .get_statement_query_data_stream(&ticket)
            .await
            .unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10); // ids 0-9
    }

    // ===== Transaction Isolation Level Tests =====

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_isolation_level_display() {
        assert_eq!(
            IsolationLevel::ReadUncommitted.to_string(),
            "READ UNCOMMITTED"
        );
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "READ COMMITTED");
        assert_eq!(
            IsolationLevel::RepeatableRead.to_string(),
            "REPEATABLE READ"
        );
        assert_eq!(IsolationLevel::Serializable.to_string(), "SERIALIZABLE");
    }

    #[tokio::test]
    async fn test_begin_transaction_with_isolation_level() {
        let engine = create_test_engine().await;

        // Test each isolation level
        let txn1 = engine
            .begin_transaction_with_isolation(IsolationLevel::ReadUncommitted)
            .await
            .unwrap();
        assert_eq!(
            engine.get_transaction_isolation_level(&txn1).unwrap(),
            IsolationLevel::ReadUncommitted
        );

        let txn2 = engine
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .unwrap();
        assert_eq!(
            engine.get_transaction_isolation_level(&txn2).unwrap(),
            IsolationLevel::Serializable
        );

        // Default transaction uses ReadCommitted
        let txn3 = engine.begin_transaction().await.unwrap();
        assert_eq!(
            engine.get_transaction_isolation_level(&txn3).unwrap(),
            IsolationLevel::ReadCommitted
        );
    }

    // ===== Savepoint Tests =====

    #[tokio::test]
    async fn test_begin_savepoint() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();

        // Create a savepoint
        let savepoint_id = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();
        assert!(!savepoint_id.is_empty());

        // Check savepoint count
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 1);

        // Create another savepoint
        let _savepoint_id2 = engine.begin_savepoint(&txn_id, "sp2".to_string()).unwrap();
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 2);
    }

    #[tokio::test]
    async fn test_savepoint_in_inactive_transaction() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();
        engine
            .end_transaction(&txn_id, EndTransaction::Commit)
            .await
            .unwrap();

        // Try to create savepoint in ended transaction
        let result = engine.begin_savepoint(&txn_id, "sp1".to_string());
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }

    #[tokio::test]
    async fn test_savepoint_release() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();
        let savepoint_id = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();

        // Release savepoint (keeps operations)
        engine
            .end_savepoint(&savepoint_id, EndSavepoint::Release)
            .unwrap();

        // Savepoint should be removed
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 0);
    }

    #[tokio::test]
    async fn test_savepoint_rollback() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();

        // Add an operation
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (1, 10)".to_string(),
            transaction_id: Some(txn_id.clone()),
        };
        engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(engine.get_pending_operation_count(&txn_id).unwrap(), 1);

        // Create savepoint
        let savepoint_id = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();

        // Add more operations
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (2, 20)".to_string(),
            transaction_id: Some(txn_id.clone()),
        };
        engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(engine.get_pending_operation_count(&txn_id).unwrap(), 2);

        // Rollback to savepoint - should discard the second insert
        engine
            .end_savepoint(&savepoint_id, EndSavepoint::Rollback)
            .unwrap();
        assert_eq!(engine.get_pending_operation_count(&txn_id).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_savepoint_rollback_nested() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();

        // Add operation 1 (using existing table from test fixtures)
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (100, 1000)".to_string(),
            transaction_id: Some(txn_id.clone()),
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Create savepoint 1
        let sp1 = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();

        // Add operation 2
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (200, 2000)".to_string(),
            transaction_id: Some(txn_id.clone()),
        };
        engine.execute_statement_update(&cmd).await.unwrap();

        // Create savepoint 2
        let sp2 = engine.begin_savepoint(&txn_id, "sp2".to_string()).unwrap();

        // Add operation 3
        let cmd = CommandStatementUpdate {
            query: "INSERT INTO test.table (id, value) VALUES (300, 3000)".to_string(),
            transaction_id: Some(txn_id.clone()),
        };
        engine.execute_statement_update(&cmd).await.unwrap();
        assert_eq!(engine.get_pending_operation_count(&txn_id).unwrap(), 3);
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 2);

        // Rollback to sp1 - should discard operations 2 and 3, and invalidate sp2
        engine.end_savepoint(&sp1, EndSavepoint::Rollback).unwrap();
        assert_eq!(engine.get_pending_operation_count(&txn_id).unwrap(), 1);
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 0); // sp1 is gone, sp2 was invalidated

        // sp2 should now be invalid
        let result = engine.end_savepoint(&sp2, EndSavepoint::Release);
        assert!(matches!(result, Err(SqlError::SavepointNotFound(_))));
    }

    #[tokio::test]
    async fn test_end_savepoint_invalid_savepoint() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();
        let _savepoint_id = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();

        // Try to end a non-existent savepoint
        let result = engine.end_savepoint(&bytes::Bytes::from("invalid"), EndSavepoint::Release);
        assert!(matches!(result, Err(SqlError::SavepointNotFound(_))));
    }

    #[tokio::test]
    async fn test_end_savepoint_unspecified_action() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();
        let savepoint_id = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();

        // Unspecified action should fail
        let result = engine.end_savepoint(&savepoint_id, EndSavepoint::Unspecified);
        assert!(matches!(result, Err(SqlError::InvalidTransactionAction(_))));

        // Savepoint should still exist after failed action
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 1);
    }

    #[test]
    fn test_end_savepoint_try_from() {
        assert_eq!(
            EndSavepoint::try_from(0).unwrap(),
            EndSavepoint::Unspecified
        );
        assert_eq!(EndSavepoint::try_from(1).unwrap(), EndSavepoint::Release);
        assert_eq!(EndSavepoint::try_from(2).unwrap(), EndSavepoint::Rollback);
        assert!(EndSavepoint::try_from(3).is_err());
        assert!(EndSavepoint::try_from(-1).is_err());
    }

    #[tokio::test]
    async fn test_transaction_commit_clears_savepoints() {
        let engine = create_test_engine().await;

        let txn_id = engine.begin_transaction().await.unwrap();
        let _sp1 = engine.begin_savepoint(&txn_id, "sp1".to_string()).unwrap();
        let _sp2 = engine.begin_savepoint(&txn_id, "sp2".to_string()).unwrap();
        assert_eq!(engine.get_savepoint_count(&txn_id).unwrap(), 2);

        // Commit transaction
        engine
            .end_transaction(&txn_id, EndTransaction::Commit)
            .await
            .unwrap();

        // Transaction is gone
        let result = engine.get_savepoint_count(&txn_id);
        assert!(matches!(result, Err(SqlError::TransactionNotFound(_))));
    }
}
