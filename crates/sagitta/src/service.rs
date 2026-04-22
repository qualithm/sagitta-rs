//! Arrow Flight service implementation.

use std::pin::Pin;
use std::sync::Arc;

use crate::{
    AccessLevel, AuthToken, DataPath, FlightDescriptorExt, InMemoryUserStore, User, UserStore,
};
use crate::{Dataset, Store};
use arrow_flight::{
    Action, ActionType, BasicAuth, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightService as FlightServiceTrait,
    sql::{
        ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
        ActionBeginTransactionResult, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, ActionEndSavepointRequest,
        ActionEndTransactionRequest, Any, Command, CommandPreparedStatementQuery,
        DoPutPreparedStatementResult, DoPutUpdateResult, EndTransaction, TicketStatementQuery,
    },
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::transport::CertificateDer;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};
use x509_parser::prelude::*;

use crate::metadata::{DEFAULT_CATALOG, DEFAULT_SCHEMA, MetadataEngine, MetadataQuery};
use crate::sql::{
    EndSavepoint, SqlEngine, create_metadata_ticket, create_prepared_statement_result,
    create_prepared_statement_ticket, create_statement_ticket,
};

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Custom action handler for extending the server with application-specific
/// DoAction RPCs.
///
/// # Lifecycle
///
/// 1. **Registration** — wrap the implementation in `Arc` and pass it to
///    [`SagittaService::register_action`]. Multiple actions can be chained:
///    ```ignore
///    let service = SagittaService::new(store).await
///        .register_action(Arc::new(MyAction))
///        .register_action(Arc::new(AnotherAction));
///    ```
/// 2. **Discovery** — the server exposes every registered action through the
///    `ListActions` RPC. [`action_type`](CustomAction::action_type) becomes the
///    `ActionType.type` field and [`description`](CustomAction::description)
///    becomes its description.
/// 3. **Execution** — when a `DoAction` request arrives whose type string does
///    not match a built-in action, the server iterates registered handlers and
///    calls [`execute`](CustomAction::execute) on the first match.
///
/// # Action type naming
///
/// Action types share a flat namespace with the built-in actions
/// (`healthcheck`, `echo`, `error`, `CreatePreparedStatement`,
/// `ClosePreparedStatement`, `BeginTransaction`, `EndTransaction`,
/// `BeginSavepoint`, `EndSavepoint`, `CancelFlightInfo`). Use a
/// distinctive, lowercase, snake_case name (e.g. `"reindex_table"`) to
/// avoid collisions — built-in actions always take precedence.
///
/// # Errors
///
/// [`execute`](CustomAction::execute) returns `Result<Vec<Bytes>, Status>`.
/// Return a [`tonic::Status`] with an appropriate gRPC code on failure —
/// the server forwards it directly to the client. Use lowercase error
/// messages with no trailing punctuation.
pub trait CustomAction: Send + Sync {
    /// Unique identifier for this action.
    ///
    /// Returned as the `ActionType.type` field in `ListActions` and matched
    /// against the incoming `DoAction` request type.
    fn action_type(&self) -> &str;

    /// Human-readable description returned in `ListActions` responses.
    fn description(&self) -> &str;

    /// Execute the action.
    ///
    /// `body` contains the raw bytes sent by the client in the `Action.body`
    /// field. Returns one or more result chunks that are streamed back as
    /// individual `FlightResult` messages.
    ///
    /// # Errors
    ///
    /// Returns [`tonic::Status`] on failure. The status code and message are
    /// forwarded to the client unchanged.
    fn execute(&self, body: bytes::Bytes) -> Result<Vec<bytes::Bytes>, Status>;
}

/// Arrow Flight service implementation for Sagitta.
///
/// Handles all Flight and Flight SQL RPCs with pluggable storage,
/// authentication, and custom action handlers.
pub struct SagittaService {
    store: Arc<dyn Store>,
    user_store: Arc<dyn UserStore>,
    sql_engine: SqlEngine,
    metadata_engine: MetadataEngine,
    custom_actions: Vec<Arc<dyn CustomAction>>,
}

impl SagittaService {
    /// Create a new Arrow Flight service with the given storage backend.
    ///
    /// Uses default catalog/schema names and test users. For production use,
    /// prefer `Sagitta` builder.
    pub async fn new(store: Arc<dyn Store>) -> Self {
        Self::build(
            store,
            Arc::new(InMemoryUserStore::with_test_users()),
            DEFAULT_CATALOG,
            DEFAULT_SCHEMA,
        )
        .await
    }

    /// Create a new Arrow Flight service with custom user store.
    pub async fn with_user_store(store: Arc<dyn Store>, user_store: Arc<dyn UserStore>) -> Self {
        Self::build(store, user_store, DEFAULT_CATALOG, DEFAULT_SCHEMA).await
    }

    /// Build a service with all configuration options.
    pub(crate) async fn build(
        store: Arc<dyn Store>,
        user_store: Arc<dyn UserStore>,
        catalog_name: &str,
        default_schema: &str,
    ) -> Self {
        Self {
            sql_engine: SqlEngine::new(store.clone(), catalog_name, default_schema).await,
            metadata_engine: MetadataEngine::new(store.clone(), catalog_name, default_schema),
            store,
            user_store,
            custom_actions: Vec::new(),
        }
    }

    /// Register a custom action handler.
    ///
    /// The action is discoverable via `ListActions` and invocable via
    /// `DoAction`. Returns `self` for chaining.
    pub fn register_action(mut self, action: Arc<dyn CustomAction>) -> Self {
        self.custom_actions.push(action);
        self
    }

    /// Extract Bearer token from request metadata.
    ///
    /// Looks for `authorization` header with `Bearer <token>` format.
    fn extract_bearer_token<T>(request: &Request<T>) -> Option<AuthToken> {
        let auth_header = request.metadata().get("authorization")?;
        let auth_str = auth_header.to_str().ok()?;
        auth_str.strip_prefix("Bearer ").map(AuthToken::new)
    }

    /// Authenticate a request using Bearer token.
    ///
    /// Returns the authenticated user or an error.
    #[allow(clippy::result_large_err)]
    fn authenticate_request<T>(&self, request: &Request<T>) -> Result<User, Status> {
        let token = Self::extract_bearer_token(request)
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;

        self.user_store
            .user_for_token(&token)
            .ok_or_else(|| Status::unauthenticated("invalid or expired token"))
    }

    /// Authenticate a request, allowing anonymous access.
    ///
    /// Returns Some(user) if authenticated, None if no auth header present.
    #[allow(dead_code)]
    #[allow(clippy::result_large_err)]
    fn authenticate_request_optional<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<User>, Status> {
        match Self::extract_bearer_token(request) {
            Some(token) => {
                let user = self
                    .user_store
                    .user_for_token(&token)
                    .ok_or_else(|| Status::unauthenticated("invalid or expired token"))?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    /// Extract Common Name (CN) from client certificate.
    #[allow(dead_code)]
    fn extract_client_cert_cn(peer_certs: &[CertificateDer<'static>]) -> Option<String> {
        if peer_certs.is_empty() {
            return None;
        }

        // Parse the first (leaf) certificate
        let cert_der = &peer_certs[0];
        let (_, cert) = X509Certificate::from_der(cert_der.as_ref()).ok()?;

        // Extract Common Name from subject
        for rdn in cert.subject.iter_rdn() {
            for attr in rdn.iter() {
                if attr.attr_type() == &oid_registry::OID_X509_COMMON_NAME {
                    return attr.as_str().ok().map(|s| s.to_string());
                }
            }
        }

        None
    }

    /// Authenticate request using mTLS client certificate.
    ///
    /// Creates a user from the certificate Common Name with full access.
    #[allow(dead_code)]
    #[allow(clippy::result_large_err)]
    fn authenticate_mtls<T>(&self, request: &Request<T>) -> Option<User> {
        let peer_certs = request.peer_certs()?;
        let cn = Self::extract_client_cert_cn(&peer_certs)?;

        debug!(cn = %cn, "authenticated via mTLS client certificate");

        // Create user from certificate CN with full access
        Some(User {
            username: cn,
            access: AccessLevel::FullAccess,
        })
    }

    /// Authenticate request using Bearer token or mTLS client certificate.
    ///
    /// Falls back to mTLS if no Bearer token is present.
    #[allow(dead_code)]
    #[allow(clippy::result_large_err)]
    fn authenticate_request_or_mtls<T>(&self, request: &Request<T>) -> Result<User, Status> {
        // Try Bearer token first
        if let Some(token) = Self::extract_bearer_token(request) {
            return self
                .user_store
                .user_for_token(&token)
                .ok_or_else(|| Status::unauthenticated("invalid or expired token"));
        }

        // Fall back to mTLS
        self.authenticate_mtls(request).ok_or_else(|| {
            Status::unauthenticated("missing authorization header or client certificate")
        })
    }

    /// Convert a stored dataset to a FlightInfo.
    #[allow(clippy::result_large_err)]
    fn dataset_to_info(&self, stored: Dataset) -> Result<FlightInfo, Status> {
        // Create flight descriptor from path
        let descriptor = FlightDescriptor::new_path(stored.path.segments().to_vec());

        // Create ticket for data retrieval (serialised path)
        let ticket = Ticket::new(stored.path.segments().join("/").into_bytes());

        // Create endpoint (empty location = same server)
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(&stored.schema)
            .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(stored.total_records as i64)
            .with_total_bytes(stored.total_bytes as i64);

        Ok(info)
    }

    /// Inner implementation of do_exchange that works with any stream.
    /// This allows testing without tonic's Streaming type.
    #[allow(clippy::result_large_err)]
    async fn do_exchange_inner<S>(&self, mut stream: S) -> Result<Vec<FlightData>, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Unpin + Send + 'static,
    {
        // First message must contain the FlightDescriptor
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty do_exchange stream"))?
            .map_err(|e| Status::invalid_argument(format!("failed to read first message: {e}")))?;

        let descriptor = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("first message must contain descriptor"))?;

        let path = descriptor
            .to_data_path()
            .ok_or_else(|| Status::invalid_argument("only PATH descriptors are supported"))?;

        debug!(path = %path.display(), "do_exchange");

        // Check exchange mode based on path
        let segments = path.segments();
        if segments.first().map(|s| s.as_str()) != Some("exchange") {
            return Err(Status::invalid_argument(
                "do_exchange requires path starting with 'exchange'",
            ));
        }

        let mode = segments.get(1).map(|s| s.as_str()).unwrap_or("echo");

        match mode {
            "echo" => {
                // Echo mode: decode incoming data and return it back
                info!(path = %path.display(), "echo exchange");

                // Reconstruct stream with first message prepended
                let first_stream = futures::stream::once(async { Ok(first) });
                let rest_stream = stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));
                let combined = first_stream
                    .map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))))
                    .chain(rest_stream);

                // Decode flight data into schema and record batches
                let mut decoder =
                    std::pin::pin!(FlightRecordBatchStream::new_from_flight_data(combined));

                // Collect all batches
                let mut batches = Vec::new();
                while let Some(result) = decoder.as_mut().try_next().await.map_err(|e| {
                    Status::invalid_argument(format!("failed to decode flight data: {e}"))
                })? {
                    batches.push(result);
                }

                // Get schema from decoder
                let schema = decoder.schema().ok_or_else(|| {
                    Status::invalid_argument("no schema received in flight data stream")
                })?;

                info!(
                    path = %path.display(),
                    batch_count = batches.len(),
                    "echoing data"
                );

                // Re-encode as FlightData
                let batch_stream =
                    futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));

                let flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
                    .with_schema(schema.clone())
                    .build(batch_stream)
                    .try_collect()
                    .await
                    .map_err(|e| Status::internal(format!("failed to encode flight data: {e}")))?;

                Ok(flight_data)
            }
            other => Err(Status::invalid_argument(format!(
                "unknown exchange mode: {other}"
            ))),
        }
    }

    /// Inner implementation of do_put that works with any stream.
    /// This allows testing without tonic's Streaming type.
    #[allow(clippy::result_large_err)]
    async fn do_put_inner<S>(&self, mut stream: S) -> Result<PutResult, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Unpin + Send + 'static,
    {
        // First message must contain the FlightDescriptor
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty do_put stream"))?
            .map_err(|e| Status::invalid_argument(format!("failed to read first message: {e}")))?;

        let descriptor = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("first message must contain descriptor"))?;

        // Check if this is a CMD descriptor (Flight SQL command)
        if let Some(cmd) = descriptor.command_bytes() {
            // Check if this command includes parameter data
            return self
                .do_put_cmd_with_stream(cmd.clone(), first, stream)
                .await;
        }

        let path = descriptor
            .to_data_path()
            .ok_or_else(|| Status::invalid_argument("descriptor must be PATH or CMD type"))?;

        debug!(path = %path.display(), "do_put");

        // Reconstruct stream with first message prepended
        let first_stream = futures::stream::once(async { Ok(first) });
        let rest_stream = stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));
        let combined = first_stream
            .map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))))
            .chain(rest_stream);

        // Decode flight data into schema and record batches
        let mut decoder = std::pin::pin!(FlightRecordBatchStream::new_from_flight_data(combined));

        // Collect all batches manually so we can access schema after
        let mut batches = Vec::new();
        while let Some(result) =
            decoder.as_mut().try_next().await.map_err(|e| {
                Status::invalid_argument(format!("failed to decode flight data: {e}"))
            })?
        {
            batches.push(result);
        }

        // Get schema from decoder (must have at least received schema message)
        let schema = decoder
            .schema()
            .ok_or_else(|| Status::invalid_argument("no schema received in flight data stream"))?;

        let batch_count = batches.len();
        let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Store the dataset
        self.store
            .put(path.clone(), schema.clone(), batches)
            .await
            .map_err(Status::from)?;

        info!(
            path = %path.display(),
            batch_count,
            total_records,
            "stored dataset"
        );

        Ok(PutResult {
            app_metadata: bytes::Bytes::new(),
        })
    }

    /// Handle Flight SQL commands via CMD descriptor with the remaining stream.
    ///
    /// Some commands like `CommandPreparedStatementQuery` can include parameter data.
    #[allow(clippy::result_large_err)]
    async fn do_put_cmd_with_stream<S>(
        &self,
        cmd: bytes::Bytes,
        first: FlightData,
        stream: S,
    ) -> Result<PutResult, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Unpin + Send + 'static,
    {
        let command = SqlEngine::parse_command(&cmd)?;

        match command {
            Command::CommandPreparedStatementQuery(prepared_cmd) => {
                // Parameter binding: client sends parameter values in the stream
                self.do_put_prepared_statement_parameters(prepared_cmd, first, stream)
                    .await
            }
            // For other commands, delegate to the simpler handler
            _ => self.do_put_cmd(cmd).await,
        }
    }

    /// Handle parameter binding via DoPut for prepared statement queries.
    ///
    /// The client sends parameter values as Arrow IPC data to bind to the statement.
    #[allow(clippy::result_large_err)]
    async fn do_put_prepared_statement_parameters<S>(
        &self,
        cmd: CommandPreparedStatementQuery,
        first: FlightData,
        stream: S,
    ) -> Result<PutResult, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Unpin + Send + 'static,
    {
        let handle = &cmd.prepared_statement_handle;
        debug!(handle = %String::from_utf8_lossy(handle), "binding parameters via DoPut");

        // Check if stream has data (parameter values)
        // First message may contain schema/data or just the descriptor
        let has_data = !first.data_header.is_empty() || !first.data_body.is_empty();

        if has_data {
            // Decode the parameter data from the stream
            let first_stream = futures::stream::once(async { Ok(first) });
            let rest_stream = stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));
            let combined = first_stream
                .map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))))
                .chain(rest_stream);

            let mut decoder =
                std::pin::pin!(FlightRecordBatchStream::new_from_flight_data(combined));

            let mut batches = Vec::new();
            while let Some(result) = decoder.as_mut().try_next().await.map_err(|e| {
                Status::invalid_argument(format!("failed to decode parameter data: {e}"))
            })? {
                batches.push(Arc::new(result));
            }

            if !batches.is_empty() {
                // Bind parameters to the prepared statement
                self.sql_engine.bind_parameters(handle, batches)?;
            }
        }

        // Return DoPutPreparedStatementResult with the handle
        let result = DoPutPreparedStatementResult {
            prepared_statement_handle: Some(handle.clone()),
        };
        let app_metadata = result.encode_to_vec().into();

        info!(
            handle = %String::from_utf8_lossy(handle),
            "parameters bound to prepared statement"
        );

        Ok(PutResult { app_metadata })
    }

    /// Handle Flight SQL commands via CMD descriptor.
    #[allow(clippy::result_large_err)]
    async fn get_flight_info_cmd(&self, cmd: bytes::Bytes) -> Result<Response<FlightInfo>, Status> {
        let command = SqlEngine::parse_command(&cmd)?;

        match command {
            Command::CommandStatementQuery(query_cmd) => {
                debug!(query = %query_cmd.query, "CommandStatementQuery");

                let result = self.sql_engine.execute_statement_query(&query_cmd).await?;

                // Create ticket with statement handle
                let ticket_bytes = create_statement_ticket(result.handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let info = FlightInfo::new()
                    .try_with_schema(&result.schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(result.total_records)
                    .with_total_bytes(-1);

                info!(query = %query_cmd.query, "returning statement query info");

                Ok(Response::new(info))
            }
            Command::CommandPreparedStatementQuery(prepared_cmd) => {
                let handle = &prepared_cmd.prepared_statement_handle;
                debug!(handle = %String::from_utf8_lossy(handle), "CommandPreparedStatementQuery");

                let result = self
                    .sql_engine
                    .execute_prepared_statement_query(&prepared_cmd)
                    .await?;

                // Create ticket with prepared statement handle
                let ticket_bytes = create_prepared_statement_ticket(result.handle.clone());
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let info = FlightInfo::new()
                    .try_with_schema(&result.schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(result.total_records)
                    .with_total_bytes(-1);

                info!(
                    handle = %String::from_utf8_lossy(handle),
                    total_records = result.total_records,
                    "returning prepared statement query info"
                );

                Ok(Response::new(info))
            }
            Command::CommandGetCatalogs(_catalogs_cmd) => {
                debug!("CommandGetCatalogs");

                let query = MetadataQuery::GetCatalogs;
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning catalogs info");

                Ok(Response::new(info))
            }
            Command::CommandGetDbSchemas(schemas_cmd) => {
                debug!(
                    catalog = ?schemas_cmd.catalog,
                    filter = ?schemas_cmd.db_schema_filter_pattern,
                    "CommandGetDbSchemas"
                );

                let query = MetadataQuery::GetDbSchemas {
                    catalog: schemas_cmd.catalog.clone(),
                    schema_filter_pattern: schemas_cmd.db_schema_filter_pattern.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning db schemas info");

                Ok(Response::new(info))
            }
            Command::CommandGetTables(tables_cmd) => {
                debug!(
                    catalog = ?tables_cmd.catalog,
                    db_schema_filter = ?tables_cmd.db_schema_filter_pattern,
                    table_name_filter = ?tables_cmd.table_name_filter_pattern,
                    table_types = ?tables_cmd.table_types,
                    include_schema = tables_cmd.include_schema,
                    "CommandGetTables"
                );

                let query = MetadataQuery::GetTables {
                    catalog: tables_cmd.catalog.clone(),
                    db_schema_filter_pattern: tables_cmd.db_schema_filter_pattern.clone(),
                    table_name_filter_pattern: tables_cmd.table_name_filter_pattern.clone(),
                    table_types: tables_cmd.table_types.clone(),
                    include_schema: tables_cmd.include_schema,
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning tables info");

                Ok(Response::new(info))
            }
            Command::CommandGetTableTypes(_table_types_cmd) => {
                debug!("CommandGetTableTypes");

                let query = MetadataQuery::GetTableTypes;
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning table types info");

                Ok(Response::new(info))
            }
            Command::CommandGetPrimaryKeys(pk_cmd) => {
                debug!(table = %pk_cmd.table, "CommandGetPrimaryKeys");

                let query = MetadataQuery::GetPrimaryKeys {
                    catalog: pk_cmd.catalog.clone(),
                    db_schema: pk_cmd.db_schema.clone(),
                    table: pk_cmd.table.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning primary keys info");

                Ok(Response::new(info))
            }
            Command::CommandGetExportedKeys(ek_cmd) => {
                debug!(table = %ek_cmd.table, "CommandGetExportedKeys");

                let query = MetadataQuery::GetExportedKeys {
                    catalog: ek_cmd.catalog.clone(),
                    db_schema: ek_cmd.db_schema.clone(),
                    table: ek_cmd.table.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning exported keys info");

                Ok(Response::new(info))
            }
            Command::CommandGetImportedKeys(ik_cmd) => {
                debug!(table = %ik_cmd.table, "CommandGetImportedKeys");

                let query = MetadataQuery::GetImportedKeys {
                    catalog: ik_cmd.catalog.clone(),
                    db_schema: ik_cmd.db_schema.clone(),
                    table: ik_cmd.table.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning imported keys info");

                Ok(Response::new(info))
            }
            Command::CommandGetCrossReference(cr_cmd) => {
                debug!(
                    pk_table = %cr_cmd.pk_table,
                    fk_table = %cr_cmd.fk_table,
                    "CommandGetCrossReference"
                );

                let query = MetadataQuery::GetCrossReference {
                    pk_catalog: cr_cmd.pk_catalog.clone(),
                    pk_db_schema: cr_cmd.pk_db_schema.clone(),
                    pk_table: cr_cmd.pk_table.clone(),
                    fk_catalog: cr_cmd.fk_catalog.clone(),
                    fk_db_schema: cr_cmd.fk_db_schema.clone(),
                    fk_table: cr_cmd.fk_table.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning cross reference info");

                Ok(Response::new(info))
            }
            Command::CommandGetSqlInfo(sql_info_cmd) => {
                debug!(info_codes = ?sql_info_cmd.info, "CommandGetSqlInfo");

                let query = MetadataQuery::GetSqlInfo {
                    info: sql_info_cmd.info.clone(),
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning sql info");

                Ok(Response::new(info))
            }
            Command::CommandGetXdbcTypeInfo(xdbc_cmd) => {
                debug!(data_type = ?xdbc_cmd.data_type, "CommandGetXdbcTypeInfo");

                let query = MetadataQuery::GetXdbcTypeInfo {
                    data_type: xdbc_cmd.data_type,
                };
                let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

                // Create ticket with metadata handle
                let handle = query.to_handle();
                let ticket_bytes = create_metadata_ticket(handle);
                let ticket = Ticket::new(ticket_bytes);

                // Create CMD descriptor for the response
                let response_descriptor = FlightDescriptor::new_cmd(cmd);

                let endpoint = FlightEndpoint::new().with_ticket(ticket);

                let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?
                    .with_descriptor(response_descriptor)
                    .with_endpoint(endpoint)
                    .with_total_records(total_records)
                    .with_total_bytes(-1);

                info!(total_records, "returning xdbc type info");

                Ok(Response::new(info))
            }
            Command::Unknown(any) => {
                warn!(type_url = %any.type_url, "unknown Flight SQL command");
                Err(Status::unimplemented(format!(
                    "unsupported command: {}",
                    any.type_url
                )))
            }
            other => {
                let type_url = other.type_url();
                warn!(type_url = %type_url, "unimplemented Flight SQL command");
                Err(Status::unimplemented(format!(
                    "command not yet implemented: {type_url}"
                )))
            }
        }
    }

    /// Handle Flight SQL commands via CMD descriptor in DoPut.
    #[allow(clippy::result_large_err)]
    async fn do_put_cmd(&self, cmd: bytes::Bytes) -> Result<PutResult, Status> {
        let command = SqlEngine::parse_command(&cmd)?;

        match command {
            Command::CommandStatementUpdate(update_cmd) => {
                debug!(query = %update_cmd.query, "CommandStatementUpdate");

                let result = self
                    .sql_engine
                    .execute_statement_update(&update_cmd)
                    .await?;

                // Return DoPutUpdateResult in app_metadata
                let update_result = DoPutUpdateResult {
                    record_count: result.record_count,
                };
                let app_metadata = update_result.encode_to_vec().into();

                info!(
                    query = %update_cmd.query,
                    record_count = result.record_count,
                    "statement update executed"
                );

                Ok(PutResult { app_metadata })
            }
            Command::CommandPreparedStatementUpdate(prepared_cmd) => {
                let handle = &prepared_cmd.prepared_statement_handle;
                debug!(handle = %String::from_utf8_lossy(handle), "CommandPreparedStatementUpdate");

                let result = self
                    .sql_engine
                    .execute_prepared_statement_update(&prepared_cmd)
                    .await?;

                // Return DoPutUpdateResult in app_metadata
                let update_result = DoPutUpdateResult {
                    record_count: result.record_count,
                };
                let app_metadata = update_result.encode_to_vec().into();

                info!(
                    handle = %String::from_utf8_lossy(handle),
                    record_count = result.record_count,
                    "prepared statement update executed"
                );

                Ok(PutResult { app_metadata })
            }
            Command::Unknown(any) => {
                warn!(type_url = %any.type_url, "unknown Flight SQL command in DoPut");
                Err(Status::unimplemented(format!(
                    "unsupported command: {}",
                    any.type_url
                )))
            }
            other => {
                let type_url = other.type_url();
                warn!(type_url = %type_url, "unimplemented Flight SQL command in DoPut");
                Err(Status::unimplemented(format!(
                    "command not yet implemented for DoPut: {type_url}"
                )))
            }
        }
    }

    /// Handle do_get for Flight SQL statement queries.
    async fn do_get_statement_query(
        &self,
        ticket: &TicketStatementQuery,
    ) -> Result<BoxedFlightStream<FlightData>, Status> {
        let handle = &ticket.statement_handle;
        let handle_str = String::from_utf8_lossy(handle);

        // Check if this is a metadata handle (starts with "meta_")
        // Metadata queries return materialized results since they're always small
        if MetadataEngine::is_metadata_handle(handle) {
            debug!(handle = %handle_str, "getting metadata query data");
            let query = MetadataQuery::from_handle(handle).ok_or_else(|| {
                Status::invalid_argument(format!("invalid metadata handle: {handle_str}"))
            })?;
            let (schema, batches) = self.metadata_engine.execute_metadata_query(&query).await?;

            info!(batch_count = batches.len(), "streaming metadata query data");

            // Create stream of RecordBatch results
            let batch_stream =
                futures::stream::iter(batches.into_iter().map(|b| Ok(b.as_ref().clone())));

            // Encode as FlightData stream with schema first
            let flight_stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream)
                .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

            return Ok(Box::pin(flight_stream));
        }

        // For user queries (statement and prepared statement), use streaming
        // to avoid loading large result sets into memory
        let (schema, data_stream) = if handle_str.starts_with("ps_") {
            debug!(handle = %handle_str, "getting prepared statement data stream");
            self.sql_engine
                .get_prepared_statement_data_stream(handle)
                .await?
        } else {
            debug!(handle = %handle_str, "getting statement query data stream");
            self.sql_engine
                .get_statement_query_data_stream(ticket)
                .await?
        };

        info!("streaming query data");

        // Encode the streaming data as FlightData stream with schema first
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(data_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

        Ok(Box::pin(flight_stream))
    }
}

#[tonic::async_trait]
impl FlightServiceTrait for SagittaService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        use futures::StreamExt;

        let mut stream = request.into_inner();

        // Get the first (and typically only) handshake request
        let handshake_request = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty handshake stream"))?
            .map_err(|e| Status::invalid_argument(format!("failed to read handshake: {e}")))?;

        debug!(
            protocol_version = handshake_request.protocol_version,
            payload_len = handshake_request.payload.len(),
            "received handshake request"
        );

        // If payload is empty, allow anonymous access (for servers that don't require auth)
        if handshake_request.payload.is_empty() {
            debug!("empty payload, allowing anonymous handshake");
            let response = HandshakeResponse {
                protocol_version: handshake_request.protocol_version,
                payload: bytes::Bytes::new(),
            };
            let stream = futures::stream::once(async { Ok(response) });
            return Ok(Response::new(Box::pin(stream)));
        }

        // Decode BasicAuth from payload
        let basic_auth = BasicAuth::decode(handshake_request.payload.as_ref())
            .map_err(|e| Status::invalid_argument(format!("invalid BasicAuth payload: {e}")))?;

        debug!(username = %basic_auth.username, "authenticating user");

        // Authenticate
        let user = self
            .user_store
            .authenticate(&basic_auth.username, &basic_auth.password)
            .ok_or_else(|| {
                warn!(username = %basic_auth.username, "authentication failed");
                Status::unauthenticated("invalid credentials")
            })?;

        info!(username = %user.username, access = ?user.access, "user authenticated");

        // Create token
        let token = self.user_store.create_token(&user);

        // Return token in response payload
        let response = HandshakeResponse {
            protocol_version: handshake_request.protocol_version,
            payload: bytes::Bytes::from(token.as_str().to_string()),
        };

        // Create response with bearer token in metadata
        let stream = futures::stream::once(async { Ok(response) });
        let mut response = Response::new(Box::pin(stream) as Self::HandshakeStream);

        // Also add token to response metadata for clients that expect it there
        response.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| Status::internal("failed to create authorization header"))?,
        );

        Ok(response)
    }

    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let criteria = request.into_inner();
        debug!(expression_len = criteria.expression.len(), "list_flights");

        // Get all datasets from store
        let stored_datasets = self
            .store
            .list()
            .await
            .map_err(|e| Status::internal(format!("failed to list datasets: {e}")))?;

        // Apply criteria filter if expression is provided
        let filtered_datasets = if criteria.expression.is_empty() {
            stored_datasets
        } else {
            // Parse filter expression as UTF-8 string (path prefix pattern)
            match std::str::from_utf8(&criteria.expression) {
                Ok(filter) => {
                    let filter = filter.trim();
                    debug!(filter = %filter, "applying criteria filter");
                    stored_datasets
                        .into_iter()
                        .filter(|f| {
                            let path_str = f.path.display();
                            path_str.starts_with(filter) || path_str.contains(filter)
                        })
                        .collect()
                }
                Err(_) => {
                    // Invalid UTF-8, return all datasets
                    warn!("criteria expression is not valid UTF-8, ignoring filter");
                    stored_datasets
                }
            }
        };

        // Convert to FlightInfo
        let flight_infos: Vec<FlightInfo> = filtered_datasets
            .into_iter()
            .map(|f| self.dataset_to_info(f))
            .collect::<Result<Vec<_>, _>>()?;

        info!(count = flight_infos.len(), "returning dataset list");

        let stream = futures::stream::iter(flight_infos.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // Handle CMD descriptors (Flight SQL commands)
        if let Some(cmd_bytes) = descriptor.command_bytes() {
            return self.get_flight_info_cmd(cmd_bytes.clone()).await;
        }

        // Handle PATH descriptors (regular datasets)
        let path = descriptor
            .to_data_path()
            .ok_or_else(|| Status::invalid_argument("descriptor must have path or cmd"))?;

        debug!(path = %path.display(), "get_flight_info");

        // Look up dataset in store
        let stored = self.store.get(&path).await.map_err(Status::from)?;

        let info = self.dataset_to_info(stored)?;

        info!(path = %path.display(), "returning dataset info");

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let descriptor = request.into_inner();

        // Handle CMD descriptors (Flight SQL commands)
        let info = if let Some(cmd_bytes) = descriptor.command_bytes() {
            self.get_flight_info_cmd(cmd_bytes.clone())
                .await?
                .into_inner()
        } else {
            // Handle PATH descriptors (regular datasets)
            let path = descriptor
                .to_data_path()
                .ok_or_else(|| Status::invalid_argument("descriptor must have path or cmd"))?;

            debug!(path = %path.display(), "poll_flight_info");

            let stored = self.store.get(&path).await.map_err(Status::from)?;
            self.dataset_to_info(stored)?
        };

        // Since all queries in Sagitta are synchronous, return completed results.
        // No flight_descriptor means the query is complete.
        let poll_info = PollInfo::new()
            .with_info(info)
            .try_with_progress(1.0)
            .map_err(|e| Status::internal(format!("failed to set progress: {e}")))?;

        info!("poll_flight_info returning completed result");

        Ok(Response::new(poll_info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        // Extract path from descriptor
        let path = descriptor
            .to_data_path()
            .ok_or_else(|| Status::invalid_argument("only PATH descriptors are supported"))?;

        debug!(path = %path.display(), "get_schema");

        // Look up schema in store
        let schema = self.store.get_schema(&path).await.map_err(Status::from)?;

        // Convert to IPC format
        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(&schema, &options);
        let result: SchemaResult =
            schema_ipc
                .try_into()
                .map_err(|e: arrow_schema::ArrowError| {
                    Status::internal(format!("failed to encode schema: {e}"))
                })?;

        info!(path = %path.display(), "returning schema");

        Ok(Response::new(result))
    }

    type DoGetStream = BoxedFlightStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Try to parse as TicketStatementQuery (Flight SQL)
        if let Ok(stmt_ticket) = TicketStatementQuery::decode(ticket.ticket.as_ref()) {
            debug!("do_get: TicketStatementQuery");
            let stream = self.do_get_statement_query(&stmt_ticket).await?;
            return Ok(Response::new(stream));
        }

        // Fall back to PATH-based ticket (path encoded as "segment1/segment2/...")
        let path_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("invalid ticket encoding"))?;

        let segments: Vec<String> = path_str.split('/').map(String::from).collect();
        let path = DataPath::new(segments);

        debug!(path = %path.display(), "do_get");

        // Get schema and batches from store
        let schema = self.store.get_schema(&path).await.map_err(Status::from)?;
        let batches = self.store.get_batches(&path).await.map_err(Status::from)?;

        info!(path = %path.display(), batch_count = batches.len(), "streaming data");

        // Create stream of RecordBatch results
        let batch_stream =
            futures::stream::iter(batches.into_iter().map(|b| Ok(b.as_ref().clone())));

        // Encode as FlightData stream with schema first
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    type DoPutStream = BoxedFlightStream<PutResult>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Authenticate and authorise write access
        let user = self.authenticate_request(&request)?;
        if !user.can_write() {
            return Err(Status::permission_denied("write access required"));
        }

        let stream = request.into_inner();
        let result = self.do_put_inner(stream).await?;
        let stream = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        // Authenticate and authorise write access for exchange
        let user = self.authenticate_request(&request)?;
        if !user.can_write() {
            return Err(Status::permission_denied("write access required"));
        }

        let stream = request.into_inner();
        let flight_data = self.do_exchange_inner(stream).await?;
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        debug!(action_type = %action.r#type, body_len = action.body.len(), "do_action");

        match action.r#type.as_str() {
            "healthcheck" => {
                info!("healthcheck action");
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(r#"{"status": "ok"}"#),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "echo" => {
                info!(body_len = action.body.len(), "echo action");
                let result = arrow_flight::Result { body: action.body };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "error" => {
                info!("error action (intentional)");
                Err(Status::internal("intentional error for testing"))
            }
            "CreatePreparedStatement" => {
                debug!("CreatePreparedStatement action");

                // Decode the request from Any wrapper
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let request: ActionCreatePreparedStatementRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode CreatePreparedStatement request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "action body is not CreatePreparedStatement request",
                        )
                    })?;

                // Create the prepared statement
                let result = self.sql_engine.create_prepared_statement(&request).await?;

                // Encode the response
                let response = create_prepared_statement_result(&result)?;
                let response_any = Any::pack(&response)
                    .map_err(|e| Status::internal(format!("failed to encode response: {e}")))?;

                info!(
                    handle = %String::from_utf8_lossy(&result.handle),
                    "prepared statement created"
                );

                let result = arrow_flight::Result {
                    body: response_any.encode_to_vec().into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "ClosePreparedStatement" => {
                debug!("ClosePreparedStatement action");

                // Decode the request from Any wrapper
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let request: ActionClosePreparedStatementRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode ClosePreparedStatement request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "action body is not ClosePreparedStatement request",
                        )
                    })?;

                // Close the prepared statement
                self.sql_engine.close_prepared_statement(&request)?;

                info!(
                    handle = %String::from_utf8_lossy(&request.prepared_statement_handle),
                    "prepared statement closed"
                );

                // Return empty result per Flight SQL spec
                let stream = futures::stream::empty();
                Ok(Response::new(Box::pin(stream)))
            }
            "BeginTransaction" => {
                debug!("BeginTransaction action");

                // Decode the request from Any wrapper (empty message, but must be valid)
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let _request: ActionBeginTransactionRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode BeginTransaction request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument("action body is not BeginTransaction request")
                    })?;

                // Begin the transaction
                let transaction_id = self.sql_engine.begin_transaction()?;

                // Encode the response
                let response = ActionBeginTransactionResult {
                    transaction_id: transaction_id.clone(),
                };
                let response_any = Any::pack(&response)
                    .map_err(|e| Status::internal(format!("failed to encode response: {e}")))?;

                info!(
                    transaction_id = %String::from_utf8_lossy(&transaction_id),
                    "transaction started"
                );

                let result = arrow_flight::Result {
                    body: response_any.encode_to_vec().into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "EndTransaction" => {
                debug!("EndTransaction action");

                // Decode the request from Any wrapper
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let request: ActionEndTransactionRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode EndTransaction request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument("action body is not EndTransaction request")
                    })?;

                // Convert the action enum
                let action = EndTransaction::try_from(request.action).map_err(|_| {
                    Status::invalid_argument(format!(
                        "invalid EndTransaction action value: {}",
                        request.action
                    ))
                })?;

                // End the transaction
                self.sql_engine
                    .end_transaction(&request.transaction_id, action)?;

                info!(
                    transaction_id = %String::from_utf8_lossy(&request.transaction_id),
                    action = request.action,
                    "transaction ended"
                );

                // Return empty result per Flight SQL spec
                let stream = futures::stream::empty();
                Ok(Response::new(Box::pin(stream)))
            }
            "BeginSavepoint" => {
                debug!("BeginSavepoint action");

                // Decode the request from Any wrapper
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let request: ActionBeginSavepointRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode BeginSavepoint request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument("action body is not BeginSavepoint request")
                    })?;

                // Create the savepoint
                let savepoint_id = self
                    .sql_engine
                    .begin_savepoint(&request.transaction_id, request.name.clone())?;

                // Encode the response
                let response = ActionBeginSavepointResult {
                    savepoint_id: savepoint_id.clone(),
                };
                let response_any = Any::pack(&response)
                    .map_err(|e| Status::internal(format!("failed to encode response: {e}")))?;

                info!(
                    transaction_id = %String::from_utf8_lossy(&request.transaction_id),
                    savepoint_id = %String::from_utf8_lossy(&savepoint_id),
                    savepoint_name = %request.name,
                    "savepoint created"
                );

                let result = arrow_flight::Result {
                    body: response_any.encode_to_vec().into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "EndSavepoint" => {
                debug!("EndSavepoint action");

                // Decode the request from Any wrapper
                let any = Any::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(format!("failed to decode Any: {e}")))?;
                let request: ActionEndSavepointRequest = any
                    .unpack()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode EndSavepoint request: {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::invalid_argument("action body is not EndSavepoint request")
                    })?;

                // Convert the action enum
                let action = EndSavepoint::try_from(request.action)?;

                // End the savepoint
                self.sql_engine
                    .end_savepoint(&request.savepoint_id, action)?;

                info!(
                    savepoint_id = %String::from_utf8_lossy(&request.savepoint_id),
                    action = request.action,
                    "savepoint ended"
                );

                // Return empty result per Flight SQL spec
                let stream = futures::stream::empty();
                Ok(Response::new(Box::pin(stream)))
            }
            "CancelFlightInfo" => {
                debug!("CancelFlightInfo action");

                // Decode the request
                let request =
                    CancelFlightInfoRequest::decode(action.body.as_ref()).map_err(|e| {
                        Status::invalid_argument(format!(
                            "failed to decode CancelFlightInfoRequest: {e}"
                        ))
                    })?;

                // Log the cancellation request
                if let Some(ref flight_info) = request.info {
                    let descriptor_str = flight_info
                        .flight_descriptor
                        .as_ref()
                        .map(|d| format!("{:?}", d))
                        .unwrap_or_else(|| "none".to_string());
                    debug!(descriptor = %descriptor_str, "cancellation requested");
                }

                // For now, we don't have long-running queries to cancel,
                // so return NOT_CANCELLABLE for any request.
                // In a real implementation, we'd track running queries and cancel them.
                let result = CancelFlightInfoResult::new(CancelStatus::NotCancellable);

                info!("cancel request: operation not cancellable");

                let flight_result = arrow_flight::Result {
                    body: result.encode_to_vec().into(),
                };
                let stream = futures::stream::once(async { Ok(flight_result) });
                Ok(Response::new(Box::pin(stream)))
            }
            other => {
                // Check custom actions
                for action_handler in &self.custom_actions {
                    if action_handler.action_type() == other {
                        debug!(action_type = %other, "executing custom action");
                        let results = action_handler.execute(action.body)?;
                        let flight_results: Vec<arrow_flight::Result> = results
                            .into_iter()
                            .map(|body| arrow_flight::Result { body })
                            .collect();
                        let stream = futures::stream::iter(flight_results.into_iter().map(Ok));
                        return Ok(Response::new(Box::pin(stream)));
                    }
                }

                warn!(action_type = %other, "unknown action type");
                Err(Status::invalid_argument(format!(
                    "unknown action type: {other}"
                )))
            }
        }
    }

    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        debug!("list_actions");

        let mut actions = vec![
            ActionType {
                r#type: "healthcheck".to_string(),
                description: "Check server health. Returns {\"status\": \"ok\"}.".to_string(),
            },
            ActionType {
                r#type: "echo".to_string(),
                description: "Echo back the request body.".to_string(),
            },
            ActionType {
                r#type: "error".to_string(),
                description: "Return an INTERNAL error for testing.".to_string(),
            },
            ActionType {
                r#type: "CreatePreparedStatement".to_string(),
                description:
                    "Create a prepared statement. Body: ActionCreatePreparedStatementRequest."
                        .to_string(),
            },
            ActionType {
                r#type: "ClosePreparedStatement".to_string(),
                description:
                    "Close a prepared statement. Body: ActionClosePreparedStatementRequest."
                        .to_string(),
            },
            ActionType {
                r#type: "BeginTransaction".to_string(),
                description: "Begin a new transaction. Body: ActionBeginTransactionRequest."
                    .to_string(),
            },
            ActionType {
                r#type: "EndTransaction".to_string(),
                description:
                    "End a transaction (commit or rollback). Body: ActionEndTransactionRequest."
                        .to_string(),
            },
            ActionType {
                r#type: "BeginSavepoint".to_string(),
                description:
                    "Create a savepoint within a transaction. Body: ActionBeginSavepointRequest."
                        .to_string(),
            },
            ActionType {
                r#type: "EndSavepoint".to_string(),
                description:
                    "End a savepoint (release or rollback). Body: ActionEndSavepointRequest."
                        .to_string(),
            },
            ActionType {
                r#type: "CancelFlightInfo".to_string(),
                description: "Cancel a running query. Body: CancelFlightInfoRequest.".to_string(),
            },
        ];

        // Add custom actions
        for action in &self.custom_actions {
            actions.push(ActionType {
                r#type: action.action_type().to_string(),
                description: action.description().to_string(),
            });
        }

        info!(count = actions.len(), "returning available actions");

        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::BasicAuth;
    use prost::Message;

    fn encode_basic_auth(username: &str, password: &str) -> bytes::Bytes {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        bytes::Bytes::from(auth.encode_to_vec())
    }

    #[test]
    fn test_basic_auth_encoding() {
        let payload = encode_basic_auth("admin", "admin123");
        let decoded = BasicAuth::decode(payload.as_ref()).unwrap();
        assert_eq!(decoded.username, "admin");
        assert_eq!(decoded.password, "admin123");
    }

    #[test]
    fn test_user_store_authentication() {
        let store = InMemoryUserStore::with_test_users();

        // Valid admin
        let user = store.authenticate("admin", "admin123");
        assert!(user.is_some());
        let user = user.unwrap();
        assert!(user.can_write());

        // Valid reader
        let user = store.authenticate("reader", "reader123");
        assert!(user.is_some());
        let user = user.unwrap();
        assert!(!user.can_write());

        // Invalid credentials
        assert!(store.authenticate("admin", "wrong").is_none());
        assert!(store.authenticate("unknown", "password").is_none());
    }

    #[test]
    fn test_token_generation_and_lookup() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("admin", "admin123").unwrap();
        let token = store.create_token(&user);

        // Token should look up the user
        let looked_up = store.user_for_token(&token);
        assert!(looked_up.is_some());
        assert_eq!(looked_up.unwrap().username, "admin");

        // Invalid token should fail
        let bad_token = crate::AuthToken::new("invalid");
        assert!(store.user_for_token(&bad_token).is_none());
    }

    #[test]
    fn test_unauthenticated_error_code() {
        use crate::Error;

        // Verify the Unauthenticated error maps to the correct gRPC status
        let err = Error::Unauthenticated("invalid credentials".to_string());
        let status: Status = err.into();

        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(status.message().contains("invalid credentials"));
    }

    #[tokio::test]
    async fn test_extract_bearer_token_valid() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create request with valid Bearer token
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer test-token-123").unwrap(),
        );

        let token = SagittaService::extract_bearer_token(&request);
        assert!(token.is_some());
        assert_eq!(token.unwrap().as_str(), "test-token-123");

        // Verify service is created (silence unused variable warning)
        let _ = service;
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        // Request without authorization header
        let request: Request<()> = Request::new(());

        let token = SagittaService::extract_bearer_token(&request);
        assert!(token.is_none());
    }

    #[test]
    fn test_extract_bearer_token_wrong_scheme() {
        use tonic::metadata::MetadataValue;

        // Request with Basic auth instead of Bearer
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Basic dXNlcjpwYXNz").unwrap(),
        );

        let token = SagittaService::extract_bearer_token(&request);
        assert!(token.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_request_valid_token() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let user_store = Arc::new(InMemoryUserStore::with_test_users());

        // Create a token for admin user
        let admin = user_store.authenticate("admin", "admin123").unwrap();
        let token = user_store.create_token(&admin);

        let service = SagittaService::with_user_store(store, user_store).await;

        // Create request with the valid token
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        let result = service.authenticate_request(&request);
        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.username, "admin");
        assert!(user.can_write());
    }

    #[tokio::test]
    async fn test_authenticate_request_invalid_token() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create request with invalid token
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer invalid-token").unwrap(),
        );

        let result = service.authenticate_request(&request);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn test_authenticate_request_missing_header() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let request: Request<()> = Request::new(());

        let result = service.authenticate_request(&request);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn test_authenticate_request_optional_with_token() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let user_store = Arc::new(InMemoryUserStore::with_test_users());

        // Create a token for reader user
        let reader = user_store.authenticate("reader", "reader123").unwrap();
        let token = user_store.create_token(&reader);

        let service = SagittaService::with_user_store(store, user_store).await;

        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        let result = service.authenticate_request_optional(&request);
        assert!(result.is_ok());
        let user = result.unwrap();
        assert!(user.is_some());
        assert_eq!(user.unwrap().username, "reader");
    }

    #[tokio::test]
    async fn test_authenticate_request_optional_without_token() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let request: Request<()> = Request::new(());

        let result = service.authenticate_request_optional(&request);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_extract_client_cert_cn_valid() {
        use rcgen::{CertificateParams, KeyPair};

        // Generate a self-signed certificate with CN=test-client at test time
        let mut params = CertificateParams::default();
        params.distinguished_name.push(
            rcgen::DnType::CommonName,
            rcgen::DnValue::Utf8String("test-client".to_string()),
        );

        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = cert.der().to_vec();

        let cert = CertificateDer::from(cert_der);

        let cn = SagittaService::extract_client_cert_cn(&[cert]);
        assert!(cn.is_some());
        assert_eq!(cn.unwrap(), "test-client");
    }

    #[test]
    fn test_extract_client_cert_cn_empty() {
        let cn = SagittaService::extract_client_cert_cn(&[]);
        assert!(cn.is_none());
    }

    #[tokio::test]
    async fn test_list_flights_returns_test_fixtures() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let criteria = Criteria::default();
        let request = Request::new(criteria);

        let response = service.list_flights(request).await.unwrap();
        let flights: Vec<FlightInfo> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Should have 9 test fixtures (6 original + 3 join test fixtures)
        assert_eq!(flights.len(), 9);

        // Verify paths are correct
        let mut paths: Vec<String> = flights
            .iter()
            .filter_map(|f| f.flight_descriptor.as_ref())
            .map(|d| d.path.join("/"))
            .collect();
        paths.sort();

        assert_eq!(
            paths,
            vec![
                "test/all_types",
                "test/customers",
                "test/empty",
                "test/integers",
                "test/large",
                "test/nested",
                "test/orders",
                "test/products",
                "test/strings"
            ]
        );
    }

    #[tokio::test]
    async fn test_list_flights_integer_fixture_metadata() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let criteria = Criteria::default();
        let request = Request::new(criteria);

        let response = service.list_flights(request).await.unwrap();
        let flights: Vec<FlightInfo> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Find integers flight
        let integers = flights
            .iter()
            .find(|f| {
                f.flight_descriptor
                    .as_ref()
                    .is_some_and(|d| d.path == vec!["test", "integers"])
            })
            .expect("integers fixture not found");

        // Should have 100 records
        assert_eq!(integers.total_records, 100);

        // Should have an endpoint with a ticket
        assert_eq!(integers.endpoint.len(), 1);
        assert!(integers.endpoint[0].ticket.is_some());
    }

    #[tokio::test]
    async fn test_list_flights_with_criteria_filter() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Filter for "integers"
        let criteria = Criteria {
            expression: bytes::Bytes::from("integers"),
        };
        let request = Request::new(criteria);

        let response = service.list_flights(request).await.unwrap();
        let flights: Vec<FlightInfo> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Should only return the integers flight
        assert_eq!(flights.len(), 1);
        let path = &flights[0].flight_descriptor.as_ref().unwrap().path;
        assert_eq!(path, &["test", "integers"]);
    }

    #[tokio::test]
    async fn test_list_flights_with_prefix_filter() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Filter for "test/" prefix to get all test fixtures
        let criteria = Criteria {
            expression: bytes::Bytes::from("test/"),
        };
        let request = Request::new(criteria);

        let response = service.list_flights(request).await.unwrap();
        let flights: Vec<FlightInfo> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Should return all 9 test fixtures
        assert_eq!(flights.len(), 9);
    }

    #[tokio::test]
    async fn test_list_flights_with_no_match_filter() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Filter for something that doesn't exist
        let criteria = Criteria {
            expression: bytes::Bytes::from("nonexistent"),
        };
        let request = Request::new(criteria);

        let response = service.list_flights(request).await.unwrap();
        let flights: Vec<FlightInfo> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Should return empty
        assert!(flights.is_empty());
    }

    #[tokio::test]
    async fn test_get_flight_info_existing_path() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor =
            FlightDescriptor::new_path(vec!["test".to_string(), "integers".to_string()]);
        let request = Request::new(descriptor);

        let response = service.get_flight_info(request).await.unwrap();
        let info = response.into_inner();

        // Verify descriptor matches
        let desc = info.flight_descriptor.as_ref().unwrap();
        assert_eq!(desc.path, vec!["test", "integers"]);

        // Verify metadata
        assert_eq!(info.total_records, 100);

        // Verify endpoint with ticket
        assert_eq!(info.endpoint.len(), 1);
        assert!(info.endpoint[0].ticket.is_some());
    }

    #[tokio::test]
    async fn test_get_flight_info_not_found() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor =
            FlightDescriptor::new_path(vec!["nonexistent".to_string(), "path".to_string()]);
        let request = Request::new(descriptor);

        let result = service.get_flight_info(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_get_flight_info_cmd_not_supported() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor = FlightDescriptor::new_cmd("SELECT * FROM test");
        let request = Request::new(descriptor);

        let result = service.get_flight_info(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_schema_existing_path() {
        use crate::MemoryStore;
        use arrow_schema::Schema;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor =
            FlightDescriptor::new_path(vec!["test".to_string(), "integers".to_string()]);
        let request = Request::new(descriptor);

        let response = service.get_schema(request).await.unwrap();
        let result = response.into_inner();

        // Verify schema can be decoded
        let schema: Schema = (&result).try_into().expect("valid Arrow IPC schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
    }

    #[tokio::test]
    async fn test_get_schema_matches_flight_info() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor =
            FlightDescriptor::new_path(vec!["test".to_string(), "integers".to_string()]);

        // Get schema via get_schema
        let schema_response = service
            .get_schema(Request::new(descriptor.clone()))
            .await
            .unwrap();
        let schema_result = schema_response.into_inner();

        // Get schema via get_flight_info
        let info_response = service
            .get_flight_info(Request::new(descriptor))
            .await
            .unwrap();
        let flight_info = info_response.into_inner();

        // Both should return the same schema bytes
        assert_eq!(schema_result.schema, flight_info.schema);
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let descriptor =
            FlightDescriptor::new_path(vec!["nonexistent".to_string(), "path".to_string()]);
        let request = Request::new(descriptor);

        let result = service.get_schema(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_do_get_simple() {
        use crate::MemoryStore;
        use arrow_flight::decode::FlightRecordBatchStream;
        use arrow_flight::error::FlightError;
        use futures::TryStreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Create ticket from path
        let ticket = Ticket::new("test/integers".as_bytes().to_vec());
        let request = Request::new(ticket);

        let response = service.do_get(request).await.unwrap();
        let stream = response.into_inner();

        // Map Status error to FlightError for decoder
        let stream = stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));

        // Wrap in FlightRecordBatchStream to decode
        let decoded = FlightRecordBatchStream::new_from_flight_data(stream);

        // Collect all batches
        let batches: Vec<_> = decoded.try_collect().await.unwrap();

        // integers fixture has 1 batch with 100 records
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 100);
    }

    #[tokio::test]
    async fn test_do_get_schema_first() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let ticket = Ticket::new("test/integers".as_bytes().to_vec());
        let request = Request::new(ticket);

        let response = service.do_get(request).await.unwrap();
        let mut stream = response.into_inner();

        // First message should contain schema (non-empty data_header)
        let first = stream.next().await.unwrap().unwrap();
        assert!(
            !first.data_header.is_empty(),
            "first message must contain schema"
        );
    }

    #[tokio::test]
    async fn test_do_get_empty_table() {
        use crate::MemoryStore;
        use arrow_flight::decode::FlightRecordBatchStream;
        use arrow_flight::error::FlightError;
        use futures::TryStreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let ticket = Ticket::new("test/empty".as_bytes().to_vec());
        let request = Request::new(ticket);

        let response = service.do_get(request).await.unwrap();
        let stream = response.into_inner();

        // Map Status error to FlightError for decoder
        let stream = stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));

        let decoded = FlightRecordBatchStream::new_from_flight_data(stream);
        let batches: Vec<_> = decoded.try_collect().await.unwrap();

        // Empty table should have 0 total rows (may have 0 or 1 empty batches)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_do_get_invalid_ticket() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        let ticket = Ticket::new("nonexistent/path".as_bytes().to_vec());
        let request = Request::new(ticket);

        let result = service.do_get(request).await;

        // Should return error before stream creation
        match result {
            Ok(_) => panic!("expected NotFound error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::NotFound),
        }
    }

    #[tokio::test]
    async fn test_do_put_simple() {
        use crate::MemoryStore;
        use arrow_array::builder::Int64Builder;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store.clone()).await;

        // Create test schema and batch
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_value(2);
        builder.append_value(3);

        let batch =
            arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
                .unwrap();

        // Encode as FlightData stream
        let descriptor = FlightDescriptor::new_path(vec!["put".to_string(), "test".to_string()]);

        let batch_stream = futures::stream::once(async move { Ok(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        // Collect into Vec<FlightData>
        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;

        // Create stream and call do_put_inner
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        let result = service.do_put_inner(stream).await.unwrap();

        assert!(result.app_metadata.is_empty());

        // Verify data was stored
        let path = DataPath::from(vec!["put", "test"]);
        assert!(store.contains(&path).await);

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 3);
    }

    #[tokio::test]
    async fn test_do_put_then_do_get() {
        use crate::MemoryStore;
        use arrow_array::builder::Int64Builder;
        use arrow_flight::decode::FlightRecordBatchStream;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_flight::error::FlightError;
        use arrow_schema::{DataType, Field, Schema};
        use futures::{StreamExt, TryStreamExt};

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let mut id_builder = Int64Builder::new();
        let mut value_builder = Int64Builder::new();
        for i in 0..10 {
            id_builder.append_value(i);
            value_builder.append_value(i * 100);
        }

        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .unwrap();

        // Put data using do_put_inner
        let descriptor = FlightDescriptor::new_path(vec!["roundtrip".to_string()]);
        let batch_stream = futures::stream::once(async move { Ok(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        service.do_put_inner(stream).await.unwrap();

        // Get data back
        let ticket = Ticket::new("roundtrip".as_bytes().to_vec());
        let response = service.do_get(Request::new(ticket)).await.unwrap();
        let stream = response
            .into_inner()
            .map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e))));
        let decoded = FlightRecordBatchStream::new_from_flight_data(stream);
        let batches: Vec<_> = decoded.try_collect().await.unwrap();

        // Verify roundtrip
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_do_put_empty_stream_error() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Empty stream
        let stream = futures::stream::empty();

        let result = service.do_put_inner(stream).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_do_put_no_descriptor_error() {
        use crate::MemoryStore;
        use arrow_array::builder::Int64Builder;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create data WITHOUT descriptor
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mut builder = Int64Builder::new();
        builder.append_value(1);

        let batch =
            arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
                .unwrap();

        // Encode WITHOUT descriptor
        let batch_stream = futures::stream::once(async { Ok(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));

        let result = service.do_put_inner(stream).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_list_actions_returns_all_actions() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let response = service.list_actions(Request::new(Empty {})).await.unwrap();
        let actions: Vec<ActionType> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(actions.len(), 10);

        let types: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
        assert!(types.contains(&"healthcheck"));
        assert!(types.contains(&"echo"));
        assert!(types.contains(&"error"));
        assert!(types.contains(&"CreatePreparedStatement"));
        assert!(types.contains(&"ClosePreparedStatement"));
        assert!(types.contains(&"BeginTransaction"));
        assert!(types.contains(&"EndTransaction"));
        assert!(types.contains(&"BeginSavepoint"));
        assert!(types.contains(&"EndSavepoint"));
        assert!(types.contains(&"CancelFlightInfo"));
    }

    #[tokio::test]
    async fn test_do_action_healthcheck() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let action = Action {
            r#type: "healthcheck".to_string(),
            body: bytes::Bytes::new(),
        };

        let response = service.do_action(Request::new(action)).await.unwrap();
        let results: Vec<arrow_flight::Result> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].body.as_ref(), b"{\"status\": \"ok\"}");
    }

    #[tokio::test]
    async fn test_do_action_echo() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let body = bytes::Bytes::from("hello world");
        let action = Action {
            r#type: "echo".to_string(),
            body: body.clone(),
        };

        let response = service.do_action(Request::new(action)).await.unwrap();
        let results: Vec<arrow_flight::Result> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].body, body);
    }

    #[tokio::test]
    async fn test_do_action_error() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let action = Action {
            r#type: "error".to_string(),
            body: bytes::Bytes::new(),
        };

        let result = service.do_action(Request::new(action)).await;
        match result {
            Ok(_) => panic!("expected error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::Internal),
        }
    }

    #[tokio::test]
    async fn test_do_action_unknown_type() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        let action = Action {
            r#type: "unknown_action".to_string(),
            body: bytes::Bytes::new(),
        };

        let result = service.do_action(Request::new(action)).await;
        match result {
            Ok(_) => panic!("expected error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }

    #[tokio::test]
    async fn test_do_action_cancel_flight_info() {
        use crate::MemoryStore;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create a minimal FlightInfo to cancel
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .unwrap()
            .with_descriptor(FlightDescriptor::new_path(vec!["test".to_string()]));

        // Create the cancel request
        let request = CancelFlightInfoRequest::new(flight_info);
        let action = Action {
            r#type: "CancelFlightInfo".to_string(),
            body: request.encode_to_vec().into(),
        };

        let response = service.do_action(Request::new(action)).await.unwrap();
        let results: Vec<arrow_flight::Result> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);

        // Decode the result
        let result = CancelFlightInfoResult::decode(results[0].body.as_ref()).unwrap();
        // Since we don't have long-running operations, expect NOT_CANCELLABLE
        assert_eq!(result.status, CancelStatus::NotCancellable as i32);
    }

    #[tokio::test]
    async fn test_do_exchange_echo_simple() {
        use crate::MemoryStore;
        use arrow_array::builder::Int64Builder;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create test schema and batch
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        builder.append_value(100);

        let batch =
            arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
                .unwrap();

        // Encode as FlightData stream with exchange/echo path
        let descriptor =
            FlightDescriptor::new_path(vec!["exchange".to_string(), "echo".to_string()]);

        let batch_stream = futures::stream::once(async move { Ok(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        // Collect into Vec<FlightData>
        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;

        // Create stream and call do_exchange_inner
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        let result = service.do_exchange_inner(stream).await.unwrap();

        // Should return FlightData messages
        assert!(!result.is_empty());

        // Decode the returned data
        let result_stream = futures::stream::iter(result.into_iter().map(Ok::<_, FlightError>));
        let decoded = FlightRecordBatchStream::new_from_flight_data(result_stream);
        let batches: Vec<_> = decoded.try_collect().await.unwrap();

        // Verify echo worked
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_do_exchange_invalid_path() {
        use crate::MemoryStore;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create minimal schema
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        // Use invalid path (not starting with "exchange")
        let descriptor = FlightDescriptor::new_path(vec!["invalid".to_string()]);

        let batch_stream =
            futures::stream::empty::<Result<arrow_array::RecordBatch, FlightError>>();
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));

        let result = service.do_exchange_inner(stream).await;
        match result {
            Ok(_) => panic!("expected error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }

    #[tokio::test]
    async fn test_do_exchange_unknown_mode() {
        use crate::MemoryStore;
        use arrow_flight::encode::FlightDataEncoderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Create minimal schema
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        // Use unknown exchange mode
        let descriptor =
            FlightDescriptor::new_path(vec!["exchange".to_string(), "unknown".to_string()]);

        let batch_stream =
            futures::stream::empty::<Result<arrow_array::RecordBatch, FlightError>>();
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        let flight_data: Vec<FlightData> = flight_stream.map(|r| r.unwrap()).collect().await;
        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));

        let result = service.do_exchange_inner(stream).await;
        match result {
            Ok(_) => panic!("expected error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }

    #[tokio::test]
    async fn test_do_exchange_empty_stream_error() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store).await;

        // Empty stream should error
        let stream = futures::stream::empty::<Result<FlightData, Status>>();
        let result = service.do_exchange_inner(stream).await;

        match result {
            Ok(_) => panic!("expected error"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }

    #[tokio::test]
    async fn test_write_authorization_denied_for_reader() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let user_store = Arc::new(InMemoryUserStore::with_test_users());

        // Create token for read-only user
        let reader = user_store.authenticate("reader", "reader123").unwrap();
        let token = user_store.create_token(&reader);

        let service = SagittaService::with_user_store(store, user_store).await;

        // Create request with reader token
        let mut request: Request<()> = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        // Authenticate succeeds
        let user = service.authenticate_request(&request).unwrap();
        assert_eq!(user.username, "reader");

        // But write access is denied
        assert!(!user.can_write());

        // This would result in PERMISSION_DENIED in do_put/do_exchange
    }

    #[tokio::test]
    async fn test_write_authorization_granted_for_admin() {
        use crate::MemoryStore;
        use tonic::metadata::MetadataValue;

        let store = Arc::new(MemoryStore::new());
        let user_store = Arc::new(InMemoryUserStore::with_test_users());

        // Create token for admin user
        let admin = user_store.authenticate("admin", "admin123").unwrap();
        let token = user_store.create_token(&admin);

        let service = SagittaService::with_user_store(store, user_store).await;

        // Create request with admin token
        let mut request: Request<()> = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        // Authenticate succeeds
        let user = service.authenticate_request(&request).unwrap();
        assert_eq!(user.username, "admin");

        // Write access is granted
        assert!(user.can_write());
    }

    #[test]
    fn test_permission_denied_error_code() {
        // Verify PERMISSION_DENIED status code is correct
        let status = Status::permission_denied("write access required");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn test_get_flight_info_cmd_statement_query() {
        use crate::MemoryStore;
        use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Create a CommandStatementQuery command
        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.integers".to_string(),
            transaction_id: None,
        };
        let any = cmd.as_any();
        let cmd_bytes = any.encode_to_vec();

        let descriptor = FlightDescriptor::new_cmd(cmd_bytes);
        let request = Request::new(descriptor);

        let response = service.get_flight_info(request).await.unwrap();
        let info = response.into_inner();

        // Verify we have an endpoint with a ticket
        assert_eq!(info.endpoint.len(), 1);
        assert!(!info.endpoint[0].ticket.as_ref().unwrap().ticket.is_empty());

        // DataFusion returns -1 for total_records until execution
        assert_eq!(info.total_records, -1);

        // Verify schema has expected columns (must be last as it consumes info)
        let schema = info.try_decode_schema().expect("valid Arrow IPC schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
    }

    #[tokio::test]
    async fn test_get_flight_info_cmd_table_not_found() {
        use crate::MemoryStore;
        use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Query for non-existent table
        let cmd = CommandStatementQuery {
            query: "SELECT * FROM nonexistent".to_string(),
            transaction_id: None,
        };
        let any = cmd.as_any();
        let cmd_bytes = any.encode_to_vec();

        let descriptor = FlightDescriptor::new_cmd(cmd_bytes);
        let request = Request::new(descriptor);

        let result = service.get_flight_info(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        // DataFusion returns Internal error for table not found during query planning
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn test_do_get_statement_query_ticket() {
        use crate::MemoryStore;
        use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
        use futures::TryStreamExt;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // First get flight info to get a ticket
        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.integers".to_string(),
            transaction_id: None,
        };
        let any = cmd.as_any();
        let cmd_bytes = any.encode_to_vec();

        let descriptor = FlightDescriptor::new_cmd(cmd_bytes);
        let request = Request::new(descriptor);

        let info = service.get_flight_info(request).await.unwrap().into_inner();
        let ticket = info.endpoint[0].ticket.clone().unwrap();

        // Now use the ticket with do_get
        let request = Request::new(ticket);
        let response = service.do_get(request).await.unwrap();
        let stream = response.into_inner();

        // Collect all flight data
        let flight_data: Vec<FlightData> = stream.try_collect().await.unwrap();

        // First message is schema, rest are data
        assert!(!flight_data.is_empty());
    }

    #[tokio::test]
    async fn test_poll_flight_info_path_descriptor() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Poll for test/integers path
        let descriptor =
            FlightDescriptor::new_path(vec!["test".to_string(), "integers".to_string()]);
        let request = Request::new(descriptor);

        let response = service.poll_flight_info(request).await.unwrap();
        let poll_info = response.into_inner();

        // Query is complete (no flight_descriptor for retry)
        assert!(poll_info.flight_descriptor.is_none());

        // Has flight info
        let info = poll_info.info.expect("should have flight info");
        assert_eq!(info.total_records, 100);

        // Progress is 1.0 (complete)
        assert_eq!(poll_info.progress, Some(1.0));
    }

    #[tokio::test]
    async fn test_poll_flight_info_cmd_descriptor() {
        use crate::MemoryStore;
        use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Poll for a Flight SQL query
        let cmd = CommandStatementQuery {
            query: "SELECT * FROM test.strings".to_string(),
            transaction_id: None,
        };
        let any = cmd.as_any();
        let cmd_bytes = any.encode_to_vec();

        let descriptor = FlightDescriptor::new_cmd(cmd_bytes);
        let request = Request::new(descriptor);

        let response = service.poll_flight_info(request).await.unwrap();
        let poll_info = response.into_inner();

        // Query is complete
        assert!(poll_info.flight_descriptor.is_none());
        assert_eq!(poll_info.progress, Some(1.0));

        // Has valid flight info with schema
        let info = poll_info.info.expect("should have flight info");
        let schema = info.try_decode_schema().expect("valid Arrow IPC schema");
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_poll_flight_info_not_found() {
        use crate::MemoryStore;

        let store = Arc::new(MemoryStore::with_test_fixtures());
        let service = SagittaService::new(store).await;

        // Poll for non-existent path
        let descriptor = FlightDescriptor::new_path(vec!["nonexistent".to_string()]);
        let request = Request::new(descriptor);

        let result = service.poll_flight_info(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    /// Example custom action for testing.
    struct UppercaseAction;

    impl CustomAction for UppercaseAction {
        fn action_type(&self) -> &str {
            "uppercase"
        }

        fn description(&self) -> &str {
            "Convert input bytes to uppercase ASCII."
        }

        fn execute(&self, body: bytes::Bytes) -> Result<Vec<bytes::Bytes>, Status> {
            let upper = body.to_ascii_uppercase();
            Ok(vec![bytes::Bytes::from(upper)])
        }
    }

    #[tokio::test]
    async fn test_custom_action_registration() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store)
            .await
            .register_action(Arc::new(UppercaseAction));

        // Verify custom action appears in list_actions
        let response = service.list_actions(Request::new(Empty {})).await.unwrap();
        let actions: Vec<ActionType> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let types: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
        assert!(types.contains(&"uppercase"));
    }

    #[tokio::test]
    async fn test_custom_action_execution() {
        use crate::MemoryStore;
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let service = SagittaService::new(store)
            .await
            .register_action(Arc::new(UppercaseAction));

        let action = Action {
            r#type: "uppercase".to_string(),
            body: bytes::Bytes::from("hello world"),
        };

        let response = service.do_action(Request::new(action)).await.unwrap();
        let results: Vec<arrow_flight::Result> = response
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].body, bytes::Bytes::from("HELLO WORLD"));
    }
}
