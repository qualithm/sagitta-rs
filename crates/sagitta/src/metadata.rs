//! Flight SQL metadata command handling.
//!
//! Implements metadata retrieval commands per the Arrow Flight SQL specification.

use std::sync::{Arc, LazyLock};

use crate::Store;
use arrow_array::{ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray, UInt8Array};
use arrow_flight::sql::metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoDataBuilder};
use arrow_flight::sql::{Nullable, Searchable, SqlInfo, XdbcDataType};
use arrow_flight::{IpcMessage, SchemaAsIpc};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use tracing::{debug, info};

use crate::catalog::StoreCatalog;
use crate::sql::{SqlError, SqlResult};

/// Default catalog name for Sagitta.
pub const DEFAULT_CATALOG: &str = "default";

/// Default schema name for Sagitta.
pub const DEFAULT_SCHEMA: &str = "public";

/// Metadata query handle prefix.
const METADATA_HANDLE_PREFIX: &str = "meta_";

/// Types of metadata queries.
#[derive(Debug, Clone)]
pub enum MetadataQuery {
    /// Get list of catalogs.
    GetCatalogs,
    /// Get list of database schemas.
    GetDbSchemas {
        /// Filter by catalog (None = all catalogs).
        catalog: Option<String>,
        /// Filter pattern for schema names (SQL LIKE syntax).
        schema_filter_pattern: Option<String>,
    },
    /// Get list of tables.
    GetTables {
        /// Filter by catalog (None = all catalogs).
        catalog: Option<String>,
        /// Filter pattern for schema names.
        db_schema_filter_pattern: Option<String>,
        /// Filter pattern for table names.
        table_name_filter_pattern: Option<String>,
        /// Filter by table types (empty = all types).
        table_types: Vec<String>,
        /// Include table schema in results.
        include_schema: bool,
    },
    /// Get list of table types.
    GetTableTypes,
    /// Get primary keys for a table.
    GetPrimaryKeys {
        /// Catalog name.
        catalog: Option<String>,
        /// Schema name.
        db_schema: Option<String>,
        /// Table name.
        table: String,
    },
    /// Get exported keys (foreign keys referencing a table).
    GetExportedKeys {
        /// Catalog name.
        catalog: Option<String>,
        /// Schema name.
        db_schema: Option<String>,
        /// Table name.
        table: String,
    },
    /// Get imported keys (foreign keys from a table).
    GetImportedKeys {
        /// Catalog name.
        catalog: Option<String>,
        /// Schema name.
        db_schema: Option<String>,
        /// Table name.
        table: String,
    },
    /// Get cross-reference between two tables.
    GetCrossReference {
        /// Primary key catalog name.
        pk_catalog: Option<String>,
        /// Primary key schema name.
        pk_db_schema: Option<String>,
        /// Primary key table name.
        pk_table: String,
        /// Foreign key catalog name.
        fk_catalog: Option<String>,
        /// Foreign key schema name.
        fk_db_schema: Option<String>,
        /// Foreign key table name.
        fk_table: String,
    },
    /// Get SQL info metadata.
    GetSqlInfo {
        /// List of info codes to return (empty = all).
        info: Vec<u32>,
    },
    /// Get XDBC type info.
    GetXdbcTypeInfo {
        /// Filter by data type (None = all types).
        data_type: Option<i32>,
    },
}

impl MetadataQuery {
    /// Encode metadata query to a handle for ticket generation.
    pub fn to_handle(&self) -> Bytes {
        match self {
            MetadataQuery::GetCatalogs => Bytes::from(format!("{METADATA_HANDLE_PREFIX}catalogs")),
            MetadataQuery::GetDbSchemas { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}schemas"))
            }
            MetadataQuery::GetTables { include_schema, .. } => {
                if *include_schema {
                    Bytes::from(format!("{METADATA_HANDLE_PREFIX}tables_with_schema"))
                } else {
                    Bytes::from(format!("{METADATA_HANDLE_PREFIX}tables"))
                }
            }
            MetadataQuery::GetTableTypes => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}table_types"))
            }
            MetadataQuery::GetPrimaryKeys { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}primary_keys"))
            }
            MetadataQuery::GetExportedKeys { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}exported_keys"))
            }
            MetadataQuery::GetImportedKeys { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}imported_keys"))
            }
            MetadataQuery::GetCrossReference { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}cross_reference"))
            }
            MetadataQuery::GetSqlInfo { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}sql_info"))
            }
            MetadataQuery::GetXdbcTypeInfo { .. } => {
                Bytes::from(format!("{METADATA_HANDLE_PREFIX}xdbc_type_info"))
            }
        }
    }

    /// Parse a handle back to a metadata query.
    pub fn from_handle(handle: &Bytes) -> Option<Self> {
        let handle_str = String::from_utf8(handle.to_vec()).ok()?;
        if !handle_str.starts_with(METADATA_HANDLE_PREFIX) {
            return None;
        }
        let query_type = &handle_str[METADATA_HANDLE_PREFIX.len()..];
        match query_type {
            "catalogs" => Some(MetadataQuery::GetCatalogs),
            "schemas" => Some(MetadataQuery::GetDbSchemas {
                catalog: None,
                schema_filter_pattern: None,
            }),
            "tables" => Some(MetadataQuery::GetTables {
                catalog: None,
                db_schema_filter_pattern: None,
                table_name_filter_pattern: None,
                table_types: vec![],
                include_schema: false,
            }),
            "tables_with_schema" => Some(MetadataQuery::GetTables {
                catalog: None,
                db_schema_filter_pattern: None,
                table_name_filter_pattern: None,
                table_types: vec![],
                include_schema: true,
            }),
            "table_types" => Some(MetadataQuery::GetTableTypes),
            "primary_keys" => Some(MetadataQuery::GetPrimaryKeys {
                catalog: None,
                db_schema: None,
                table: String::new(),
            }),
            "exported_keys" => Some(MetadataQuery::GetExportedKeys {
                catalog: None,
                db_schema: None,
                table: String::new(),
            }),
            "imported_keys" => Some(MetadataQuery::GetImportedKeys {
                catalog: None,
                db_schema: None,
                table: String::new(),
            }),
            "cross_reference" => Some(MetadataQuery::GetCrossReference {
                pk_catalog: None,
                pk_db_schema: None,
                pk_table: String::new(),
                fk_catalog: None,
                fk_db_schema: None,
                fk_table: String::new(),
            }),
            "sql_info" => Some(MetadataQuery::GetSqlInfo { info: vec![] }),
            "xdbc_type_info" => Some(MetadataQuery::GetXdbcTypeInfo { data_type: None }),
            _ => None,
        }
    }
}

/// Static SQL info data for the server.
static SQL_INFO_DATA: LazyLock<arrow_flight::sql::metadata::SqlInfoData> = LazyLock::new(|| {
    let mut builder = SqlInfoDataBuilder::new();

    // Server identification
    builder.append(SqlInfo::FlightSqlServerName, "Sagitta");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "57.0.0");

    // Server capabilities
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerTransaction, 2i32); // Transaction support with savepoints
    builder.append(SqlInfo::FlightSqlServerCancel, false);

    // SQL features
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(SqlInfo::SqlTransactionsSupported, true);

    builder.build().expect("valid sql info data")
});

/// Static XDBC type info data for the server.
static XDBC_TYPE_INFO_DATA: LazyLock<arrow_flight::sql::metadata::XdbcTypeInfoData> =
    LazyLock::new(|| {
        let mut builder = XdbcTypeInfoDataBuilder::new();

        // Integer types
        builder.append(XdbcTypeInfo {
            type_name: "INT8".into(),
            data_type: XdbcDataType::XdbcTinyint,
            column_size: Some(8),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcTinyint,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "INT16".into(),
            data_type: XdbcDataType::XdbcSmallint,
            column_size: Some(16),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcSmallint,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "INT32".into(),
            data_type: XdbcDataType::XdbcInteger,
            column_size: Some(32),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcInteger,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "INT64".into(),
            data_type: XdbcDataType::XdbcBigint,
            column_size: Some(64),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcBigint,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        // Float types
        builder.append(XdbcTypeInfo {
            type_name: "FLOAT32".into(),
            data_type: XdbcDataType::XdbcFloat,
            column_size: Some(32),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcFloat,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "FLOAT64".into(),
            data_type: XdbcDataType::XdbcDouble,
            column_size: Some(64),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcDouble,
            num_prec_radix: Some(2),
            ..Default::default()
        });

        // String type
        builder.append(XdbcTypeInfo {
            type_name: "UTF8".into(),
            data_type: XdbcDataType::XdbcVarchar,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarchar,
            ..Default::default()
        });

        // Binary type
        builder.append(XdbcTypeInfo {
            type_name: "BINARY".into(),
            data_type: XdbcDataType::XdbcVarbinary,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarbinary,
            ..Default::default()
        });

        // Boolean type
        builder.append(XdbcTypeInfo {
            type_name: "BOOLEAN".into(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcBit,
            ..Default::default()
        });

        // Date/Time types
        builder.append(XdbcTypeInfo {
            type_name: "DATE32".into(),
            data_type: XdbcDataType::XdbcDate,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcDate,
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "TIMESTAMP".into(),
            data_type: XdbcDataType::XdbcTimestamp,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTimestamp,
            ..Default::default()
        });

        builder.build().expect("valid xdbc type info data")
    });

/// Handles Flight SQL metadata commands (catalogs, schemas, tables, SQL info).
pub struct MetadataEngine {
    store: Arc<dyn Store>,
    catalog_name: String,
    default_schema: String,
}

impl MetadataEngine {
    /// Create a new metadata engine.
    pub fn new(store: Arc<dyn Store>, catalog_name: &str, default_schema: &str) -> Self {
        Self {
            store,
            catalog_name: catalog_name.to_string(),
            default_schema: default_schema.to_string(),
        }
    }

    /// Get the schema for CommandGetCatalogs result.
    pub fn catalogs_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]))
    }

    /// Get the schema for CommandGetDbSchemas result.
    pub fn db_schemas_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]))
    }

    /// Get the schema for CommandGetTables result.
    pub fn tables_schema(include_schema: bool) -> SchemaRef {
        let mut fields = vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ];
        if include_schema {
            fields.push(Field::new("table_schema", DataType::Binary, false));
        }
        Arc::new(Schema::new(fields))
    }

    /// Get the schema for CommandGetTableTypes result.
    pub fn table_types_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]))
    }

    /// Get the schema for CommandGetPrimaryKeys result.
    pub fn primary_keys_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("key_name", DataType::Utf8, true),
            Field::new("key_sequence", DataType::Int32, false),
        ]))
    }

    /// Get the schema for CommandGetExportedKeys / CommandGetImportedKeys result.
    pub fn foreign_keys_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk_catalog_name", DataType::Utf8, true),
            Field::new("pk_db_schema_name", DataType::Utf8, true),
            Field::new("pk_table_name", DataType::Utf8, false),
            Field::new("pk_column_name", DataType::Utf8, false),
            Field::new("fk_catalog_name", DataType::Utf8, true),
            Field::new("fk_db_schema_name", DataType::Utf8, true),
            Field::new("fk_table_name", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("fk_key_name", DataType::Utf8, true),
            Field::new("pk_key_name", DataType::Utf8, true),
            Field::new("update_rule", DataType::UInt8, false),
            Field::new("delete_rule", DataType::UInt8, false),
        ]))
    }

    /// Execute CommandGetCatalogs and return the result data.
    pub fn get_catalogs(&self) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_catalogs");

        let schema = Self::catalogs_schema();

        // Return a single catalog
        let catalog_names: ArrayRef = Arc::new(StringArray::from(vec![self.catalog_name.as_str()]));

        let batch = RecordBatch::try_new(schema.clone(), vec![catalog_names])
            .map_err(|e| SqlError::Internal(format!("failed to create catalog batch: {e}")))?;

        info!(catalog_count = 1, "returning catalogs");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetDbSchemas and return the result data.
    pub async fn get_db_schemas(
        &self,
        catalog: &Option<String>,
        _schema_filter_pattern: &Option<String>,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!(catalog = ?catalog, "executing get_db_schemas");

        let schema = Self::db_schemas_schema();

        // Check catalog filter
        if let Some(cat) = catalog
            && cat != &self.catalog_name
        {
            // Return empty result for unknown catalog
            let catalog_names: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
            let schema_names: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));

            let batch = RecordBatch::try_new(schema.clone(), vec![catalog_names, schema_names])
                .map_err(|e| SqlError::Internal(format!("failed to create schemas batch: {e}")))?;

            return Ok((schema, vec![Arc::new(batch)]));
        }

        // Build schema list: always include the default schema, plus any explicitly created schemas
        // and schemas derived from table paths.
        let mut schema_set = std::collections::HashSet::new();
        schema_set.insert(self.default_schema.clone());

        // Add schemas from table paths
        if let Ok(datasets) = self.store.list(None).await {
            for dataset in &datasets {
                let (_, schema_name, _) =
                    crate::catalog::StoreCatalog::path_to_catalog_schema_table(
                        &dataset.path,
                        &self.catalog_name,
                        &self.default_schema,
                    );
                schema_set.insert(schema_name);
            }
        }

        // Add explicitly stored schemas
        if let Ok(explicit) = self.store.list_schemas().await {
            for s in explicit {
                schema_set.insert(s);
            }
        }

        let mut schema_list: Vec<String> = schema_set.into_iter().collect();
        schema_list.sort();

        let catalog_names: ArrayRef = Arc::new(StringArray::from(
            schema_list
                .iter()
                .map(|_| Some(self.catalog_name.as_str()))
                .collect::<Vec<_>>(),
        ));
        let schema_names: ArrayRef = Arc::new(StringArray::from(
            schema_list.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![catalog_names, schema_names])
            .map_err(|e| SqlError::Internal(format!("failed to create schemas batch: {e}")))?;

        info!(schema_count = schema_list.len(), "returning db schemas");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetTables and return the result data.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_tables(
        &self,
        catalog: &Option<String>,
        _db_schema_filter_pattern: &Option<String>,
        _table_name_filter_pattern: &Option<String>,
        table_types: &[String],
        include_schema: bool,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!(
            catalog = ?catalog,
            include_schema = include_schema,
            "executing get_tables"
        );

        let result_schema = Self::tables_schema(include_schema);

        // Check catalog filter
        if let Some(cat) = catalog
            && cat != &self.catalog_name
        {
            // Return empty result for unknown catalog
            return self.empty_tables_result(include_schema);
        }

        // Get all datasets from store
        let datasets = self
            .store
            .list(None)
            .await
            .map_err(|e| SqlError::Internal(format!("failed to list datasets: {e}")))?;

        // Filter by table type if specified
        let table_type = "TABLE"; // All our datasets are tables
        if !table_types.is_empty() && !table_types.iter().any(|t| t == table_type) {
            return self.empty_tables_result(include_schema);
        }

        // Build result arrays
        let mut catalog_names: Vec<Option<String>> = Vec::new();
        let mut schema_names: Vec<Option<String>> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut table_types_col: Vec<&str> = Vec::new();
        let mut table_schemas: Vec<Vec<u8>> = Vec::new();

        for dataset in &datasets {
            let (cat, schema, table) = StoreCatalog::path_to_catalog_schema_table(
                &dataset.path,
                &self.catalog_name,
                &self.default_schema,
            );
            catalog_names.push(Some(cat));
            schema_names.push(Some(schema));
            table_names.push(table);
            table_types_col.push(table_type);

            if include_schema {
                // Encode schema to IPC format
                let schema_bytes = Self::encode_schema_to_ipc(&dataset.schema)?;
                table_schemas.push(schema_bytes);
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalog_names)),
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_types_col)),
        ];

        if include_schema {
            columns.push(Arc::new(BinaryArray::from_vec(
                table_schemas.iter().map(|s| s.as_slice()).collect(),
            )));
        }

        let batch = RecordBatch::try_new(result_schema.clone(), columns)
            .map_err(|e| SqlError::Internal(format!("failed to create tables batch: {e}")))?;

        info!(table_count = datasets.len(), "returning tables");

        Ok((result_schema, vec![Arc::new(batch)]))
    }

    /// Return an empty tables result.
    fn empty_tables_result(
        &self,
        include_schema: bool,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        let schema = Self::tables_schema(include_schema);
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        if include_schema {
            columns.push(Arc::new(BinaryArray::from_vec(Vec::<&[u8]>::new())));
        }
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SqlError::Internal(format!("failed to create empty tables batch: {e}")))?;
        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Encode a schema to IPC format.
    fn encode_schema_to_ipc(schema: &SchemaRef) -> SqlResult<Vec<u8>> {
        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(schema, &options);
        let ipc_message: IpcMessage =
            schema_ipc
                .try_into()
                .map_err(|e: arrow_schema::ArrowError| {
                    SqlError::Internal(format!("failed to encode schema: {e}"))
                })?;
        Ok(ipc_message.to_vec())
    }

    /// Execute CommandGetTableTypes and return the result data.
    pub fn get_table_types(&self) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_table_types");

        let schema = Self::table_types_schema();

        // We only support TABLE type
        let table_types: ArrayRef = Arc::new(StringArray::from(vec!["TABLE"]));

        let batch = RecordBatch::try_new(schema.clone(), vec![table_types])
            .map_err(|e| SqlError::Internal(format!("failed to create table_types batch: {e}")))?;

        info!(type_count = 1, "returning table types");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetPrimaryKeys and return the result data.
    ///
    /// Returns empty results as we don't track primary keys.
    pub fn get_primary_keys(
        &self,
        _catalog: &Option<String>,
        _schema: &Option<String>,
        _table: &str,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_primary_keys");

        let schema = Self::primary_keys_schema();

        // Return empty result - we don't track primary keys
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // catalog_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // db_schema_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // table_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // column_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // key_name
            Arc::new(Int32Array::from(Vec::<i32>::new())),           // key_sequence
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SqlError::Internal(format!("failed to create primary_keys batch: {e}")))?;

        info!("returning empty primary keys");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetExportedKeys and return the result data.
    ///
    /// Returns empty results as we don't track foreign keys.
    pub fn get_exported_keys(
        &self,
        _catalog: &Option<String>,
        _schema: &Option<String>,
        _table: &str,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_exported_keys");

        let schema = Self::foreign_keys_schema();
        let batch = self.empty_foreign_keys_batch(&schema)?;

        info!("returning empty exported keys");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetImportedKeys and return the result data.
    ///
    /// Returns empty results as we don't track foreign keys.
    pub fn get_imported_keys(
        &self,
        _catalog: &Option<String>,
        _schema: &Option<String>,
        _table: &str,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_imported_keys");

        let schema = Self::foreign_keys_schema();
        let batch = self.empty_foreign_keys_batch(&schema)?;

        info!("returning empty imported keys");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetCrossReference and return the result data.
    ///
    /// Returns empty results as we don't track foreign keys.
    #[allow(clippy::too_many_arguments)]
    pub fn get_cross_reference(
        &self,
        _pk_catalog: &Option<String>,
        _pk_db_schema: &Option<String>,
        _pk_table: &str,
        _fk_catalog: &Option<String>,
        _fk_db_schema: &Option<String>,
        _fk_table: &str,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!("executing get_cross_reference");

        let schema = Self::foreign_keys_schema();
        let batch = self.empty_foreign_keys_batch(&schema)?;

        info!("returning empty cross reference");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Create an empty foreign keys batch.
    fn empty_foreign_keys_batch(&self, schema: &SchemaRef) -> SqlResult<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // pk_catalog_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // pk_db_schema_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // pk_table_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // pk_column_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // fk_catalog_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // fk_db_schema_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // fk_table_name
            Arc::new(StringArray::from(Vec::<&str>::new())),         // fk_column_name
            Arc::new(Int32Array::from(Vec::<i32>::new())),           // key_sequence
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // fk_key_name
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())), // pk_key_name
            Arc::new(UInt8Array::from(Vec::<u8>::new())),            // update_rule
            Arc::new(UInt8Array::from(Vec::<u8>::new())),            // delete_rule
        ];

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SqlError::Internal(format!("failed to create foreign_keys batch: {e}")))
    }

    /// Execute CommandGetSqlInfo and return the result data.
    pub fn get_sql_info(&self, info: &[u32]) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!(info_codes = ?info, "executing get_sql_info");

        let batch = SQL_INFO_DATA
            .record_batch(info.iter().copied())
            .map_err(|e| SqlError::Internal(format!("failed to get sql info: {e}")))?;

        let schema = batch.schema();

        info!(row_count = batch.num_rows(), "returning sql info");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Execute CommandGetXdbcTypeInfo and return the result data.
    pub fn get_xdbc_type_info(
        &self,
        data_type: Option<i32>,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        debug!(data_type = ?data_type, "executing get_xdbc_type_info");

        let batch = XDBC_TYPE_INFO_DATA
            .record_batch(data_type)
            .map_err(|e| SqlError::Internal(format!("failed to get xdbc type info: {e}")))?;

        let schema = batch.schema();

        info!(row_count = batch.num_rows(), "returning xdbc type info");

        Ok((schema, vec![Arc::new(batch)]))
    }

    /// Get schema and data for a metadata query.
    pub async fn execute_metadata_query(
        &self,
        query: &MetadataQuery,
    ) -> SqlResult<(SchemaRef, Vec<Arc<RecordBatch>>)> {
        match query {
            MetadataQuery::GetCatalogs => self.get_catalogs(),
            MetadataQuery::GetDbSchemas {
                catalog,
                schema_filter_pattern,
            } => self.get_db_schemas(catalog, schema_filter_pattern).await,
            MetadataQuery::GetTables {
                catalog,
                db_schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
                include_schema,
            } => {
                self.get_tables(
                    catalog,
                    db_schema_filter_pattern,
                    table_name_filter_pattern,
                    table_types,
                    *include_schema,
                )
                .await
            }
            MetadataQuery::GetTableTypes => self.get_table_types(),
            MetadataQuery::GetPrimaryKeys {
                catalog,
                db_schema,
                table,
            } => self.get_primary_keys(catalog, db_schema, table),
            MetadataQuery::GetExportedKeys {
                catalog,
                db_schema,
                table,
            } => self.get_exported_keys(catalog, db_schema, table),
            MetadataQuery::GetImportedKeys {
                catalog,
                db_schema,
                table,
            } => self.get_imported_keys(catalog, db_schema, table),
            MetadataQuery::GetCrossReference {
                pk_catalog,
                pk_db_schema,
                pk_table,
                fk_catalog,
                fk_db_schema,
                fk_table,
            } => self.get_cross_reference(
                pk_catalog,
                pk_db_schema,
                pk_table,
                fk_catalog,
                fk_db_schema,
                fk_table,
            ),
            MetadataQuery::GetSqlInfo { info } => self.get_sql_info(info),
            MetadataQuery::GetXdbcTypeInfo { data_type } => self.get_xdbc_type_info(*data_type),
        }
    }

    /// Check if a handle is a metadata query handle.
    pub fn is_metadata_handle(handle: &Bytes) -> bool {
        String::from_utf8(handle.to_vec())
            .map(|s| s.starts_with(METADATA_HANDLE_PREFIX))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    fn create_test_store() -> Arc<dyn Store> {
        Arc::new(MemoryStore::new())
    }

    fn create_test_engine() -> MetadataEngine {
        MetadataEngine::new(create_test_store(), DEFAULT_CATALOG, DEFAULT_SCHEMA)
    }

    #[test]
    fn test_catalogs_schema() {
        let schema = MetadataEngine::catalogs_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(!schema.field(0).is_nullable());
    }

    #[test]
    fn test_get_catalogs() {
        let engine = create_test_engine();

        let (schema, batches) = engine.get_catalogs().unwrap();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        // Check the catalog name
        let catalog_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(catalog_col.value(0), DEFAULT_CATALOG);
    }

    #[test]
    fn test_metadata_handle_roundtrip() {
        let query = MetadataQuery::GetCatalogs;
        let handle = query.to_handle();
        let parsed = MetadataQuery::from_handle(&handle).unwrap();

        assert!(matches!(parsed, MetadataQuery::GetCatalogs));
    }

    #[test]
    fn test_is_metadata_handle() {
        assert!(MetadataEngine::is_metadata_handle(&Bytes::from(
            "meta_catalogs"
        )));
        assert!(MetadataEngine::is_metadata_handle(&Bytes::from(
            "meta_schemas"
        )));
        assert!(!MetadataEngine::is_metadata_handle(&Bytes::from(
            "ps_12345"
        )));
        assert!(!MetadataEngine::is_metadata_handle(&Bytes::from(
            "some_query"
        )));
    }

    #[test]
    fn test_db_schemas_schema() {
        let schema = MetadataEngine::db_schemas_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "db_schema_name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());
    }

    #[tokio::test]
    async fn test_get_db_schemas() {
        let engine = create_test_engine();

        let (schema, batches) = engine.get_db_schemas(&None, &None).await.unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        // Check the catalog and schema names
        let catalog_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(catalog_col.value(0), DEFAULT_CATALOG);

        let schema_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(schema_col.value(0), DEFAULT_SCHEMA);
    }

    #[tokio::test]
    async fn test_get_db_schemas_with_matching_catalog() {
        let engine = create_test_engine();

        let (_, batches) = engine
            .get_db_schemas(&Some(DEFAULT_CATALOG.to_string()), &None)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_get_db_schemas_with_non_matching_catalog() {
        let engine = create_test_engine();

        let (_, batches) = engine
            .get_db_schemas(&Some("other_catalog".to_string()), &None)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_db_schemas_handle_roundtrip() {
        let query = MetadataQuery::GetDbSchemas {
            catalog: None,
            schema_filter_pattern: None,
        };
        let handle = query.to_handle();
        let parsed = MetadataQuery::from_handle(&handle).unwrap();

        assert!(matches!(parsed, MetadataQuery::GetDbSchemas { .. }));
    }

    #[test]
    fn test_tables_schema_without_schema() {
        let schema = MetadataEngine::tables_schema(false);
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(1).name(), "db_schema_name");
        assert_eq!(schema.field(2).name(), "table_name");
        assert_eq!(schema.field(3).name(), "table_type");
    }

    #[test]
    fn test_tables_schema_with_schema() {
        let schema = MetadataEngine::tables_schema(true);
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(4).name(), "table_schema");
        assert_eq!(schema.field(4).data_type(), &DataType::Binary);
    }

    async fn create_test_store_with_tables() -> Arc<dyn Store> {
        use crate::DataPath;
        use arrow_array::Int64Array;

        let store = Arc::new(MemoryStore::new());

        // Add a test table
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            table_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        store
            .put(
                DataPath::new(vec!["test".to_string(), "users".to_string()]),
                table_schema,
                vec![batch],
            )
            .await
            .unwrap();

        store
    }

    #[tokio::test]
    async fn test_get_tables_empty_store() {
        let engine = create_test_engine();

        let (schema, batches) = engine
            .get_tables(&None, &None, &None, &[], false)
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    async fn test_get_tables_with_data() {
        let store = create_test_store_with_tables().await;
        let engine = MetadataEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let (schema, batches) = engine
            .get_tables(&None, &None, &None, &[], false)
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(batches[0].num_rows(), 1);

        // DataPath ["test", "users"] → schema="test", table="users"
        let schema_name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(schema_name_col.value(0), "test");

        let table_name_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_name_col.value(0), "users");
    }

    #[tokio::test]
    async fn test_get_tables_with_schema() {
        let store = create_test_store_with_tables().await;
        let engine = MetadataEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let (schema, batches) = engine
            .get_tables(&None, &None, &None, &[], true)
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(batches[0].num_rows(), 1);

        // Verify table_schema column contains data
        let schema_col = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(!schema_col.value(0).is_empty());
    }

    #[tokio::test]
    async fn test_get_tables_wrong_catalog() {
        let store = create_test_store_with_tables().await;
        let engine = MetadataEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let (_, batches) = engine
            .get_tables(&Some("wrong_catalog".to_string()), &None, &None, &[], false)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    async fn test_get_tables_wrong_table_type() {
        let store = create_test_store_with_tables().await;
        let engine = MetadataEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let (_, batches) = engine
            .get_tables(&None, &None, &None, &["VIEW".to_string()], false)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    async fn test_get_tables_correct_table_type() {
        let store = create_test_store_with_tables().await;
        let engine = MetadataEngine::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let (_, batches) = engine
            .get_tables(&None, &None, &None, &["TABLE".to_string()], false)
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_tables_handle_roundtrip() {
        let query = MetadataQuery::GetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let handle = query.to_handle();
        let parsed = MetadataQuery::from_handle(&handle).unwrap();

        assert!(matches!(
            parsed,
            MetadataQuery::GetTables {
                include_schema: false,
                ..
            }
        ));
    }

    #[test]
    fn test_tables_with_schema_handle_roundtrip() {
        let query = MetadataQuery::GetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: true,
        };
        let handle = query.to_handle();
        let parsed = MetadataQuery::from_handle(&handle).unwrap();

        assert!(matches!(
            parsed,
            MetadataQuery::GetTables {
                include_schema: true,
                ..
            }
        ));
    }
}
