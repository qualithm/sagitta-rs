//! Storage backend trait definition.

use std::sync::Arc;

use crate::{DataPath, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

/// Metadata about a stored dataset.
#[derive(Debug, Clone)]
pub struct Dataset {
    /// The data path.
    pub path: DataPath,
    /// The Arrow schema.
    pub schema: SchemaRef,
    /// Total number of records.
    pub total_records: usize,
    /// Total size in bytes (approximate).
    pub total_bytes: usize,
}

/// Trait for storage backends.
///
/// This trait abstracts over different storage implementations,
/// allowing Sagitta to use in-memory storage, persistent storage,
/// or other backends.
#[async_trait]
pub trait Store: Send + Sync {
    /// List all available datasets.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn list(&self) -> Result<Vec<Dataset>>;

    /// Get metadata for a specific dataset.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn get(&self, path: &DataPath) -> Result<Dataset>;

    /// Get the schema for a dataset.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn get_schema(&self, path: &DataPath) -> Result<SchemaRef>;

    /// Get all record batches for a dataset.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn get_batches(&self, path: &DataPath) -> Result<Vec<Arc<RecordBatch>>>;

    /// Store a dataset with its schema and data.
    ///
    /// Overwrites any existing dataset at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    async fn put(&self, path: DataPath, schema: SchemaRef, batches: Vec<RecordBatch>)
    -> Result<()>;

    /// Append record batches to an existing dataset.
    ///
    /// The batches must have a schema compatible with the existing dataset.
    /// Returns an error if the dataset does not exist.
    async fn append_batches(&self, path: &DataPath, batches: Vec<RecordBatch>) -> Result<()>;

    /// Truncate a dataset, removing all data but keeping the schema.
    ///
    /// Returns an error if the dataset does not exist.
    async fn truncate(&self, path: &DataPath) -> Result<()>;

    /// Check if a dataset exists.
    async fn contains(&self, path: &DataPath) -> bool;

    /// Remove a dataset.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn remove(&self, path: &DataPath) -> Result<()>;

    /// Create a named schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema already exists.
    async fn create_schema(&self, name: &str) -> Result<()>;

    /// Drop a named schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn drop_schema(&self, name: &str) -> Result<bool>;

    /// List explicitly created schema names.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn list_schemas(&self) -> Result<Vec<String>>;

    /// Check if a named schema has been explicitly created.
    async fn schema_exists(&self, name: &str) -> bool;
}
