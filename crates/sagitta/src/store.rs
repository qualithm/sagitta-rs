//! Storage backend trait definition.

use std::sync::Arc;

use crate::{DataPath, Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;

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
    /// Unix timestamp (seconds) when the dataset was created, if known.
    pub created_at: Option<i64>,
    /// Unix timestamp (seconds) when the dataset was last modified, if known.
    pub updated_at: Option<i64>,
    /// Arbitrary key-value metadata tags.
    pub tags: std::collections::HashMap<String, String>,
}

/// Trait for storage backends.
///
/// This trait abstracts over different storage implementations,
/// allowing Sagitta to use in-memory storage, persistent storage,
/// or other backends.
#[async_trait]
pub trait Store: Send + Sync {
    /// List all available datasets, optionally filtered by schema name.
    ///
    /// When `schema_filter` is `Some`, only datasets whose path starts with
    /// that schema segment are returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn list(&self, schema_filter: Option<&str>) -> Result<Vec<Dataset>>;

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

    /// Get all record batches for a dataset as a streaming source.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn get_batches(&self, path: &DataPath) -> Result<SendableRecordBatchStream>;

    /// Scan a dataset with optional column projection.
    ///
    /// When `projection` is `Some`, the store returns only the listed column
    /// indices, which allows backends to avoid loading unreferenced columns.
    /// Callers must not reference columns outside the original schema.
    ///
    /// Returns a default implementation that calls [`Self::get_batches`] and
    /// applies the projection in memory.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn scan(
        &self,
        path: &DataPath,
        projection: Option<&[usize]>,
    ) -> Result<Vec<Arc<RecordBatch>>> {
        let schema = self.get_schema(path).await?;
        let stream = self.get_batches(path).await?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| Error::Arrow(Box::new(e.into())))?;

        match projection {
            None => Ok(batches.into_iter().map(Arc::new).collect()),
            Some(proj) => {
                let projected_schema = Arc::new(
                    schema
                        .project(proj)
                        .map_err(|e| Error::Arrow(Box::new(e)))?,
                );
                batches
                    .into_iter()
                    .map(|batch| {
                        let columns: Vec<_> =
                            proj.iter().map(|&i| batch.column(i).clone()).collect();
                        RecordBatch::try_new(projected_schema.clone(), columns)
                            .map(Arc::new)
                            .map_err(|e| Error::Arrow(Box::new(e)))
                    })
                    .collect()
            }
        }
    }

    /// Scan a dataset with optional projection and a row limit.
    ///
    /// Backends that support predicate pushdown or native limit pruning (e.g.
    /// Lance, Parquet with row-group skipping) **should override this method**
    /// to push `limit` down into the storage layer and avoid reading
    /// unnecessary data.
    ///
    /// The default implementation delegates to [`Self::scan`] and truncates
    /// the result to `limit` rows in memory.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn scan_with_limit(
        &self,
        path: &DataPath,
        projection: Option<&[usize]>,
        limit: Option<usize>,
    ) -> Result<Vec<Arc<RecordBatch>>> {
        let batches = self.scan(path, projection).await?;

        let limit = match limit {
            None => return Ok(batches),
            Some(0) => return Ok(vec![]),
            Some(n) => n,
        };

        let mut result = Vec::new();
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                result.push(batch);
            } else {
                result.push(Arc::new(batch.slice(0, remaining)));
                remaining = 0;
            }
        }
        Ok(result)
    }

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

    /// Update the schema for an existing dataset.
    ///
    /// The new schema must be compatible with the existing data, or the
    /// implementation may error. Callers are responsible for migrating data
    /// when making incompatible changes.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn update_schema(&self, path: &DataPath, schema: SchemaRef) -> Result<()>;

    /// Check whether a dataset exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn contains(&self, path: &DataPath) -> Result<bool>;

    /// Remove a dataset.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the dataset does not exist.
    async fn remove(&self, path: &DataPath) -> Result<()>;

    /// Copy a dataset to a new path.
    ///
    /// If a dataset already exists at `dst`, it is overwritten.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the source dataset does not exist.
    async fn copy(&self, src: &DataPath, dst: DataPath) -> Result<()>;

    /// Rename (move) a dataset to a new path.
    ///
    /// If a dataset already exists at `dst`, it is overwritten.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the source dataset does not exist.
    async fn rename(&self, src: &DataPath, dst: DataPath) -> Result<()>;

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

    /// Check whether a named schema has been explicitly created.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage is inaccessible.
    async fn schema_exists(&self, name: &str) -> Result<bool>;

    // -------------------------------------------------------------------------
    // Transaction hooks
    // -------------------------------------------------------------------------
    // These are optional hooks for storage backends that support native
    // transactions. The default implementations are no-ops that return a
    // dummy transaction ID; backends that have real transactional semantics
    // should override them.

    /// Begin a storage-level transaction.
    ///
    /// Returns an opaque transaction ID that must be passed to
    /// [`commit_transaction`](Self::commit_transaction) or
    /// [`rollback_transaction`](Self::rollback_transaction).
    ///
    /// The default implementation is a no-op and returns an empty ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot start a transaction.
    async fn begin_transaction(&self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    /// Commit a storage-level transaction.
    ///
    /// The default implementation is a no-op.
    ///
    /// # Errors
    ///
    /// Returns `Error::Aborted` if the transaction has already been ended or
    /// if a conflict was detected.
    async fn commit_transaction(&self, _id: &Bytes) -> Result<()> {
        Ok(())
    }

    /// Roll back a storage-level transaction, discarding all changes.
    ///
    /// The default implementation is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot roll back the transaction.
    async fn rollback_transaction(&self, _id: &Bytes) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn test_path() -> DataPath {
        DataPath::from(vec!["test", "tbl"])
    }

    async fn make_store_with_data() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        store.put(test_path(), schema, vec![batch]).await.unwrap();
        store
    }

    #[tokio::test]
    async fn test_default_scan_no_projection() {
        let store = make_store_with_data().await;
        let batches = store.scan(&test_path(), None).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn test_default_scan_with_projection() {
        let store = make_store_with_data().await;
        let batches = store.scan(&test_path(), Some(&[0])).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_default_scan_with_limit_none() {
        let store = make_store_with_data().await;
        let batches = store
            .scan_with_limit(&test_path(), None, None)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn test_default_scan_with_limit_zero() {
        let store = make_store_with_data().await;
        let batches = store
            .scan_with_limit(&test_path(), None, Some(0))
            .await
            .unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_default_scan_with_limit_partial() {
        let store = make_store_with_data().await;
        let batches = store
            .scan_with_limit(&test_path(), None, Some(3))
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn test_default_scan_with_limit_exceeds_rows() {
        let store = make_store_with_data().await;
        let batches = store
            .scan_with_limit(&test_path(), None, Some(100))
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn test_default_begin_transaction_returns_empty() {
        let store = MemoryStore::new();
        let txn_id = store.begin_transaction().await.unwrap();
        assert!(txn_id.is_empty());
    }

    #[tokio::test]
    async fn test_default_commit_transaction_succeeds() {
        let store = MemoryStore::new();
        let txn_id = store.begin_transaction().await.unwrap();
        assert!(store.commit_transaction(&txn_id).await.is_ok());
    }

    #[tokio::test]
    async fn test_default_rollback_transaction_succeeds() {
        let store = MemoryStore::new();
        let txn_id = store.begin_transaction().await.unwrap();
        assert!(store.rollback_transaction(&txn_id).await.is_ok());
    }
}
