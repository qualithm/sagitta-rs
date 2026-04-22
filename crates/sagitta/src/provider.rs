//! DataFusion TableProvider implementation for Store.

use std::any::Any;
use std::sync::Arc;

use crate::DataPath;
use crate::Store;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DfResult;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

/// A DataFusion `TableProvider` backed by a [`Store`] entry.
pub struct StoreTableProvider {
    store: Arc<dyn Store>,
    path: DataPath,
    schema: SchemaRef,
}

impl StoreTableProvider {
    /// Create a new table provider for the given data path.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset does not exist or its schema cannot be read.
    pub async fn new(store: Arc<dyn Store>, path: DataPath) -> DfResult<Self> {
        let schema = store
            .get_schema(&path)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        Ok(Self {
            store,
            path,
            schema,
        })
    }

    /// Get the data path for this table.
    pub fn path(&self) -> &DataPath {
        &self.path
    }
}

impl std::fmt::Debug for StoreTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreTableProvider")
            .field("path", &self.path)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for StoreTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let batches = self
            .store
            .get_batches(&self.path)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Convert Arc<RecordBatch> to owned RecordBatch
        let batches: Vec<RecordBatch> = batches.iter().map(|b| b.as_ref().clone()).collect();

        // Use MemTable to handle the execution plan creation
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;

        mem_table.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use arrow_array::builder::Int64Builder;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    async fn create_test_store() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
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

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .unwrap();

        store
            .put(DataPath::from(vec!["test", "data"]), schema, vec![batch])
            .await
            .unwrap();

        store
    }

    #[tokio::test]
    async fn test_provider_schema() {
        let store = create_test_store().await;
        let provider = StoreTableProvider::new(store, DataPath::from(vec!["test", "data"]))
            .await
            .unwrap();

        assert_eq!(provider.schema().fields().len(), 2);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_provider_scan() {
        let store = create_test_store().await;
        let provider = StoreTableProvider::new(store, DataPath::from(vec!["test", "data"]))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_data", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test_data").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);
    }

    #[tokio::test]
    async fn test_provider_projection() {
        let store = create_test_store().await;
        let provider = StoreTableProvider::new(store, DataPath::from(vec!["test", "data"]))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_data", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT id FROM test_data").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn test_provider_filter() {
        let store = create_test_store().await;
        let provider = StoreTableProvider::new(store, DataPath::from(vec!["test", "data"]))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_data", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("SELECT * FROM test_data WHERE id > 5")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // id 6, 7, 8, 9
    }

    #[tokio::test]
    async fn test_provider_aggregate() {
        let store = create_test_store().await;
        let provider = StoreTableProvider::new(store, DataPath::from(vec!["test", "data"]))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_data", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("SELECT COUNT(*), SUM(value) FROM test_data")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_provider_not_found() {
        let store = Arc::new(MemoryStore::new());
        let result = StoreTableProvider::new(store, DataPath::from(vec!["nonexistent"])).await;
        assert!(result.is_err());
    }
}
