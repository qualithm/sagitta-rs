//! DataFusion catalog and schema providers for Store.
//!
//! Provides proper `catalog.schema.table` support for SQL queries.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use std::sync::RwLock;

use crate::DataPath;
use crate::Store;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::TableProvider;
use tracing::debug;

use crate::provider::StoreTableProvider;

/// A DataFusion CatalogProvider backed by Store.
///
/// This catalog provides access to tables stored in Store using
/// proper `catalog.schema.table` qualified names.
pub struct StoreCatalog {
    name: String,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl StoreCatalog {
    /// Create a new catalog with all tables from the store.
    ///
    /// Tables are organized by their DataPath:
    /// - `["table"]` → `{catalog}.{schema}.table`
    /// - `["schema", "table"]` → `{catalog}.schema.table`
    /// - `["catalog", "schema", "table"]` → `catalog.schema.table`
    pub async fn new(store: Arc<dyn Store>, catalog_name: &str, default_schema: &str) -> Self {
        let mut catalog_schemas: HashMap<String, HashMap<String, Vec<(String, DataPath)>>> =
            HashMap::new();

        // Group tables by catalog and schema
        if let Ok(datasets) = store.list(None).await {
            for dataset in datasets {
                let (cat, schema, table) =
                    Self::path_to_catalog_schema_table(&dataset.path, catalog_name, default_schema);

                catalog_schemas
                    .entry(cat)
                    .or_default()
                    .entry(schema)
                    .or_default()
                    .push((table, dataset.path));
            }
        }

        // Build schema providers for the default catalog
        let mut schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();

        if let Some(default_schemas) = catalog_schemas.remove(catalog_name) {
            for (schema_name, tables) in default_schemas {
                let schema_provider = StoreSchema::new(store.clone(), tables);
                schemas.insert(schema_name, Arc::new(schema_provider));
            }
        }

        // Ensure the default schema exists even if empty
        if !schemas.contains_key(default_schema) {
            schemas.insert(
                default_schema.to_string(),
                Arc::new(StoreSchema::new(store.clone(), vec![])),
            );
        }

        // Include explicitly created schemas from the store
        if let Ok(explicit_schemas) = store.list_schemas().await {
            for schema_name in explicit_schemas {
                schemas
                    .entry(schema_name)
                    .or_insert_with(|| Arc::new(StoreSchema::new(store.clone(), vec![])));
            }
        }

        Self {
            name: catalog_name.to_string(),
            schemas,
        }
    }

    /// Convert a DataPath to (catalog, schema, table) tuple.
    ///
    /// - `["table"]` → `(catalog_name, default_schema, "table")`
    /// - `["schema", "table"]` → `(catalog_name, "schema", "table")`
    /// - `["catalog", "schema", "table"]` → `("catalog", "schema", "table")`
    pub fn path_to_catalog_schema_table(
        path: &DataPath,
        catalog_name: &str,
        default_schema: &str,
    ) -> (String, String, String) {
        let segments = path.segments();
        match segments.len() {
            0 => (
                catalog_name.to_string(),
                default_schema.to_string(),
                "unknown".to_string(),
            ),
            1 => (
                catalog_name.to_string(),
                default_schema.to_string(),
                segments[0].clone(),
            ),
            2 => (
                catalog_name.to_string(),
                segments[0].clone(),
                segments[1].clone(),
            ),
            _ => (
                segments[0].clone(),
                segments[1].clone(),
                segments[2..].join("_"),
            ),
        }
    }
}

impl CatalogProvider for StoreCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

impl std::fmt::Debug for StoreCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreCatalog")
            .field("name", &self.name)
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A DataFusion SchemaProvider backed by Store.
///
/// This schema supports both Store tables and dynamically registered
/// views/tables.
pub struct StoreSchema {
    store: Arc<dyn Store>,
    /// Tables backed by Store
    tables: HashMap<String, DataPath>,
    /// Dynamically registered tables/views (not backed by Store)
    dynamic_tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl StoreSchema {
    /// Create a new schema provider with the given tables.
    pub fn new(store: Arc<dyn Store>, tables: Vec<(String, DataPath)>) -> Self {
        let tables = tables.into_iter().collect();
        Self {
            store,
            tables,
            dynamic_tables: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for StoreSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        if let Ok(dynamic) = self.dynamic_tables.read() {
            names.extend(dynamic.keys().cloned());
        }
        names
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        debug!(table = %name, "looking up table");

        // First check Store tables
        if let Some(path) = self.tables.get(name) {
            let provider = StoreTableProvider::new(self.store.clone(), path.clone()).await?;
            return Ok(Some(Arc::new(provider)));
        }

        // Then check dynamic tables (views)
        if let Ok(dynamic) = self.dynamic_tables.read()
            && let Some(provider) = dynamic.get(name)
        {
            return Ok(Some(provider.clone()));
        }

        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            return true;
        }
        if let Ok(dynamic) = self.dynamic_tables.read() {
            return dynamic.contains_key(name);
        }
        false
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        debug!(table = %name, "registering dynamic table/view");
        let mut dynamic = self
            .dynamic_tables
            .write()
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
        let old = dynamic.insert(name, table);
        Ok(old)
    }

    fn deregister_table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        debug!(table = %name, "deregistering dynamic table/view");
        let mut dynamic = self
            .dynamic_tables
            .write()
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
        Ok(dynamic.remove(name))
    }
}

impl std::fmt::Debug for StoreSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreSchema")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use arrow_array::RecordBatch;
    use arrow_array::builder::Int64Builder;
    use arrow_schema::{DataType, Field, Schema};

    use crate::metadata::{DEFAULT_CATALOG, DEFAULT_SCHEMA};

    async fn create_test_store() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut value_builder = Int64Builder::new();
        for i in 0..5 {
            id_builder.append_value(i);
            value_builder.append_value(i * 10);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .unwrap();

        // Table with 1 segment: public.simple
        store
            .put(
                DataPath::from(vec!["simple"]),
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            .unwrap();

        // Table with 2 segments: myschema.users
        store
            .put(
                DataPath::from(vec!["myschema", "users"]),
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            .unwrap();

        // Table with 3 segments: default.myschema.orders
        store
            .put(
                DataPath::from(vec!["default", "myschema", "orders"]),
                schema.clone(),
                vec![batch],
            )
            .await
            .unwrap();

        store
    }

    #[test]
    fn test_path_to_catalog_schema_table() {
        // 1 segment
        let (cat, schema, table) = StoreCatalog::path_to_catalog_schema_table(
            &DataPath::from(vec!["users"]),
            DEFAULT_CATALOG,
            DEFAULT_SCHEMA,
        );
        assert_eq!(cat, "default");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");

        // 2 segments
        let (cat, schema, table) = StoreCatalog::path_to_catalog_schema_table(
            &DataPath::from(vec!["myschema", "users"]),
            DEFAULT_CATALOG,
            DEFAULT_SCHEMA,
        );
        assert_eq!(cat, "default");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "users");

        // 3 segments
        let (cat, schema, table) = StoreCatalog::path_to_catalog_schema_table(
            &DataPath::from(vec!["mycat", "myschema", "orders"]),
            DEFAULT_CATALOG,
            DEFAULT_SCHEMA,
        );
        assert_eq!(cat, "mycat");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "orders");
    }

    #[tokio::test]
    async fn test_catalog_schema_names() {
        let store = create_test_store().await;
        let catalog = StoreCatalog::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        let schema_names = catalog.schema_names();
        assert!(schema_names.contains(&"public".to_string()));
        assert!(schema_names.contains(&"myschema".to_string()));
    }

    #[tokio::test]
    async fn test_schema_table_names() {
        let store = create_test_store().await;
        let catalog = StoreCatalog::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        // Check public schema
        let public_schema = catalog.schema("public").unwrap();
        let public_tables = public_schema.table_names();
        assert!(public_tables.contains(&"simple".to_string()));

        // Check myschema
        let my_schema = catalog.schema("myschema").unwrap();
        let my_tables = my_schema.table_names();
        assert!(my_tables.contains(&"users".to_string()));
        assert!(my_tables.contains(&"orders".to_string()));
    }

    #[tokio::test]
    async fn test_schema_table_lookup() {
        let store = create_test_store().await;
        let catalog = StoreCatalog::new(store, DEFAULT_CATALOG, DEFAULT_SCHEMA).await;

        let schema = catalog.schema("public").unwrap();
        let table = schema.table("simple").await.unwrap();
        assert!(table.is_some());

        let missing = schema.table("nonexistent").await.unwrap();
        assert!(missing.is_none());
    }
}
