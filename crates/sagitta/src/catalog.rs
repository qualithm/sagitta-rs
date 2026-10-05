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
        let schema_provider = StoreSchema::new(store.clone(), tables).with_store_lookup(
          Self::schema_prefixes(&schema_name, catalog_name, default_schema),
        );
        schemas.insert(schema_name, Arc::new(schema_provider));
      }
    }

    let empty_schema = |schema_name: &str| -> Arc<dyn SchemaProvider> {
      Arc::new(
        StoreSchema::new(store.clone(), vec![]).with_store_lookup(Self::schema_prefixes(
          schema_name,
          catalog_name,
          default_schema,
        )),
      )
    };

    // Ensure the default schema exists even if empty
    schemas
      .entry(default_schema.to_string())
      .or_insert_with(|| empty_schema(default_schema));

    // Include explicitly created schemas from the store
    if let Ok(explicit_schemas) = store.list_schemas().await {
      for schema_name in explicit_schemas {
        let provider = empty_schema(&schema_name);
        schemas.entry(schema_name).or_insert(provider);
      }
    }

    Self {
      name: catalog_name.to_string(),
      schemas,
    }
  }

  /// Every [`DataPath`] prefix that [`Self::path_to_catalog_schema_table`] maps
  /// into `schema_name` of `catalog_name`, so a table name can be resolved back
  /// to its path.
  fn schema_prefixes(
    schema_name: &str,
    catalog_name: &str,
    default_schema: &str,
  ) -> Vec<Vec<String>> {
    let mut prefixes = vec![vec![schema_name.to_string()]];
    if schema_name == default_schema {
      prefixes.push(vec![]);
    }
    prefixes.push(vec![catalog_name.to_string(), schema_name.to_string()]);
    prefixes
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
  /// Path prefixes probed in the store for a table missing from `tables`, so a
  /// table created after this schema was built still resolves
  lookup_prefixes: Vec<Vec<String>>,
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
      lookup_prefixes: Vec::new(),
      dynamic_tables: RwLock::new(HashMap::new()),
    }
  }

  /// Resolve a table missing from the initial listing by probing the store at
  /// each of `prefixes` followed by the table name, in order.
  #[must_use]
  pub fn with_store_lookup(mut self, prefixes: Vec<Vec<String>>) -> Self {
    self.lookup_prefixes = prefixes;
    self
  }

  /// The store path of a table created after this schema was built, if any.
  async fn lookup_in_store(&self, name: &str) -> DfResult<Option<DataPath>> {
    for prefix in &self.lookup_prefixes {
      let mut segments = prefix.clone();
      segments.push(name.to_string());
      let path = DataPath::from(segments);
      let exists = self
        .store
        .contains(&path)
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
      if exists {
        return Ok(Some(path));
      }
    }
    Ok(None)
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

    // Finally ask the store, which may hold a table created since this schema
    // was built, e.g. by another node sharing the same backing catalog
    if let Some(path) = self.lookup_in_store(name).await? {
      debug!(table = %name, path = %path.display(), "resolved table from store");
      let provider = StoreTableProvider::new(self.store.clone(), path).await?;
      return Ok(Some(Arc::new(provider)));
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

  #[tokio::test]
  async fn test_schema_resolves_tables_created_after_build() {
    let store = create_test_store().await;
    let catalog = StoreCatalog::new(store.clone(), DEFAULT_CATALOG, DEFAULT_SCHEMA).await;
    let late = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    for path in [
      vec!["late_default"],
      vec!["myschema", "late_two"],
      vec!["default", "myschema", "late_three"],
    ] {
      store
        .put(DataPath::from(path), late.clone(), vec![])
        .await
        .unwrap();
    }

    let public = catalog.schema("public").unwrap();
    assert!(public.table("late_default").await.unwrap().is_some());
    let my_schema = catalog.schema("myschema").unwrap();
    assert!(my_schema.table("late_two").await.unwrap().is_some());
    assert!(my_schema.table("late_three").await.unwrap().is_some());
    assert!(my_schema.table("late_default").await.unwrap().is_none());
  }

  #[tokio::test]
  async fn test_schema_without_store_lookup_ignores_late_tables() {
    let store = create_test_store().await;
    let schema = StoreSchema::new(store.clone(), vec![]);
    let late = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    store
      .put(DataPath::from(vec!["late"]), late, vec![])
      .await
      .unwrap();

    assert!(schema.table("late").await.unwrap().is_none());
  }
}
