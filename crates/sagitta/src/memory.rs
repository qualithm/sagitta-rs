//! In-memory storage backend.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::{DataPath, Error, Result};
use arrow_array::RecordBatch;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;

use crate::store::{Dataset, Store};

/// Entry stored in memory for each dataset.
#[derive(Debug)]
struct MemoryEntry {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
}

impl MemoryEntry {
    fn total_records(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    fn total_bytes(&self) -> usize {
        self.batches.iter().map(|b| b.get_array_memory_size()).sum()
    }
}

/// In-memory storage backend for Sagitta.
///
/// This is the default storage backend, suitable for testing and
/// development. All data is stored in memory and lost on restart.
#[derive(Debug)]
pub struct MemoryStore {
    datasets: RwLock<HashMap<DataPath, MemoryEntry>>,
    schemas: RwLock<HashSet<String>>,
}

impl MemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            datasets: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::new()),
        }
    }

    /// Create a memory store pre-populated with test fixtures.
    pub fn with_test_fixtures() -> Self {
        let store = Self::new();
        store.populate_test_fixtures();
        store
    }

    fn populate_test_fixtures(&self) {
        self.create_integers_fixture();
        self.create_strings_fixture();
        self.create_empty_fixture();
        self.create_all_types_fixture();
        self.create_large_fixture();
        self.create_nested_fixture();
        self.create_join_test_fixtures();
    }

    fn put_sync(&self, path: DataPath, schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        datasets.insert(
            path,
            MemoryEntry {
                schema,
                batches: batches.into_iter().map(Arc::new).collect(),
            },
        );

        Ok(())
    }

    fn create_integers_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut value_builder = Int64Builder::new();

        for i in 0..100 {
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
        .expect("failed to create integers fixture");

        self.put_sync(
            DataPath::from(vec!["test", "integers"]),
            schema,
            vec![batch],
        )
        .expect("failed to store integers fixture");
    }

    fn create_strings_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut name_builder = StringBuilder::new();

        for i in 0..100 {
            id_builder.append_value(i);
            name_builder.append_value(format!("item_{i}"));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
            ],
        )
        .expect("failed to create strings fixture");

        self.put_sync(DataPath::from(vec!["test", "strings"]), schema, vec![batch])
            .expect("failed to store strings fixture");
    }

    fn create_empty_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Empty RecordBatch with the schema, but no data
        let batch = RecordBatch::new_empty(schema.clone());

        self.put_sync(DataPath::from(vec!["test", "empty"]), schema, vec![batch])
            .expect("failed to store empty fixture");
    }

    fn create_all_types_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_int8", DataType::Int8, false),
            Field::new("col_int16", DataType::Int16, false),
            Field::new("col_int32", DataType::Int32, false),
            Field::new("col_int64", DataType::Int64, false),
            Field::new("col_uint8", DataType::UInt8, false),
            Field::new("col_uint16", DataType::UInt16, false),
            Field::new("col_uint32", DataType::UInt32, false),
            Field::new("col_uint64", DataType::UInt64, false),
            Field::new("col_float32", DataType::Float32, false),
            Field::new("col_float64", DataType::Float64, false),
            Field::new("col_boolean", DataType::Boolean, false),
            Field::new("col_utf8", DataType::Utf8, false),
            Field::new("col_binary", DataType::Binary, false),
            Field::new("col_date32", DataType::Date32, false),
            Field::new(
                "col_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        let mut int8_builder = Int8Builder::new();
        let mut int16_builder = Int16Builder::new();
        let mut int32_builder = Int32Builder::new();
        let mut int64_builder = Int64Builder::new();
        let mut uint8_builder = UInt8Builder::new();
        let mut uint16_builder = UInt16Builder::new();
        let mut uint32_builder = UInt32Builder::new();
        let mut uint64_builder = UInt64Builder::new();
        let mut float32_builder = Float32Builder::new();
        let mut float64_builder = Float64Builder::new();
        let mut boolean_builder = BooleanBuilder::new();
        let mut utf8_builder = StringBuilder::new();
        let mut binary_builder = BinaryBuilder::new();
        let mut date32_builder = Date32Builder::new();
        let mut timestamp_builder = TimestampMicrosecondBuilder::new();

        for i in 0..10 {
            int8_builder.append_value(i as i8);
            int16_builder.append_value(i as i16);
            int32_builder.append_value(i);
            int64_builder.append_value(i as i64);
            uint8_builder.append_value(i as u8);
            uint16_builder.append_value(i as u16);
            uint32_builder.append_value(i as u32);
            uint64_builder.append_value(i as u64);
            float32_builder.append_value(i as f32 * 1.5);
            float64_builder.append_value(i as f64 * 1.5);
            boolean_builder.append_value(i % 2 == 0);
            utf8_builder.append_value(format!("row_{i}"));
            binary_builder.append_value(vec![i as u8; 4]);
            date32_builder.append_value(19000 + i); // days since epoch
            timestamp_builder.append_value((i as i64) * 1_000_000); // microseconds
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(int8_builder.finish()),
                Arc::new(int16_builder.finish()),
                Arc::new(int32_builder.finish()),
                Arc::new(int64_builder.finish()),
                Arc::new(uint8_builder.finish()),
                Arc::new(uint16_builder.finish()),
                Arc::new(uint32_builder.finish()),
                Arc::new(uint64_builder.finish()),
                Arc::new(float32_builder.finish()),
                Arc::new(float64_builder.finish()),
                Arc::new(boolean_builder.finish()),
                Arc::new(utf8_builder.finish()),
                Arc::new(binary_builder.finish()),
                Arc::new(date32_builder.finish()),
                Arc::new(timestamp_builder.finish()),
            ],
        )
        .expect("failed to create all_types fixture");

        self.put_sync(
            DataPath::from(vec!["test", "all_types"]),
            schema,
            vec![batch],
        )
        .expect("failed to store all_types fixture");
    }

    fn create_large_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Binary, false),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut data_builder = BinaryBuilder::new();

        for i in 0..10_000 {
            id_builder.append_value(i);
            // 64 bytes of data per row
            data_builder.append_value(vec![(i % 256) as u8; 64]);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(data_builder.finish()),
            ],
        )
        .expect("failed to create large fixture");

        self.put_sync(DataPath::from(vec!["test", "large"]), schema, vec![batch])
            .expect("failed to store large fixture");
    }

    fn create_nested_fixture(&self) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                false,
            ),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut list_builder = ListBuilder::new(Int64Builder::new());

        for i in 0..50 {
            id_builder.append_value(i);
            // Each row has i+1 items in the list
            for j in 0..=i {
                list_builder.values().append_value(j * 10);
            }
            list_builder.append(true);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(list_builder.finish()),
            ],
        )
        .expect("failed to create nested fixture");

        self.put_sync(DataPath::from(vec!["test", "nested"]), schema, vec![batch])
            .expect("failed to store nested fixture");
    }

    /// Create test fixtures for JOIN operations testing.
    ///
    /// Creates three related tables:
    /// - test/customers: customer_id, name
    /// - test/orders: order_id, customer_id, amount
    /// - test/products: product_id, order_id, product_name, price
    fn create_join_test_fixtures(&self) {
        // Customers table
        let customers_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let mut customer_id_builder = Int64Builder::new();
        let mut name_builder = StringBuilder::new();

        // Create 10 customers
        for i in 1..=10 {
            customer_id_builder.append_value(i);
            name_builder.append_value(format!("Customer {i}"));
        }

        let customers_batch = RecordBatch::try_new(
            customers_schema.clone(),
            vec![
                Arc::new(customer_id_builder.finish()),
                Arc::new(name_builder.finish()),
            ],
        )
        .expect("failed to create customers fixture");

        self.put_sync(
            DataPath::from(vec!["test", "customers"]),
            customers_schema,
            vec![customers_batch],
        )
        .expect("failed to store customers fixture");

        // Orders table
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        let mut order_id_builder = Int64Builder::new();
        let mut order_customer_id_builder = Int64Builder::new();
        let mut amount_builder = Float64Builder::new();

        // Create 25 orders (some customers have multiple orders)
        let order_data = [
            (1, 1, 100.0),
            (2, 1, 250.0),
            (3, 2, 75.0),
            (4, 2, 150.0),
            (5, 3, 300.0),
            (6, 4, 50.0),
            (7, 4, 125.0),
            (8, 4, 200.0),
            (9, 5, 500.0),
            (10, 6, 80.0),
            (11, 7, 175.0),
            (12, 7, 225.0),
            (13, 8, 350.0),
            (14, 9, 90.0),
            (15, 9, 110.0),
            (16, 10, 425.0),
            (17, 1, 60.0),
            (18, 3, 180.0),
            (19, 5, 275.0),
            (20, 6, 95.0),
            (21, 8, 320.0),
            (22, 10, 150.0),
            (23, 2, 85.0),
            (24, 4, 115.0),
            (25, 7, 190.0),
        ];

        for (order_id, customer_id, amount) in order_data {
            order_id_builder.append_value(order_id);
            order_customer_id_builder.append_value(customer_id);
            amount_builder.append_value(amount);
        }

        let orders_batch = RecordBatch::try_new(
            orders_schema.clone(),
            vec![
                Arc::new(order_id_builder.finish()),
                Arc::new(order_customer_id_builder.finish()),
                Arc::new(amount_builder.finish()),
            ],
        )
        .expect("failed to create orders fixture");

        self.put_sync(
            DataPath::from(vec!["test", "orders"]),
            orders_schema,
            vec![orders_batch],
        )
        .expect("failed to store orders fixture");

        // Products table (order line items)
        let products_schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("product_name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));

        let mut product_id_builder = Int64Builder::new();
        let mut product_order_id_builder = Int64Builder::new();
        let mut product_name_builder = StringBuilder::new();
        let mut price_builder = Float64Builder::new();

        // Create products linked to orders
        let product_data = [
            (1, 1, "Widget A", 50.0),
            (2, 1, "Widget B", 50.0),
            (3, 2, "Gadget X", 125.0),
            (4, 2, "Gadget Y", 125.0),
            (5, 3, "Widget A", 75.0),
            (6, 4, "Gadget X", 75.0),
            (7, 4, "Gadget Y", 75.0),
            (8, 5, "Premium Kit", 300.0),
            (9, 6, "Widget A", 50.0),
            (10, 7, "Widget B", 62.5),
            (11, 7, "Gadget X", 62.5),
            (12, 8, "Premium Kit", 200.0),
            (13, 9, "Ultra Package", 500.0),
            (14, 10, "Widget A", 40.0),
            (15, 10, "Widget B", 40.0),
        ];

        for (product_id, order_id, product_name, price) in product_data {
            product_id_builder.append_value(product_id);
            product_order_id_builder.append_value(order_id);
            product_name_builder.append_value(product_name);
            price_builder.append_value(price);
        }

        let products_batch = RecordBatch::try_new(
            products_schema.clone(),
            vec![
                Arc::new(product_id_builder.finish()),
                Arc::new(product_order_id_builder.finish()),
                Arc::new(product_name_builder.finish()),
                Arc::new(price_builder.finish()),
            ],
        )
        .expect("failed to create products fixture");

        self.put_sync(
            DataPath::from(vec!["test", "products"]),
            products_schema,
            vec![products_batch],
        )
        .expect("failed to store products fixture");
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn list(&self, schema_filter: Option<&str>) -> Result<Vec<Dataset>> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        Ok(datasets
            .iter()
            .filter(|(path, _)| match schema_filter {
                None => true,
                Some(filter) => path
                    .segments()
                    .first()
                    .map(|s| s == filter)
                    .unwrap_or(false),
            })
            .map(|(path, entry)| Dataset {
                path: path.clone(),
                schema: entry.schema.clone(),
                total_records: entry.total_records(),
                total_bytes: entry.total_bytes(),
                created_at: None,
                updated_at: None,
                tags: Default::default(),
            })
            .collect())
    }

    async fn get(&self, path: &DataPath) -> Result<Dataset> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        let entry = datasets
            .get(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        Ok(Dataset {
            path: path.clone(),
            schema: entry.schema.clone(),
            total_records: entry.total_records(),
            total_bytes: entry.total_bytes(),
            created_at: None,
            updated_at: None,
            tags: Default::default(),
        })
    }

    async fn get_schema(&self, path: &DataPath) -> Result<SchemaRef> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        let entry = datasets
            .get(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        Ok(entry.schema.clone())
    }

    async fn get_batches(&self, path: &DataPath) -> Result<SendableRecordBatchStream> {
        let (schema, arc_batches) = {
            let datasets = self
                .datasets
                .read()
                .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

            let entry = datasets
                .get(path)
                .ok_or_else(|| Error::NotFound(path.display()))?;

            (entry.schema.clone(), entry.batches.clone())
        };

        let owned: Vec<RecordBatch> = arc_batches.iter().map(|b| b.as_ref().clone()).collect();
        let batch_stream = stream::iter(owned.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }

    async fn put(
        &self,
        path: DataPath,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.put_sync(path, schema, batches)
    }

    async fn append_batches(&self, path: &DataPath, batches: Vec<RecordBatch>) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        let entry = datasets
            .get_mut(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        for batch in batches {
            entry.batches.push(Arc::new(batch));
        }

        Ok(())
    }

    async fn truncate(&self, path: &DataPath) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        let entry = datasets
            .get_mut(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        entry.batches.clear();

        Ok(())
    }

    async fn update_schema(&self, path: &DataPath, schema: SchemaRef) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        let entry = datasets
            .get_mut(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        entry.schema = schema;

        Ok(())
    }

    async fn contains(&self, path: &DataPath) -> Result<bool> {
        let datasets = self
            .datasets
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        Ok(datasets.contains_key(path))
    }

    async fn remove(&self, path: &DataPath) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        datasets
            .remove(path)
            .ok_or_else(|| Error::NotFound(path.display()))?;

        Ok(())
    }

    async fn copy(&self, src: &DataPath, dst: DataPath) -> Result<()> {
        let (schema, batches) = {
            let datasets = self
                .datasets
                .read()
                .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

            let entry = datasets
                .get(src)
                .ok_or_else(|| Error::NotFound(src.display()))?;

            (entry.schema.clone(), entry.batches.clone())
        };

        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        datasets.insert(dst, MemoryEntry { schema, batches });

        Ok(())
    }

    async fn rename(&self, src: &DataPath, dst: DataPath) -> Result<()> {
        let mut datasets = self
            .datasets
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        let entry = datasets
            .remove(src)
            .ok_or_else(|| Error::NotFound(src.display()))?;

        datasets.insert(dst, entry);

        Ok(())
    }

    async fn create_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self
            .schemas
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        if !schemas.insert(name.to_string()) {
            return Err(Error::InvalidRequest(format!(
                "schema already exists: {name}"
            )));
        }

        Ok(())
    }

    async fn drop_schema(&self, name: &str) -> Result<bool> {
        let mut schemas = self
            .schemas
            .write()
            .map_err(|_| Error::Aborted("write conflict: store lock poisoned".to_string()))?;

        Ok(schemas.remove(name))
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        Ok(schemas.iter().cloned().collect())
    }

    async fn schema_exists(&self, name: &str) -> Result<bool> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| Error::Internal(format!("failed to acquire read lock: {}", e)))?;

        Ok(schemas.contains(name))
    }

    async fn begin_transaction(&self) -> Result<bytes::Bytes> {
        Ok(bytes::Bytes::new())
    }

    async fn commit_transaction(&self, _id: &bytes::Bytes) -> Result<()> {
        Ok(())
    }

    async fn rollback_transaction(&self, _id: &bytes::Bytes) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn create_test_batch(schema: SchemaRef, num_rows: i64) -> RecordBatch {
        use arrow_array::builder::Int64Builder;

        let mut id_builder = Int64Builder::new();
        for i in 0..num_rows {
            id_builder.append_value(i);
        }

        RecordBatch::try_new(schema, vec![Arc::new(id_builder.finish())])
            .expect("failed to create test batch")
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_path() -> DataPath {
        DataPath::from(vec!["test", "table"])
    }

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = MemoryStore::new();
        let datasets = store.list(None).await.unwrap();
        assert!(datasets.is_empty());
    }

    #[tokio::test]
    async fn test_put_and_get_info() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();
        let batch = create_test_batch(schema.clone(), 10);

        store
            .put(path.clone(), schema.clone(), vec![batch])
            .await
            .unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.path, path);
        assert_eq!(info.total_records, 10);
        assert_eq!(info.schema, schema);
    }

    #[tokio::test]
    async fn test_put_and_get_schema() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();
        let batch = create_test_batch(schema.clone(), 5);

        store
            .put(path.clone(), schema.clone(), vec![batch])
            .await
            .unwrap();

        let retrieved = store.get_schema(&path).await.unwrap();
        assert_eq!(retrieved, schema);
    }

    #[tokio::test]
    async fn test_put_and_get_batches() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();
        let batch = create_test_batch(schema.clone(), 25);

        store.put(path.clone(), schema, vec![batch]).await.unwrap();

        let stream = store.get_batches(&path).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 25);
    }

    #[tokio::test]
    async fn test_put_multiple_batches() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();
        let batch1 = create_test_batch(schema.clone(), 10);
        let batch2 = create_test_batch(schema.clone(), 15);

        store
            .put(path.clone(), schema, vec![batch1, batch2])
            .await
            .unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 25);

        let stream = store.get_batches(&path).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn test_list_with_data() {
        let store = MemoryStore::new();
        let schema = test_schema();

        store
            .put(
                DataPath::from(vec!["a"]),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 5)],
            )
            .await
            .unwrap();
        store
            .put(
                DataPath::from(vec!["b"]),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 10)],
            )
            .await
            .unwrap();

        let datasets = store.list(None).await.unwrap();
        assert_eq!(datasets.len(), 2);
    }

    #[tokio::test]
    async fn test_list_with_schema_filter() {
        let store = MemoryStore::new();
        let schema = test_schema();

        store
            .put(
                DataPath::from(vec!["schema_a", "t1"]),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 5)],
            )
            .await
            .unwrap();
        store
            .put(
                DataPath::from(vec!["schema_b", "t2"]),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 5)],
            )
            .await
            .unwrap();

        let datasets = store.list(Some("schema_a")).await.unwrap();
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].path, DataPath::from(vec!["schema_a", "t1"]));
    }

    #[tokio::test]
    async fn test_contains_existing() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();

        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 1)],
            )
            .await
            .unwrap();

        assert!(store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_contains_non_existing() {
        let store = MemoryStore::new();
        let path = test_path();

        assert!(!store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_remove_existing() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();

        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 1)],
            )
            .await
            .unwrap();

        assert!(store.contains(&path).await.unwrap());
        store.remove(&path).await.unwrap();
        assert!(!store.contains(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_remove_non_existing_returns_error() {
        let store = MemoryStore::new();
        let path = test_path();

        let result = store.remove(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let store = MemoryStore::new();
        let path = test_path();

        let result = store.get(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let store = MemoryStore::new();
        let path = test_path();

        let result = store.get_schema(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_batches_not_found() {
        let store = MemoryStore::new();
        let path = test_path();

        let result = store.get_batches(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_overwrites_existing() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();

        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 10)],
            )
            .await
            .unwrap();
        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 50)],
            )
            .await
            .unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 50);
    }

    #[tokio::test]
    async fn test_empty_batch_has_zero_records() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();
        let batch = RecordBatch::new_empty(schema.clone());

        store.put(path.clone(), schema, vec![batch]).await.unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 0);
    }

    #[tokio::test]
    async fn test_copy() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let src = test_path();
        let dst = DataPath::from(vec!["test", "copy"]);

        store
            .put(
                src.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 10)],
            )
            .await
            .unwrap();

        store.copy(&src, dst.clone()).await.unwrap();

        assert!(store.contains(&src).await.unwrap());
        assert!(store.contains(&dst).await.unwrap());
        assert_eq!(store.get(&dst).await.unwrap().total_records, 10);
    }

    #[tokio::test]
    async fn test_rename() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let src = test_path();
        let dst = DataPath::from(vec!["test", "renamed"]);

        store
            .put(
                src.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 10)],
            )
            .await
            .unwrap();

        store.rename(&src, dst.clone()).await.unwrap();

        assert!(!store.contains(&src).await.unwrap());
        assert!(store.contains(&dst).await.unwrap());
        assert_eq!(store.get(&dst).await.unwrap().total_records, 10);
    }

    #[tokio::test]
    async fn test_update_schema() {
        let store = MemoryStore::new();
        let path = test_path();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, true),
        ]));

        store.put(path.clone(), schema, vec![]).await.unwrap();

        store
            .update_schema(&path, new_schema.clone())
            .await
            .unwrap();
        assert_eq!(store.get_schema(&path).await.unwrap(), new_schema);
    }

    #[tokio::test]
    async fn test_with_test_fixtures_populates_data() {
        let store = MemoryStore::with_test_fixtures();
        let datasets = store.list(None).await.unwrap();

        assert!(datasets.len() >= 6);
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "integers"]))
                .await
                .unwrap()
        );
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "strings"]))
                .await
                .unwrap()
        );
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "empty"]))
                .await
                .unwrap()
        );
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "all_types"]))
                .await
                .unwrap()
        );
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "large"]))
                .await
                .unwrap()
        );
        assert!(
            store
                .contains(&DataPath::from(vec!["test", "nested"]))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_integers_fixture_has_correct_records() {
        let store = MemoryStore::with_test_fixtures();
        let path = DataPath::from(vec!["test", "integers"]);

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 100);

        let schema = store.get_schema(&path).await.unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_large_fixture_has_correct_records() {
        let store = MemoryStore::with_test_fixtures();
        let path = DataPath::from(vec!["test", "large"]);

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 10_000);
    }

    #[test]
    fn test_default_creates_empty_store() {
        let store = MemoryStore::default();
        assert!(std::format!("{:?}", store).contains("MemoryStore"));
    }

    #[tokio::test]
    async fn test_append_batches_success() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();

        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema.clone(), 5)],
            )
            .await
            .unwrap();

        store
            .append_batches(&path, vec![create_test_batch(schema, 3)])
            .await
            .unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 8);
    }

    #[tokio::test]
    async fn test_append_batches_not_found() {
        let store = MemoryStore::new();
        let path = test_path();
        let schema = test_schema();

        let result = store
            .append_batches(&path, vec![create_test_batch(schema, 3)])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_success() {
        let store = MemoryStore::new();
        let schema = test_schema();
        let path = test_path();

        store
            .put(
                path.clone(),
                schema.clone(),
                vec![create_test_batch(schema, 10)],
            )
            .await
            .unwrap();

        store.truncate(&path).await.unwrap();

        let info = store.get(&path).await.unwrap();
        assert_eq!(info.total_records, 0);
        // Schema should still be present
        assert!(store.get_schema(&path).await.is_ok());
    }

    #[tokio::test]
    async fn test_truncate_not_found() {
        let store = MemoryStore::new();
        let result = store.truncate(&test_path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_schema_success() {
        let store = MemoryStore::new();
        store.create_schema("myschema").await.unwrap();
        assert!(store.schema_exists("myschema").await.unwrap());
    }

    #[tokio::test]
    async fn test_create_schema_duplicate_error() {
        let store = MemoryStore::new();
        store.create_schema("duplicate").await.unwrap();
        let result = store.create_schema("duplicate").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_schema_existing() {
        let store = MemoryStore::new();
        store.create_schema("todrop").await.unwrap();
        let dropped = store.drop_schema("todrop").await.unwrap();
        assert!(dropped);
        assert!(!store.schema_exists("todrop").await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_schema_non_existing() {
        let store = MemoryStore::new();
        let dropped = store.drop_schema("nothere").await.unwrap();
        assert!(!dropped);
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let store = MemoryStore::new();
        store.create_schema("alpha").await.unwrap();
        store.create_schema("beta").await.unwrap();

        let mut schemas = store.list_schemas().await.unwrap();
        schemas.sort();
        assert!(schemas.contains(&"alpha".to_string()));
        assert!(schemas.contains(&"beta".to_string()));
    }

    #[tokio::test]
    async fn test_schema_exists_false() {
        let store = MemoryStore::new();
        assert!(!store.schema_exists("absent").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_begin_transaction() {
        let store = MemoryStore::new();
        let id = store.begin_transaction().await.unwrap();
        assert!(id.is_empty());
    }

    #[tokio::test]
    async fn test_memory_store_commit_transaction() {
        let store = MemoryStore::new();
        let id = store.begin_transaction().await.unwrap();
        store.commit_transaction(&id).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_store_rollback_transaction() {
        let store = MemoryStore::new();
        let id = store.begin_transaction().await.unwrap();
        store.rollback_transaction(&id).await.unwrap();
    }

    #[tokio::test]
    async fn test_copy_not_found() {
        let store = MemoryStore::new();
        let src = DataPath::from(vec!["no", "src"]);
        let dst = DataPath::from(vec!["no", "dst"]);
        let result = store.copy(&src, dst).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rename_not_found() {
        let store = MemoryStore::new();
        let src = DataPath::from(vec!["no", "src"]);
        let dst = DataPath::from(vec!["no", "dst"]);
        let result = store.rename(&src, dst).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_schema_not_found() {
        let store = MemoryStore::new();
        let result = store.update_schema(&test_path(), test_schema()).await;
        assert!(result.is_err());
    }
}
