//! Performance benchmarks for Sagitta.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_flight::sql::{CommandStatementQuery, TicketStatementQuery};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use futures::TryStreamExt;
use sagitta::DataPath;
use sagitta::internals::SqlEngine;
use sagitta::{MemoryStore, Store};
use tokio::runtime::Runtime;

fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let mut id_builder = Int64Builder::with_capacity(rows);
    let mut name_builder = StringBuilder::with_capacity(rows, rows * 10);

    for i in 0..rows {
        id_builder.append_value(i as i64);
        name_builder.append_value(format!("row_{i}"));
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .unwrap()
}

fn bench_store_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store = MemoryStore::with_test_fixtures();
    let path = DataPath::from(vec!["test", "integers"]);

    c.bench_function("store_get", |b| {
        b.to_async(&rt).iter(|| async {
            let dataset = store.get(black_box(&path)).await.unwrap();
            black_box(dataset);
        })
    });

    c.bench_function("store_get_batches", |b| {
        b.to_async(&rt).iter(|| async {
            let batches = store.get_batches(black_box(&path)).await.unwrap();
            black_box(batches);
        })
    });
}

fn bench_store_put(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store = MemoryStore::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = create_test_batch(1000);

    c.bench_function("store_put_1000_rows", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let path =
                    DataPath::from(vec!["bench", &format!("table_{}", rand::random::<u32>())]);
                (path, schema.clone(), vec![batch.clone()])
            },
            |(path, schema, batches)| async {
                store.put(path, schema, batches).await.unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_store_list(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store = MemoryStore::with_test_fixtures();

    c.bench_function("store_list", |b| {
        b.to_async(&rt).iter(|| async {
            let datasets = store.list(None).await.unwrap();
            black_box(datasets);
        })
    });
}

fn bench_batch_creation(c: &mut Criterion) {
    c.bench_function("create_batch_100_rows", |b| {
        b.iter(|| {
            let batch = create_test_batch(black_box(100));
            black_box(batch);
        })
    });

    c.bench_function("create_batch_10000_rows", |b| {
        b.iter(|| {
            let batch = create_test_batch(black_box(10000));
            black_box(batch);
        })
    });
}

fn bench_query_execution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store: Arc<dyn Store> = Arc::new(MemoryStore::with_test_fixtures());
    let engine = rt.block_on(SqlEngine::new(store, "default", "public"));

    // Benchmark simple SELECT
    c.bench_function("query_select_all_100_rows", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT * FROM test.integers".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });

    // Benchmark SELECT with WHERE clause
    c.bench_function("query_select_with_filter", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT * FROM test.integers WHERE id > 50".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });

    // Benchmark aggregate query
    c.bench_function("query_aggregate_count", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT COUNT(*) FROM test.integers".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });

    // Benchmark aggregate with GROUP BY
    c.bench_function("query_aggregate_group_by", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT id % 10 as bucket, COUNT(*) FROM test.integers GROUP BY id % 10"
                    .to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });
}

fn bench_query_large_dataset(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store: Arc<dyn Store> = Arc::new(MemoryStore::with_test_fixtures());
    let engine = rt.block_on(SqlEngine::new(store, "default", "public"));

    // Benchmark SELECT on large dataset (10000 rows)
    c.bench_function("query_select_large_10000_rows", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT * FROM test.large".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });

    // Benchmark streaming SELECT on large dataset
    c.bench_function("query_select_large_streaming", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT * FROM test.large".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, stream) = engine
                .get_statement_query_data_stream(&ticket)
                .await
                .unwrap();
            let batches: Vec<_> = stream.try_collect().await.unwrap();
            black_box(batches);
        })
    });

    // Benchmark aggregate on large dataset
    c.bench_function("query_aggregate_large_sum", |b| {
        b.to_async(&rt).iter(|| async {
            let cmd = CommandStatementQuery {
                query: "SELECT SUM(id) FROM test.large".to_string(),
                transaction_id: None,
            };
            let result = engine.execute_statement_query(&cmd).await.unwrap();
            let ticket = TicketStatementQuery {
                statement_handle: result.handle,
            };
            let (_, batches) = engine.get_statement_query_data(&ticket).await.unwrap();
            black_box(batches);
        })
    });
}

criterion_group!(
    benches,
    bench_store_get,
    bench_store_put,
    bench_store_list,
    bench_batch_creation,
    bench_query_execution,
    bench_query_large_dataset,
);
criterion_main!(benches);
