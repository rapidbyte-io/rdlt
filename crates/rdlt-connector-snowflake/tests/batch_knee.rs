//! Measuring the rows-per-statement constant, rather than guessing it.
//!
//! Larger statements amortise the round trip; the service also caps statement
//! text, and parse cost grows with it. Where those meet is a property of the
//! data's width and of the network between here and the account, so it is
//! measured on the real thing.
//!
//! What this times is EXACTLY what the constant controls: the same rows, split
//! into statements different ways, executed against a real table. Deliberately
//! not through the engine — an engine-level sweep varies how often a load
//! COMMITS, which on a SaaS destination dominates everything else and answers
//! a different question. (It was run that way first, and did: throughput rose
//! monotonically from 25 to 628 rows/s across the batch sizes, measuring
//! commit frequency and locating no knee at all.)
//!
//! `#[ignore]` by default: this is an INSTRUMENT, not a gate. It costs
//! warehouse time, its numbers move with the network, and a measurement that
//! fails a build teaches people to distrust the build.
//!
//! ```text
//! cargo nextest run -p rdlt-connector-snowflake --test batch_knee \
//!   --run-ignored all --no-capture
//! ```

use std::sync::Arc;

use rdlt_connector::core::{
    ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema,
};
use rdlt_connector_snowflake::dest::SnowflakeConfig;
use rdlt_connector_snowflake::dest::testhook::{
    apply, connect_and_run, insert_statements_chunked,
};
use rdlt_testkit::snowflake::{credentials, scratch_schema};
use serde_json::json;

/// Rows per measured run — enough that per-run fixed cost does not dominate.
const ROWS: usize = 20_000;

/// The chunk sizes swept.
const CHUNKS: &[usize] = &[100, 250, 500, 1_000, 2_000, 5_000, 10_000, 20_000];

fn config_in(schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    Some(
        SnowflakeConfig::from_value(json!({
            "account": creds.account,
            "user": creds.user,
            "database": creds.database,
            "schema": schema,
            "warehouse": creds.warehouse,
            "role": creds.role,
            "auth": {"key_pair": {
                "private_key": creds.private_key_path,
                "passphrase": creds.passphrase,
            }},
        }))
        .expect("valid config"),
    )
}

/// A dozen columns of mixed width, so statement text grows the way real data
/// makes it grow rather than the way one integer column does.
fn schema() -> TableSchema {
    let column = |name: &str, ty: LogicalType| ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::scalar(ty),
        nullable: true,
        provenance: Provenance::Inferred,
    };
    TableSchema {
        table: TableName::from("knee"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64),
            column("name", LogicalType::Utf8),
            column("email", LogicalType::Utf8),
            column("region", LogicalType::Utf8),
            column("amount", LogicalType::Float64),
            column("quantity", LogicalType::Int64),
            column("active", LogicalType::Bool),
            column("note", LogicalType::Utf8),
            column("sku", LogicalType::Utf8),
            column("channel", LogicalType::Utf8),
            column("source_system", LogicalType::Utf8),
            column("batch_ref", LogicalType::Utf8),
        ],
    }
}

fn batch() -> arrow_array::RecordBatch {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let ids: Vec<i64> = (0..ROWS as i64).collect();
    let text = |f: &dyn Fn(i64) -> String| {
        Arc::new(StringArray::from(
            ids.iter().map(|id| f(*id)).collect::<Vec<_>>(),
        )) as arrow_array::ArrayRef
    };

    let arrow = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("quantity", DataType::Int64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("note", DataType::Utf8, true),
        Field::new("sku", DataType::Utf8, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("source_system", DataType::Utf8, true),
        Field::new("batch_ref", DataType::Utf8, true),
    ]));
    arrow_array::RecordBatch::try_new(
        arrow,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            text(&|id| format!("customer-{id}")),
            text(&|id| format!("user{id}@example.invalid")),
            text(&|id| if id % 3 == 0 { "emea".into() } else { "amer".into() }),
            Arc::new(Float64Array::from(
                ids.iter().map(|id| (id % 997) as f64 / 7.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 13).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                ids.iter().map(|id| id % 2 == 0).collect::<Vec<_>>(),
            )),
            text(&|_| "a description of moderate length, as free text tends to be".into()),
            text(&|id| format!("SKU-{:08}", id % 50_000)),
            text(&|_| "web".into()),
            text(&|_| "oracle-erp".into()),
            text(&|id| format!("batch-{}", id / 1000)),
        ],
    )
    .expect("batch")
}

#[tokio::test]
#[ignore = "measurement instrument: costs warehouse time, gates nothing"]
async fn sweep_the_rows_per_statement_knee() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema_name = scratch_schema("knee");
    let database = admin.database.to_uppercase();
    apply(
        &admin,
        &format!("CREATE SCHEMA \"{database}\".\"{schema_name}\""),
    )
    .await
    .expect("scratch schema");

    let config = config_in(&schema_name).expect("credentials");
    let table_schema = schema();
    let batch = batch();
    let target = format!("\"{database}\".\"{schema_name}\".\"KNEE\"");

    println!("\n=== rows-per-statement sweep ({ROWS} rows × 12 columns) ===");
    let mut results: Vec<(usize, f64, usize, usize)> = Vec::new();

    for &chunk in CHUNKS {
        // A fresh table per chunk size, so no run pays for another's rows.
        apply(
            &config,
            &format!(
                "CREATE OR REPLACE TABLE {target} (\"ID\" NUMBER, \"NAME\" VARCHAR, \
                 \"EMAIL\" VARCHAR, \"REGION\" VARCHAR, \"AMOUNT\" FLOAT, \
                 \"QUANTITY\" NUMBER, \"ACTIVE\" BOOLEAN, \"NOTE\" VARCHAR, \
                 \"SKU\" VARCHAR, \"CHANNEL\" VARCHAR, \"SOURCE_SYSTEM\" VARCHAR, \
                 \"BATCH_REF\" VARCHAR)"
            ),
        )
        .await
        .expect("target table");

        let statements = insert_statements_chunked(&target, &table_schema, &batch, chunk)
            .expect("statements render");
        let widest = statements.iter().map(String::len).max().unwrap_or(0);

        let started = std::time::Instant::now();
        for sql in &statements {
            apply(&config, sql).await.unwrap_or_else(|e| {
                panic!("chunk {chunk} ({} statements, widest {widest}B): {e:?}", statements.len())
            });
        }
        let elapsed = started.elapsed().as_secs_f64();

        let landed = connect_and_run(&config, &format!("SELECT count(*) FROM {target}"))
            .await
            .expect("count");
        assert_eq!(landed, ROWS.to_string(), "every row must land at chunk {chunk}");

        let rate = ROWS as f64 / elapsed;
        results.push((chunk, rate, statements.len(), widest));
        println!(
            "  {chunk:>6} rows/stmt: {:>3} statements, widest {:>8}B, {elapsed:>6.2}s, {rate:>8.0} rows/s",
            statements.len(),
            widest
        );
    }

    let _ = apply(
        &admin,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema_name}\""),
    )
    .await;

    let best = results
        .iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .expect("a sweep produced results");
    println!(
        "  → fastest: {} rows/statement at {:.0} rows/s ({} statements, widest {}B)\n",
        best.0, best.1, best.2, best.3
    );
}
