#![forbid(unsafe_code)]
//! Orders reshaped before they land in SQLite: a column renamed, quantities cast, totals computed,
//! test orders filtered out, emails masked and a column added; then a load whose cast refuses an
//! order.

use rdlt::{
    Connector, Host, Literal, MaskStep, MaskWith, Outcome, Pipeline, Side, Stream, TransformStep,
    Value, WriteMode, json,
};
use rdlt_connector_reference::{MemorySource, SqliteDestination};

/// The SQL step: casts each quantity, prices each order, and keeps those that are not tests.
const PRICED: &str = "SELECT id, email, CAST(quantity AS BIGINT) AS quantity, price_cents, \
     CAST(quantity AS BIGINT) * price_cents AS total_cents, status \
     FROM input WHERE status <> 'test'";

/// The orders a shop sends, each quantity as text, a test order among them.
const ORDERS: &str = r#"[
    { "id": 1, "email": "buyer1@example.com", "qty": "2", "price_cents": 1250, "status": "paid" },
    { "id": 2, "email": "buyer2@example.com", "qty": "1", "price_cents": 725, "status": "test" },
    { "id": 3, "email": "buyer3@example.com", "qty": "4", "price_cents": 1000, "status": "paid" },
    { "id": 4, "email": "buyer4@example.com", "qty": "1", "price_cents": 310, "status": "refunded" }
]"#;

/// A later order, whose quantity is no number.
const LATE: &str = r#"
    { "id": 5, "email": "buyer5@example.com", "qty": "two", "price_cents": 450, "status": "paid" }
"#;

/// The `orders` stream, each step reshaping what the step before it leaves, its table replaced by
/// each load.
fn reshaped() -> Stream {
    // The key is a secret reference the host resolves, never the key itself.
    let masked = MaskStep::new(["email"], MaskWith::HmacSha256).key("${env:ORDERS_KEY}");
    Stream::new("orders")
        .write(WriteMode::Replace)
        .transform(TransformStep::rename([("qty", "quantity")]))
        .transform(TransformStep::Sql(PRICED.to_owned()))
        .transform(TransformStep::Mask(masked))
        .transform(TransformStep::constant([(
            "currency",
            Literal::Text("EUR".to_owned()),
        )]))
}

/// Loads `orders` into the SQLite database at `database`, reshaped on their way.
async fn load(host: &Host, database: &str, orders: &[Value]) -> Result<Outcome, rdlt::Error> {
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(json!({ "streams": { "orders": orders } }));
    let destination =
        Side::new(Connector::new("io.rapidbyte.sqlite")).config(json!({ "path": database }));
    let pipeline = Pipeline::builder("orders", source, destination)
        .stream(reshaped())
        .build(host)
        .await?;
    Ok(pipeline.start().await)
}

/// Says how a load ended: what it committed and what its filter dropped, or the error that ended
/// it.
fn tell(outcome: &Outcome) {
    let report = &outcome.report;
    match &outcome.error {
        None => {
            let filtered = report
                .streams
                .get("orders")
                .map_or(0, |stream| stream.filtered.dropped);
            println!(
                "{:?}: committed {} rows, {filtered} filtered out",
                report.status, report.rows
            );
        }
        // The error as data: its code, a line naming its subject and what it came from, each
        // shown and bounded.
        Some(error) => {
            let told = error.report();
            let code = told.code.map_or("", |code| code.as_str());
            let why = told.causes.first().map_or("", String::as_str);
            println!("{:?}: {code}, {}: {why}", report.status, told.message);
        }
    }
}

/// Prints each order the database at `database` holds.
fn print_orders(database: &str) -> rusqlite::Result<()> {
    let database = rusqlite::Connection::open(database)?;
    let mut orders = database.prepare(
        "SELECT id, quantity, total_cents, status, currency, email FROM orders ORDER BY id",
    )?;
    println!("id  quantity  total_cents  status    currency  email");
    let mut rows = orders.query([])?;
    while let Some(row) = rows.next()? {
        let (id, quantity, total): (i64, i64, i64) = (row.get(0)?, row.get(1)?, row.get(2)?);
        let (status, currency, email): (String, String, String) =
            (row.get(3)?, row.get(4)?, row.get(5)?);
        println!("{id:<3} {quantity:<9} {total:<12} {status:<9} {currency:<9} {email}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "orders.db".to_owned());
    // Compiled in and trusted, both connectors run in this process; the mask's key comes from
    // the environment variable the host allows by name.
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<SqliteDestination>()
        .allow_env(["ORDERS_KEY"]);
    let mut orders: Vec<Value> = serde_json::from_str(ORDERS)?;
    let outcome = load(&host, &database, &orders).await?;
    tell(&outcome);
    if let Some(error) = outcome.error {
        return Err(error.into());
    }
    print_orders(&database)?;
    // A later load, with an order whose quantity is no number: its cast refuses it, the load
    // fails, and the table keeps what the first load replaced it with.
    orders.push(serde_json::from_str(LATE)?);
    tell(&load(&host, &database, &orders).await?);
    print_orders(&database)?;
    Ok(())
}
