//! A document's masks reach the values they name, from its text through the facade into the
//! destination: a column named plainly, one whose name holds a dot, and a field within a struct
//! by its path.

use rdlt::Pipeline;
use rdlt::arrow::array::{Array, AsArray as _};
use rdlt::arrow::datatypes::DataType;

use crate::support::{memory_host, within};

/// The text of every cell `array` holds, a struct's fields among them.
fn texts(array: &dyn Array, into: &mut Vec<String>) {
    match array.data_type() {
        DataType::Utf8 => {
            let strings = array.as_string::<i32>();
            into.extend(strings.iter().flatten().map(str::to_owned));
        }
        DataType::Struct(_) => {
            for field in array.as_struct().columns() {
                texts(field.as_ref(), into);
            }
        }
        _ => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mask_reaches_a_column_by_its_name_as_written_and_a_field_by_its_path() {
    let rows = serde_json::json!([
        {
            "id": 1,
            "email": "a@example.com",
            "user.email": "b@example.com",
            "user": { "email": "c@example.com", "name": "c" },
        },
        {
            "id": 2,
            "email": "d@example.com",
            "user.email": "e@example.com",
            "user": { "email": "f@example.com", "name": "f" },
        },
    ]);
    let text = format!(
        "version: 1\n\
         pipeline: masks\n\
         source:\n  connector: io.rapidbyte.memory\n  config: {{ streams: {{ users: {rows} }} }}\n\
         destination:\n  connector: io.rapidbyte.memory\n  config: {{ store: masks-named }}\n\
         streams:\n\
         \x20 - name: users\n\
         \x20   transform:\n\
         \x20     - mask: {{ columns: [email, user.email, [user, email]], with: redact }}\n"
    );
    let pipeline = Pipeline::from_text(&text, "masks.yaml").expect("the document reads");
    let pipeline = within(pipeline.build(&memory_host())).await.expect("built");
    let outcome = within(pipeline.start()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut cells = Vec::new();
    for batch in rdlt_connector_reference::published("masks-named", "users") {
        for column in batch.columns() {
            texts(column.as_ref(), &mut cells);
        }
    }
    assert!(
        cells.iter().all(|cell| !cell.contains('@')),
        "an address is in the clear: {cells:?}"
    );
    // Each row's three addresses, wherever the destination keeps the struct's fields.
    let masked = cells.iter().filter(|cell| cell.contains("***")).count();
    assert_eq!(masked, 6, "{cells:?}");
}
