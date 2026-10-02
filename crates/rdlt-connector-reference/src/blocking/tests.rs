use rdlt_connector::ConnectorErrorKind;

use super::blocking;

#[tokio::test]
async fn a_blocking_call_that_panics_fails_without_saying_what_the_panic_said() {
    let failed = blocking(|| -> rdlt_connector::Result<()> {
        panic!("row index out of bounds: \u{1b}[2J hunter2");
    })
    .await
    .expect_err("the call panicked");
    assert_eq!(failed.kind(), ConnectorErrorKind::Internal);
    assert!(std::error::Error::source(&failed).is_none());
    let said = failed.to_string();
    assert!(
        !said.contains("hunter2") && !said.contains("row index"),
        "{said}"
    );
}

#[tokio::test]
async fn a_blocking_call_answers_what_its_work_answered() {
    let answered = blocking(|| Ok(7)).await;
    assert_eq!(answered.expect("the work answers"), 7);
    let refused = blocking(|| Err::<(), _>(rdlt_connector::ConnectorError::data("no"))).await;
    assert_eq!(
        refused.expect_err("it fails").kind(),
        ConnectorErrorKind::Data
    );
}
