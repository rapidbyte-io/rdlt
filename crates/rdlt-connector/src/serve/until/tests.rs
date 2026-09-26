use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::Until;

#[tokio::test]
async fn a_stream_ends_once_its_connection_is_stopping_and_not_before() {
    let stopping = CancellationToken::new();
    let (items, receiver) = tokio::sync::mpsc::channel(4);
    let mut until = Until::new(
        tokio_stream::wrappers::ReceiverStream::new(receiver),
        stopping.clone().cancelled_owned(),
    );
    items.send(1).await.expect("the stream receives");
    assert_eq!(until.next().await, Some(1));
    stopping.cancel();
    items.send(2).await.expect("the stream receives");
    assert_eq!(until.next().await, None);
}
