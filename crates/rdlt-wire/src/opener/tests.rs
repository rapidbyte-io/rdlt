use std::io::{Read as _, Write as _};
use std::os::fd::{AsFd as _, OwnedFd};
use std::os::unix::net::{UnixDatagram, UnixStream};

use super::{Accepting, OPEN_MAX, Opening, Passing, raise_open_files};

/// Runs `work` on a runtime of its own that drives sockets.
fn running<F: Future>(work: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .expect("a runtime builds")
        .block_on(work)
}

/// An opener's two ends.
fn opener() -> (Passing, Accepting) {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    let passing = Passing::new(OwnedFd::from(host)).expect("the host's end registers");
    let accepting =
        Accepting::new(OwnedFd::from(connector)).expect("the connector's end registers");
    (passing, accepting)
}

#[test]
fn streams_passed_arrive_in_order_close_on_exec_and_carry_their_bytes() {
    running(async {
        let (passing, accepting) = opener();
        let mut kept = Vec::new();
        for opening in [Opening::Control, Opening::Data, Opening::Data] {
            let (host, connector) = UnixStream::pair().expect("a socket pair");
            passing
                .pass(opening, connector.as_fd())
                .await
                .expect("passed");
            drop(connector);
            kept.push((opening, host));
        }
        for (index, (opening, mut host)) in kept.into_iter().enumerate() {
            let (received, stream) = accepting.next().await.expect("received").expect("passed");
            assert_eq!(received, opening, "stream {index}");
            let flags = rustix::io::fcntl_getfd(&stream).expect("its flags read");
            assert!(
                flags.contains(rustix::io::FdFlags::CLOEXEC),
                "stream {index}"
            );
            let mut stream = UnixStream::from(stream);
            host.write_all(&[u8::try_from(index).unwrap()])
                .expect("written");
            let mut byte = [0];
            stream.read_exact(&mut byte).expect("read");
            assert_eq!(usize::from(byte[0]), index);
        }
        drop(passing);
        assert!(
            accepting.next().await.expect("the end").is_none(),
            "a closed opener ends"
        );
    });
}

#[test]
fn a_message_without_a_stream_or_with_another_kind_of_descriptor_is_refused() {
    running(async {
        let (mut host, connector) = UnixStream::pair().expect("a socket pair");
        let accepting = Accepting::new(OwnedFd::from(connector)).expect("registers");
        host.write_all(b"d").expect("written");
        let refused = accepting.next().await.expect_err("a byte alone is refused");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
        let passing = Passing::new(OwnedFd::from(host)).expect("registers");
        let (datagram, _other) = UnixDatagram::pair().expect("a datagram pair");
        passing
            .pass(Opening::Data, datagram.as_fd())
            .await
            .expect("passed");
        let refused = accepting
            .next()
            .await
            .expect_err("a datagram socket is refused");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
    });
}

#[test]
fn a_byte_naming_no_stream_is_refused() {
    running(async {
        let (host, connector) = UnixStream::pair().expect("a socket pair");
        let accepting = Accepting::new(OwnedFd::from(connector)).expect("registers");
        let socket = OwnedFd::from(host);
        let (_kept, passed) = UnixStream::pair().expect("a socket pair");
        let mut space = [std::mem::MaybeUninit::uninit(); super::SPACE];
        let mut control = rustix::net::SendAncillaryBuffer::new(&mut space);
        let descriptors = [passed.as_fd()];
        assert!(control.push(rustix::net::SendAncillaryMessage::ScmRights(&descriptors)));
        let slices = [std::io::IoSlice::new(b"x")];
        rustix::net::sendmsg(
            &socket,
            &slices,
            &mut control,
            rustix::net::SendFlags::empty(),
        )
        .expect("sent");
        let refused = accepting
            .next()
            .await
            .expect_err("an unknown byte is refused");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
    });
}

#[test]
fn the_open_files_a_process_may_hold_are_raised_to_its_hard_limit() {
    use rustix::process::{Resource, getrlimit};
    raise_open_files();
    let limit = getrlimit(Resource::Nofile);
    let raised = limit.maximum.unwrap_or(OPEN_MAX);
    assert!(
        limit.current.is_none_or(|current| current >= raised),
        "{limit:?}"
    );
}
