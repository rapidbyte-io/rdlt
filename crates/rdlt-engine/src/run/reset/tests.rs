use rdlt_connector::{Epoch, PipelineState, StreamName, StreamState};

use super::recorded;

fn namespaced() -> StreamName {
    StreamName::with_namespace("public", "orders").expect("a valid stream")
}

fn displayed_alike() -> StreamName {
    StreamName::new("public.orders").expect("a valid stream")
}

#[test]
fn a_stream_is_reset_by_its_own_name_never_by_one_displayed_as_it_is() {
    let mut state = PipelineState::default();
    state.streams.insert(namespaced(), StreamState::default());
    recorded(&state, &namespaced()).expect("recorded under its own name");
    // Displayed alike, recorded nowhere: its tables are another's.
    let refused = recorded(&state, &displayed_alike()).expect_err("not recorded");
    assert_eq!(refused.code(), Some("stream_not_found"));
    // A reset marker records a stream too.
    let mut reset = PipelineState::default();
    reset.resets.insert(namespaced(), Epoch(3));
    recorded(&reset, &namespaced()).expect("recorded by its reset");
}

#[test]
fn a_stream_another_recorded_stream_is_displayed_as_is_refused_as_ambiguous() {
    let mut state = PipelineState::default();
    state.streams.insert(namespaced(), StreamState::default());
    state.resets.insert(displayed_alike(), Epoch(2));
    for stream in [namespaced(), displayed_alike()] {
        let refused = recorded(&state, &stream).expect_err("their tables are one");
        assert_eq!(refused.code(), Some("stream_ambiguous"), "{stream}");
    }
}
