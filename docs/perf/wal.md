# Write-ahead log

What the write-ahead log's own bookkeeping costs beside the frames it writes.

## The room a log keeps for ending its chunks

After every publish and receipt the writer notes the room it keeps for its own frames: what
ending the chunk staged writes, and what staging and ending one more after it writes (a preamble
and header, a closing frame, and an end frame naming every chunk and commit the log then holds).
Those lengths are counted, not encoded: an end frame's from the digits of its numbers, a header's
by writing its JSON to a counter that keeps only how many bytes it was given.

- **Signal.** The heap's peak while `Log::ending()` measures a fresh log, as `peak_alloc` counts
  it in the test's own process: exact, whatever the machine or its load.
- **Test.** `measuring_the_room_a_log_keeps_allocates_nothing`
  (`crates/rdlt-engine/src/wal/writer/tests/sized.rs`), which asserts the figure.
- **Lengths.** `a_header_frame_s_length_is_what_it_encodes_to` and the property test
  `an_end_frame_s_length_is_what_it_encodes_to` (`crates/rdlt-engine/src/wal/frame/tests.rs`)
  hold each counted length to the frame's encoding, at every digit boundary of its numbers.
- **Build.** `cargo nextest` of rdlt-engine's library tests, dev profile, Rust 1.98.1, at commit
  `f18394480449`.

| Measurement | Heap peak |
|---|---|
| `Log::ending()` on a fresh log | 0 bytes |
