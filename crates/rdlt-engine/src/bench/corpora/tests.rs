use std::num::NonZeroU64;

use super::{Corpus, corpus};

#[test]
fn each_corpus_is_the_rows_its_seed_draws() {
    for (corpus, digest) in [
        (
            Corpus::Nested,
            "1ae2fbeb9aefb2ffbb5532e67f09adb6ac617bf275f8fef636fc5668568c225e",
        ),
        (
            Corpus::Sparse,
            "22a8050e3c812561d4df5425dbb52c19bcfab5ba8b1b6a618439e9b465501f8f",
        ),
        (
            Corpus::FlatNarrow,
            "7a43f6f71714132dbc881509e2aaa252153e049450f3c6728000fe5f6222ed15",
        ),
        (
            Corpus::Wide(200),
            "5ff96274cc6746321b28112eaa0230a9b5324de02033266724adcfcd1e84d8e4",
        ),
        (
            Corpus::StringHeavy,
            "bbd413d399caa86ccfb76e4be2733150f82e918d77d091e5c2693b2d6f33f32b",
        ),
        (
            Corpus::WithArrays,
            "eed42de58b12801c2a4728190e6cf45ae53b0fd69fd4c5da026b01550f026370",
        ),
        (
            Corpus::Orders,
            "5b2f71fd1b69c55e9a9dd72a06c7a0746620b2a78020768b3ae2e91d7b0389ab",
        ),
    ] {
        let mut hasher = blake3::Hasher::new();
        for push in corpus.pushes(1 << 18) {
            hasher.update(&push);
        }
        assert_eq!(hasher.finalize().to_hex().as_str(), digest, "{corpus:?}");
    }
}

#[test]
fn a_corpus_is_cut_into_pushes_of_whole_lines_once_it_holds_its_bytes() {
    let row = |index: u64| format!(r#"{{"id":{index}}}"#);
    let pushes = corpus(10_000, 1_000, |index, _| row(index));
    let (last, full) = pushes.split_last().unwrap();
    assert!(full.iter().all(|push| (1_000..1_016).contains(&push.len())));
    assert!(!last.is_empty());
    assert!(pushes.iter().all(|push| push.ends_with(b"\n")));
    let text = String::from_utf8(pushes.concat()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines
            .iter()
            .zip(0..)
            .all(|(line, index)| *line == row(index))
    );
    let before_last = text.len() - lines.last().unwrap().len() - 1;
    assert!(
        before_last < 10_000 && text.len() >= 10_000,
        "{}",
        text.len()
    );
}

#[test]
fn the_corpora_shredded_on_one_core_are_named_as_their_benchmarks() {
    let names: Vec<String> = Corpus::SHREDDED.into_iter().map(Corpus::name).collect();
    assert_eq!(
        names,
        [
            "nested",
            "sparse",
            "flat_narrow",
            "wide_200",
            "string_heavy"
        ]
    );
    assert_eq!(Corpus::WithArrays.name(), "with_arrays");
}

#[test]
fn a_corpus_s_first_rows_are_cut_into_pushes_of_a_count_of_lines() {
    let pushes = Corpus::Nested.rows(10, NonZeroU64::new(4).unwrap());
    let lines = |push: &bytes::Bytes| String::from_utf8(push.to_vec()).unwrap().lines().count();
    assert_eq!(pushes.iter().map(lines).collect::<Vec<_>>(), [4, 4, 2]);
    assert!(pushes.iter().all(|push| push.ends_with(b"\n")));
    let cut = pushes.concat();
    let whole = Corpus::Nested.pushes(1 << 12).concat();
    assert!(whole.starts_with(&cut));
    assert!(Corpus::Nested.rows(0, NonZeroU64::MIN).is_empty());
}

#[test]
fn an_order_holds_as_many_items_as_its_id_modulo_four_each_of_two_tags() {
    let pushes = Corpus::Orders.rows(8, NonZeroU64::new(8).unwrap());
    let text = String::from_utf8(pushes.concat()).unwrap();
    for (id, line) in text.lines().enumerate() {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["id"], id);
        let items = row["items"].as_array().unwrap();
        assert_eq!(items.len(), id % 4);
        assert!(
            items
                .iter()
                .all(|item| item["tags"].as_array().unwrap().len() == 2)
        );
    }
    assert_eq!(Corpus::Orders.name(), "orders");
}

#[test]
fn a_wide_row_holds_its_columns_integers_and_strings_by_turns() {
    let pushes = Corpus::Wide(5_000).rows(2, NonZeroU64::MIN);
    let row: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&pushes[0]).unwrap();
    assert_eq!(row.len(), 5_000);
    assert!(row["c4998"].is_u64() && row["c4999"].is_string());
    assert_eq!(Corpus::Wide(5_000).name(), "wide_5000");
}
