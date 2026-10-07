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
            Corpus::FlatWide,
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
    let names: Vec<&str> = Corpus::SHREDDED.into_iter().map(Corpus::name).collect();
    assert_eq!(
        names,
        [
            "nested",
            "sparse",
            "flat_narrow",
            "flat_wide",
            "string_heavy"
        ]
    );
    assert_eq!(Corpus::WithArrays.name(), "with_arrays");
}
