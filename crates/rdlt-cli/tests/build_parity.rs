//! Construction failures surface as typed Resolve errors naming the
//! problem: a missing path-form config file refuses at desugar —
//! before any connector binary is looked for — and must not panic or
//! misclassify.

use rdlt::pipeline_spec::{Spec, SpecError, build_pipeline};

#[tokio::test]
async fn missing_config_file_is_a_resolve_error() {
    let yaml = "pipeline: p\nsource:\n  rest: /no/such/file.yaml\n\
                destination:\n  file:\n    path: ./out\n";
    let spec: Spec = serde_yaml::from_str(yaml).expect("parses");
    match build_pipeline(&spec, std::path::Path::new("")).await {
        Err(SpecError::Resolve(message)) => {
            assert!(message.contains("/no/such/file.yaml"), "{message}");
        }
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }
}
