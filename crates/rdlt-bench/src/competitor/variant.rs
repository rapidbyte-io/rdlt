//! The variant registry: every `benches/competitors/*/variants.toml`
//! discovered into ONE flat namespace of ids, each carrying the pin every
//! artifact fingerprint records and how its arms execute.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result, load_toml};

/// How a variant's arms execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    /// A pinned container image whose entrypoint self-times and prints the
    /// summary line (the dlt shape). Spelled `self_timed_container` in the
    /// registry.
    #[default]
    #[serde(rename = "self_timed_container")]
    Container,
    /// A host-side `driver.py` in the variant's module directory: it drives an
    /// external system (e.g. an Airbyte cluster), times the work itself, and
    /// prints the SAME summary line — zero artifact divergence.
    Driver,
}

/// A resolved competitor variant. `pin`/`image` may come from the file's
/// `[defaults]` table (all dlt variants share one pinned image), a
/// per-variant value overriding it.
#[derive(Debug, Clone)]
pub(crate) struct Variant {
    pub(crate) id: String,
    /// e.g. "dlt 1.29.0" — recorded in every artifact fingerprint.
    pub(crate) pin: String,
    pub(crate) kind: Kind,
    /// Container image (container kind only).
    pub(crate) image: Option<String>,
    /// Driver script path, resolved relative to the module directory
    /// (driver kind only).
    pub(crate) driver: Option<PathBuf>,
    /// Machine-prerequisite probe, run with `sh -c` in the module directory
    /// before any driver run; non-zero exit ⇒ the arm records
    /// `Missing{reason}` (loud skip, never an error).
    pub(crate) prerequisite_sh: Option<String>,
    /// Per-variant run-count override (a cell's competitor entry still wins).
    pub(crate) runs: Option<u32>,
    /// The `benches/competitors/<module>/` directory the variant came from —
    /// drivers execute with this as their working directory.
    pub(crate) module_dir: PathBuf,
}

/// Shared defaults every variant inherits unless it states its own.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    pin: Option<String>,
    image: Option<String>,
}

/// One `[[variant]]` as written: `pin`/`image` are optional here and fall back
/// to `[defaults]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    id: String,
    pin: Option<String>,
    image: Option<String>,
    #[serde(default)]
    kind: Kind,
    driver: Option<String>,
    prerequisite_sh: Option<String>,
    runs: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    defaults: Defaults,
    #[serde(default, rename = "variant")]
    variants: Vec<Raw>,
}

fn load(path: &Path) -> Result<Vec<Variant>> {
    fn resolve(
        value: Option<String>,
        default: &Option<String>,
        id: &str,
        name: &str,
    ) -> Result<String> {
        value.or_else(|| default.clone()).ok_or_else(|| {
            Error(format!(
                "variant `{id}`: no `{name}` and none in [defaults]"
            ))
        })
    }
    let module_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| ".".into());
    let file: File = load_toml(path)?;
    file.variants
        .into_iter()
        .map(|raw| {
            let pin = resolve(raw.pin, &file.defaults.pin, &raw.id, "pin")?;
            let (image, driver) = match raw.kind {
                Kind::Container => (
                    Some(resolve(raw.image, &file.defaults.image, &raw.id, "image")?),
                    None,
                ),
                Kind::Driver => {
                    let driver = raw.driver.ok_or_else(|| {
                        Error(format!("variant `{}`: kind=driver needs `driver`", raw.id))
                    })?;
                    (None, Some(module_dir.join(driver)))
                }
            };
            Ok(Variant {
                pin,
                kind: raw.kind,
                image,
                driver,
                prerequisite_sh: raw.prerequisite_sh,
                runs: raw.runs,
                module_dir: module_dir.clone(),
                id: raw.id,
            })
        })
        .collect()
}

/// Discover every module's variants into ONE flat namespace:
/// `benches/competitors/*/variants.toml`, deterministic (sorted) module
/// order, duplicate variant id = load-time error naming both files.
pub(crate) fn discover(competitors_dir: &Path) -> Result<Vec<Variant>> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(competitors_dir) {
        Ok(entries) => entries
            .filter_map(|e| Some(e.ok()?.path().join("variants.toml")))
            .filter(|p| p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    let mut all: Vec<Variant> = Vec::new();
    let mut sources: BTreeMap<String, PathBuf> = BTreeMap::new();
    for file in files {
        for variant in load(&file)? {
            if let Some(first) = sources.get(&variant.id) {
                return Err(Error(format!(
                    "duplicate variant id `{}`: declared in both {} and {} (variant ids are one flat namespace)",
                    variant.id,
                    first.display(),
                    file.display(),
                )));
            }
            sources.insert(variant.id.clone(), file.clone());
            all.push(variant);
        }
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_toml_parses_and_rejects_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[[variant]]\nid='dlt-pyarrow'\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n",
        )
        .unwrap();
        let variants = load(&p).unwrap();
        assert_eq!(variants[0].pin, "dlt 1.29.0");
        assert_eq!(variants[0].image.as_deref(), Some("rdlt-baseline"));
        assert_eq!(variants[0].kind, Kind::Container);

        std::fs::write(&p, "[[variant]]\nid='x'\npin='p'\nimage='i'\nnope=1\n").unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
    }

    /// The registry's kind spelling stays `self_timed_container`.
    #[test]
    fn the_container_kind_keeps_its_registry_spelling() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[[variant]]\nid='dlt'\npin='p'\nimage='i'\nkind='self_timed_container'\n",
        )
        .unwrap();
        assert_eq!(load(&p).unwrap()[0].kind, Kind::Container);
        std::fs::write(
            &p,
            "[[variant]]\nid='dlt'\npin='p'\nimage='i'\nkind='container'\n",
        )
        .unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("container"), "{err}");
    }

    #[test]
    fn variant_defaults_fill_pin_and_image() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[defaults]\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n\n[[variant]]\nid='dlt'\n\n[[variant]]\nid='other'\nimage='custom'\n",
        )
        .unwrap();
        let variants = load(&p).unwrap();
        assert_eq!(variants[0].pin, "dlt 1.29.0");
        assert_eq!(variants[0].image.as_deref(), Some("rdlt-baseline"));
        // per-variant override wins over the default
        assert_eq!(variants[1].image.as_deref(), Some("custom"));

        // no value and no default → a loud error naming the variant + field
        std::fs::write(&p, "[[variant]]\nid='bare'\n").unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("bare") && err.contains("pin"), "{err}");
    }

    #[test]
    fn discovery_is_flat_and_duplicate_ids_name_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("alpha");
        let b = dir.path().join("beta");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(
            a.join("variants.toml"),
            "[[variant]]\nid='dlt'\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n",
        )
        .unwrap();
        std::fs::write(
            b.join("variants.toml"),
            "[[variant]]\nid='airbyte'\npin='airbyte 2.1.1'\nkind='driver'\ndriver='driver.py'\n",
        )
        .unwrap();
        let variants = discover(dir.path()).unwrap();
        let ids: Vec<&str> = variants.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, ["dlt", "airbyte"]); // sorted module order (alpha, beta)

        std::fs::write(
            b.join("variants.toml"),
            "[[variant]]\nid='dlt'\npin='p'\nimage='i'\n",
        )
        .unwrap();
        let err = discover(dir.path()).unwrap_err().to_string();
        assert!(
            err.contains("duplicate variant id `dlt`")
                && err.contains("alpha")
                && err.contains("beta"),
            "{err}"
        );
    }

    #[test]
    fn driver_variant_parses_and_requires_driver() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[[variant]]\nid='airbyte'\npin='airbyte 2.1.1'\nkind='driver'\ndriver='driver.py'\nprerequisite_sh='true'\nruns=3\n",
        )
        .unwrap();
        let v = &load(&p).unwrap()[0];
        assert_eq!(v.kind, Kind::Driver);
        assert_eq!(v.runs, Some(3));
        assert_eq!(v.driver.as_ref().unwrap(), &dir.path().join("driver.py"));
        assert!(v.image.is_none());

        std::fs::write(&p, "[[variant]]\nid='x'\npin='p'\nkind='driver'\n").unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("needs `driver`"), "{err}");
    }
}
