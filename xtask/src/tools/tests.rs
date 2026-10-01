use std::fs;
use std::path::Path;

use super::{PLATFORMS, UNBUILT, Unlocked, check};

const SHA256: &str = "sha256:8fe196b894ccf9072f98d4e1013a180306e17d244830b03986ee5e8eabeb6156";
const BLAKE3: &str = "blake3:4871fab0e60275a1eb46e7190726e144f56c9a9527f59b0d1da5a042baead8e2";

const FUZZ: &str = "github:rust-fuzz/cargo-fuzz";

const CONFIG: &str = "[tools]\njust = \"1.58.0\"\n\"github:rust-fuzz/cargo-fuzz\" = { version = \"0.13.2\", os = [\"linux\"] }\n";

fn platform(tool: &str, platform: &str, checksum: Option<&str>, url: Option<&str>) -> String {
    let line = |key: &str, value: Option<&str>| {
        value.map_or_else(String::new, |value| format!("{key} = \"{value}\"\n"))
    };
    format!(
        "[tools.\"{tool}\".\"platforms.{platform}\"]\n{}{}",
        line("checksum", checksum),
        line("url", url)
    )
}

fn entry(tool: &str, version: &str) -> String {
    format!("[[tools.\"{tool}\"]]\nversion = \"{version}\"\nbackend = \"aqua:x/y\"\n\n")
}

/// A lockfile holding both tools of [`CONFIG`], with `just`'s Linux download as given.
fn lock(just_linux: &str) -> String {
    let url = Some("https://example.com/asset.tar.gz");
    [
        entry("just", "1.58.0"),
        just_linux.to_owned(),
        platform("just", "macos-arm64", Some(BLAKE3), url),
        entry(FUZZ, "0.13.2"),
        platform(FUZZ, "linux-x64", Some(SHA256), url),
    ]
    .join("\n")
}

fn locked() -> String {
    let url = Some("https://example.com/asset.tar.gz");
    lock(&platform("just", "linux-x64", Some(SHA256), url))
}

fn linux(tool: &str) -> (String, String) {
    (tool.to_owned(), "linux-x64".to_owned())
}

#[test]
fn tools_locked_to_a_digest_on_every_platform_pass() {
    assert_eq!(check(CONFIG, &locked()).unwrap(), Vec::new());
}

#[test]
fn a_download_without_a_digest_is_reported() {
    let url = Some("https://example.com/asset.tar.gz");
    let short = &SHA256[..SHA256.len() - 1];
    let long = format!("{SHA256}0");
    let upper = SHA256.replace("8fe", "8FE");
    let checksums = [
        None,
        Some(""),
        Some("sha256:"),
        Some("8fe196b894ccf9072f98d4e1013a180306e17d244830b03986ee5e8eabeb6156"),
        Some("md5:8fe196b894ccf9072f98d4e1013a180306e17d244830b03986ee5e8eabeb6156"),
        Some("sha256:zze196b894ccf9072f98d4e1013a180306e17d244830b03986ee5e8eabeb6156"),
        Some(short),
        Some(long.as_str()),
        Some(upper.as_str()),
    ];
    for checksum in checksums {
        let lock = lock(&platform("just", "linux-x64", checksum, url));
        let (tool, platform) = linux("just");
        let expected = vec![Unlocked::Checksum { tool, platform }];
        assert_eq!(check(CONFIG, &lock).unwrap(), expected, "{checksum:?}");
    }
}

#[test]
fn a_platform_without_a_download_is_reported() {
    let cases = [
        String::new(),
        platform("just", "linux-x64", Some(SHA256), None),
        platform(
            "just",
            "linux-x64",
            Some(SHA256),
            Some("http://example.com/a"),
        ),
        platform(
            "just",
            "linux-arm64",
            Some(SHA256),
            Some("https://example.com/a"),
        ),
    ];
    for case in cases {
        let (tool, platform) = linux("just");
        let expected = vec![Unlocked::Platform { tool, platform }];
        assert_eq!(check(CONFIG, &lock(&case)).unwrap(), expected, "{case:?}");
    }
}

// A tool installed through cargo has no download to lock: its entry names no platform.
#[test]
fn a_tool_whose_backend_locks_no_download_is_reported_for_every_platform() {
    let config = "[tools]\n\"cargo:cargo-hack\" = \"0.6.45\"\n";
    let lock =
        "[[tools.\"cargo:cargo-hack\"]]\nversion = \"0.6.45\"\nbackend = \"cargo:cargo-hack\"\n";
    let expected: Vec<Unlocked> = PLATFORMS
        .iter()
        .map(|platform| Unlocked::Platform {
            tool: "cargo:cargo-hack".to_owned(),
            platform: (*platform).to_owned(),
        })
        .collect();
    assert_eq!(check(config, lock).unwrap(), expected);
}

#[test]
fn a_tool_missing_from_the_lockfile_or_locked_at_another_version_is_reported() {
    let config = format!("{CONFIG}typos = \"1.50.2\"\n");
    let tool = "typos".to_owned();
    assert_eq!(
        check(&config, &locked()).unwrap(),
        vec![Unlocked::Missing { tool }]
    );
    let config = CONFIG.replace("1.58.0", "1.59.0");
    let tool = "just".to_owned();
    assert_eq!(
        check(&config, &locked()).unwrap(),
        vec![Unlocked::Version { tool }]
    );
    let config = CONFIG.replace("0.13.2", "0.13.3");
    let tool = FUZZ.to_owned();
    assert_eq!(
        check(&config, &locked()).unwrap(),
        vec![Unlocked::Version { tool }]
    );
}

// Only the platforms a tool's authors do not build for may go without a download.
#[test]
fn a_platform_is_excused_only_for_the_tools_listed() {
    assert!(UNBUILT.contains(&(FUZZ, "macos-arm64")));
    assert!(!UNBUILT.iter().any(|(tool, _)| *tool == "just"));
    let without_mac = locked().replace(
        &platform(
            "just",
            "macos-arm64",
            Some(BLAKE3),
            Some("https://example.com/asset.tar.gz"),
        ),
        "",
    );
    let expected = vec![Unlocked::Platform {
        tool: "just".to_owned(),
        platform: "macos-arm64".to_owned(),
    }];
    assert_eq!(check(CONFIG, &without_mac).unwrap(), expected);
}

#[test]
fn a_file_that_is_not_what_mise_writes_is_an_error() {
    assert!(check("[tools", &locked()).is_err());
    assert!(check(CONFIG, "[[tools.just]").is_err());
    assert!(check("[tools]\njust = 1\n", &locked()).is_err());
    assert!(check("[tools]\njust = { os = [\"linux\"] }\n", &locked()).is_err());
    assert!(check(CONFIG, "tools = 1\n").is_err());
}

#[test]
fn a_configuration_or_lockfile_without_tools_has_none_unlocked() {
    assert_eq!(check("", "").unwrap(), Vec::new());
    let expected = vec![
        Unlocked::Missing {
            tool: FUZZ.to_owned(),
        },
        Unlocked::Missing {
            tool: "just".to_owned(),
        },
    ];
    assert_eq!(check(CONFIG, "").unwrap(), expected);
}

#[test]
fn this_repository_locks_every_tool() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let config = fs::read_to_string(root.join("mise.toml")).unwrap();
    let lock = fs::read_to_string(root.join("mise.lock")).unwrap();
    assert_eq!(check(&config, &lock).unwrap(), Vec::new());
    assert!(config.contains("[tools]"));
}
