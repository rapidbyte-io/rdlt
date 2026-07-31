//! The shared CDC rig and config scaffolding consumed by the test_cdc_*
//! case files — slot lifecycle, the streaming cycle, replica identity/TOAST,
//! and damage recovery.

use rdlt_connector_postgres::fixtures::CdcPgFixture;
use rdlt_connector_postgres::source::config::{AckMode, CdcConfig, CdcMode, Wait};

pub fn cdc(slot: &str, publication: &str, create_if_missing: bool) -> CdcConfig {
    CdcConfig {
        slot: slot.into(),
        publication: publication.into(),
        create_if_missing,
        mode: CdcMode::Catchup,
        idle_wait: Wait { seconds: 1 },
        flag_column: "_rdlt_deleted".into(),
        ack: AckMode::Auto,
    }
}

pub const SEED: &str = r#"
CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);
INSERT INTO public.orders VALUES (1, 10), (2, 20);
"#;

// ───────────────────── bounded catch-up ─────────────────────

use rdlt_connector_postgres::dest::{DestinationOptions, MergeStrategy, Postgres, TableOptions};
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_engine::{Engine, EngineConfig};

/// The recommended composition (contract C3): CDC source → postgres dest,
/// `merge{key}` + `merge_strategy: upsert` + `hard_delete: <flag>` into a
/// `mirror` schema of the SAME database (equality checks become SQL).
pub struct CdcRig {
    pub fixture: CdcPgFixture,
    pub workdir: std::path::PathBuf,
    pipeline: String,
}

impl CdcRig {
    /// Skip-not-fail: `None` when no container runtime, so callers return early.
    pub async fn start(pipeline: &str) -> Option<Self> {
        let fixture = CdcPgFixture::start().await?;
        let dir = tempfile::tempdir().expect("workdir");
        let workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        Some(Self {
            fixture,
            workdir,
            pipeline: pipeline.to_string(),
        })
    }

    /// A fresh pipeline identity (new workdir + name): the documented
    /// recovery for wedged streams — cursors gone, next run snapshots.
    pub fn reset_state(&mut self, pipeline: &str) {
        let dir = tempfile::tempdir().expect("workdir");
        self.workdir = dir.path().to_path_buf();
        std::mem::forget(dir);
        self.pipeline = pipeline.to_string();
    }

    fn source(&self, tables: &[&str]) -> PostgresSource {
        let list = tables
            .iter()
            .map(|t| format!("  - name: {t}\n"))
            .collect::<String>();
        PostgresSource::from_yaml(&format!(
            "conn: \"{}\"\ncdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\ntables:\n{list}",
            self.fixture.conn
        ))
        .expect("cdc source config")
    }

    pub fn dest(&self, tables: &[&str]) -> Postgres {
        Postgres::connect(self.fixture.conn.clone())
            .dataset("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: tables
                    .iter()
                    .map(|t| {
                        (
                            t.to_string(),
                            TableOptions {
                                hard_delete: Some("_rdlt_deleted".into()),
                                ..TableOptions::default()
                            },
                        )
                    })
                    .collect(),
            })
            .expect("valid dest options")
    }

    pub async fn run(&self, tables: &[&str], key: &str) -> u64 {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config = config.with_workdir(self.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        });
        let report = Engine::new(config, self.source(tables), self.dest(tables))
            .run()
            .await
            .expect("cdc run");
        report.total_rows()
    }

    pub async fn run_expect_err(&self, tables: &[&str], key: &str) -> String {
        let mut config = EngineConfig::new(self.pipeline.as_str());
        config = config.with_workdir(self.workdir.clone());
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec![key.into()],
        });
        Engine::new(config, self.source(tables), self.dest(tables))
            .run()
            .await
            .expect_err("run should fail")
            .to_string()
    }

    /// Row-for-row equality on the projected columns, both directions.
    pub async fn assert_mirror_equals(&self, table: &str, cols: &str) {
        let client = self.fixture.client().await;
        for (a, b) in [("public", "mirror"), ("mirror", "public")] {
            let diff: i64 = client
                .query_one(
                    &format!(
                        "SELECT count(*) FROM (SELECT {cols} FROM {a}.\"{table}\" \
                         EXCEPT SELECT {cols} FROM {b}.\"{table}\") d"
                    ),
                    &[],
                )
                .await
                .expect("equality query")
                .get(0);
            assert_eq!(diff, 0, "{a} \\ {b} on {table} should be empty");
        }
    }

    pub async fn scalar(&self, sql: &str) -> i64 {
        self.fixture
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar")
            .get(0)
    }

    pub async fn scalar_text(&self, sql: &str) -> String {
        self.fixture
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar text")
            .get(0)
    }
}
