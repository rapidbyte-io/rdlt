//! Data-file writing for one table's commit window: a plain writer for
//! unpartitioned tables, the library's fanout writer (a splitter computes
//! partition values from source columns) for partitioned ones. Closing
//! yields the staged Iceberg data files the commit loop appends.

use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::DataFileFormat;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder as IcebergDataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use rdlt_connector::DestinationError;
use rdlt_connector::core::crash_point;

use super::errors::classify;

/// The fully-parameterized data-file writer builder this crate stages files
/// through. The library type only appears here, fully qualified, so the
/// short alias names the concrete stack everywhere else.
type DataFileWriterBuilder = IcebergDataFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// One table's writer for the current commit window: plain for
/// unpartitioned tables, library fanout (splitter computes the
/// partition values from source columns) for partitioned ones.
pub(crate) enum TableWriter {
    Plain(Box<dyn IcebergWriter>),
    Fanout(Box<FanoutParts>),
}

pub(crate) struct FanoutParts {
    writer: FanoutWriter<DataFileWriterBuilder>,
    splitter: RecordBatchPartitionSplitter,
}

impl TableWriter {
    /// Open a writer against the CURRENT table metadata. `session_nonce`
    /// makes file names unique ACROSS sessions: a recovery session
    /// re-staging the same (load, window) must never overwrite a file a
    /// prior session already committed (its own files stay orphaned and
    /// invisible when the replay check discards them).
    pub(crate) async fn open(
        table: &iceberg::table::Table,
        file_prefix: &str,
        session_nonce: &str,
    ) -> Result<Self, DestinationError> {
        let context = || format!("table `{}`", table.identifier());
        let location =
            DefaultLocationGenerator::new(table.metadata()).map_err(|e| classify(&context(), e))?;
        let names = DefaultFileNameGenerator::new(
            file_prefix.to_owned(),
            Some(session_nonce.to_owned()),
            DataFileFormat::Parquet,
        );
        let parquet = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            location,
            names,
        );
        crash_point!(
            "ice.files.write",
            Err(DestinationError::fatal("injected crash at ice.files.write"))
        );
        let builder = DataFileWriterBuilder::new(rolling);
        let spec = table.metadata().default_partition_spec().clone();
        if spec.fields().is_empty() {
            let inner = builder
                .build(None)
                .await
                .map_err(|e| classify(&context(), e))?;
            Ok(Self::Plain(Box::new(inner)))
        } else {
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                table.metadata().current_schema().clone(),
                spec,
            )
            .map_err(|e| classify(&context(), e))?;
            Ok(Self::Fanout(Box::new(FanoutParts {
                writer: FanoutWriter::new(builder),
                splitter,
            })))
        }
    }

    pub(crate) async fn write(
        &mut self,
        context: &str,
        batch: arrow_array::RecordBatch,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Plain(inner) => inner.write(batch).await.map_err(|e| classify(context, e)),
            Self::Fanout(fanout) => {
                let parts = fanout
                    .splitter
                    .split(&batch)
                    .map_err(|e| classify(context, e))?;
                for (key, part) in parts {
                    fanout
                        .writer
                        .write(key, part)
                        .await
                        .map_err(|e| classify(context, e))?;
                }
                Ok(())
            }
        }
    }

    pub(crate) async fn close(
        self,
        context: &str,
    ) -> Result<Vec<iceberg::spec::DataFile>, DestinationError> {
        match self {
            Self::Plain(mut inner) => inner.close().await.map_err(|e| classify(context, e)),
            Self::Fanout(fanout) => fanout
                .writer
                .close()
                .await
                .map_err(|e| classify(context, e)),
        }
    }
}
