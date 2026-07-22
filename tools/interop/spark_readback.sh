#!/usr/bin/env bash
# Spark read-back oracle (feature 016 T014, contract ID8 — DEEP tier).
#
# Reads an rdlt-written table through the SAME Iceberg REST catalog with
# Spark (the reference JVM implementation) and prints:
#     COUNT=<rows>
#     COLUMNS=<name:type,name:type,…>
#
# Runs apache/spark in a podman container on the host network (the
# catalog and object store listen on 127.0.0.1); Iceberg runtime +
# AWS bundle resolve from Maven Central into a named-volume ivy cache
# (rdlt-spark-ivy) so repeat runs skip the download.
set -euo pipefail

URI="$1" WAREHOUSE="$2" CREDENTIAL="$3" NAMESPACE="$4" TABLE="$5"
S3_ENDPOINT="$6" S3_KEY="$7" S3_SECRET="$8"

SPARK_IMAGE="${RDLT_SPARK_IMAGE:-docker.io/apache/spark:3.5.3}"
ICEBERG_VERSION="${RDLT_ICEBERG_SPARK_VERSION:-1.7.1}"

# spark-sql prints DESCRIBE rows tab-separated (name<TAB>type<TAB>comment);
# the Rust cell parses those lines directly.
SQL="SELECT CONCAT('COUNT=', COUNT(*)) FROM rdlt.\`${NAMESPACE}\`.\`${TABLE}\`;
DESCRIBE TABLE rdlt.\`${NAMESPACE}\`.\`${TABLE}\`;"

exec podman run --rm --network host \
  -v rdlt-spark-ivy:/tmp/ivy \
  -e HOME=/tmp \
  "${SPARK_IMAGE}" \
  /opt/spark/bin/spark-sql \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}" \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.ui.enabled=false \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.rdlt=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.rdlt.type=rest" \
  --conf "spark.sql.catalog.rdlt.uri=${URI}" \
  --conf "spark.sql.catalog.rdlt.warehouse=${WAREHOUSE}" \
  --conf "spark.sql.catalog.rdlt.credential=${CREDENTIAL}" \
  --conf "spark.sql.catalog.rdlt.scope=PRINCIPAL_ROLE:ALL" \
  --conf "spark.sql.catalog.rdlt.header.X-Iceberg-Access-Delegation=" \
  --conf "spark.sql.catalog.rdlt.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.rdlt.s3.endpoint=${S3_ENDPOINT}" \
  --conf "spark.sql.catalog.rdlt.s3.path-style-access=true" \
  --conf "spark.sql.catalog.rdlt.s3.access-key-id=${S3_KEY}" \
  --conf "spark.sql.catalog.rdlt.s3.secret-access-key=${S3_SECRET}" \
  --conf "spark.sql.catalog.rdlt.client.region=us-east-1" \
  -e "${SQL}"
