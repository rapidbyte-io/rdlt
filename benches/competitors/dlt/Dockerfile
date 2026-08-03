# Pinned Python dlt baseline (measure FIRST, same datasets/hardware). The e2e
# matrix (feature 018) drops the duckdb extras and adds s3fs so dlt's
# filesystem source/destination can reach the RUSTFS object store; connectorx
# stays for the fastest pg extraction path.
#
# `oracledb` (032) is the Oracle DBAPI SQLAlchemy's oracle+oracledb dialect
# drives, and it runs THIN by default — no Instant Client, no Oracle client
# libraries, nothing but the wheel. It is a separate install because dlt has no
# `oracle` extra. connectorx is deliberately NOT usable for Oracle: its Oracle
# source is ODPI-C and needs Instant Client, which is not pip-installable.
# Adding this wheel changes the image the five 018 cells' dlt arms also run in
# (nothing existing imports it, but the image identity moves), so the version
# policy's conservative call applies: re-run the WHOLE matrix in one session
# rather than splicing the new row into an older recording. dlt's own pin is
# unchanged, so variants.toml's `pin = "dlt 1.29.0"` stays true.
FROM python:3.12-slim
RUN pip install --no-cache-dir "setuptools<81" \
    "dlt[filesystem,postgres,sql_database]==1.29.0" \
    pyarrow connectorx pandas s3fs \
    oracledb==4.0.2
WORKDIR /bench
COPY pipeline_pg_pg.py pipeline_pg_s3parquet.py pipeline_s3jsonl_pg.py \
     pipeline_s3jsonl_s3parquet.py pipeline_pg_pg_dedup.py \
     pipeline_oracle_pg.py ./
ENTRYPOINT ["python"]
