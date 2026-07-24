# Pinned Python dlt baseline (measure FIRST, same datasets/hardware). The e2e
# matrix (feature 018) drops the duckdb extras and adds s3fs so dlt's
# filesystem source/destination can reach the RUSTFS object store; connectorx
# stays for the fastest pg extraction path.
FROM python:3.12-slim
RUN pip install --no-cache-dir "setuptools<81" \
    "dlt[filesystem,postgres,sql_database]==1.29.0" \
    pyarrow connectorx pandas s3fs
WORKDIR /bench
COPY pipeline_pg_pg.py pipeline_pg_s3parquet.py pipeline_s3jsonl_pg.py \
     pipeline_s3jsonl_s3parquet.py pipeline_pg_pg_dedup.py ./
ENTRYPOINT ["python"]
