# Pinned Python dlt baseline (measure FIRST, same datasets/hardware).
FROM python:3.12-slim
RUN pip install --no-cache-dir "setuptools<81" "dlt[duckdb,filesystem,postgres]==1.29.0" pyarrow
WORKDIR /bench
COPY pipeline_jsonl_duckdb.py pipeline_parquet.py pipeline_rest_pg.py normalize_only.py cold_start.py ./
ENTRYPOINT ["python"]
