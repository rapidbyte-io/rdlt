# Pinned Python dlt baseline (measure FIRST, same datasets/hardware).
FROM python:3.12-slim
RUN pip install --no-cache-dir "setuptools<81" "dlt[duckdb,filesystem]==1.11.0" pyarrow
WORKDIR /bench
COPY pipeline_jsonl_duckdb.py pipeline_parquet.py ./
ENTRYPOINT ["python"]
