# Pinned Python dlt baseline (measure FIRST, same datasets/hardware).
FROM python:3.12-slim
RUN pip install --no-cache-dir "setuptools<81" "dlt[duckdb]==1.11.0"
WORKDIR /bench
COPY pipeline_jsonl_duckdb.py .
ENTRYPOINT ["python", "pipeline_jsonl_duckdb.py"]
