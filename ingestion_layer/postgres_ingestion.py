import pandas as pd
from io import BytesIO
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import ClientError
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from scripts.utils import get_logger, get_db_engine, get_destination_s3_client

# ----------------------------
# CONFIG
# ----------------------------
BUCKET = "supplychain360-data-lake"
TARGET_PREFIX = "raw/store_sales_transactions/"
CHUNK_SIZE = 10000

logger = get_logger(__name__)
s3 = get_destination_s3_client()


# ----------------------------
# ENGINE — created once, reused across all tables
# ----------------------------
def get_engine_with_keepalive():
    """
    Creates an engine with TCP keepalive and a generous connect/statement
    timeout so the SSL connection isn't silently dropped mid-read.
    """
    engine = get_db_engine(
        connect_args={
            "connect_timeout": 30,
            "keepalives": 1,
            "keepalives_idle": 30,       # send first keepalive after 30s idle
            "keepalives_interval": 10,   # retry every 10s
            "keepalives_count": 5,       # drop after 5 unanswered probes
            "options": "-c statement_timeout=0",  # no server-side statement limit
        }
    )
    return engine


# ----------------------------
# IDEMPOTENCY CHECK
# ----------------------------
def already_loaded(table_name):
    key = f"{TARGET_PREFIX}{table_name}.parquet"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# ----------------------------
# S3 UPLOAD
# ----------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
def upload_to_s3(buffer, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


# ----------------------------
# EXTRACT + LOAD
# Reads all chunks into memory first, writes ONE parquet — no mid-loop S3 I/O.
# This is the critical change: the DB connection stays active and uninterrupted
# for the full read, and we never trigger the ~64s idle timeout between chunks.
# ----------------------------
@retry(
    retry=retry_if_exception_type(OperationalError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2),
    reraise=True,
)
def extract_table_to_s3(table_name, engine):
    logger.info(f"Extracting {table_name}...")

    target_key = f"{TARGET_PREFIX}{table_name}.parquet"

    if already_loaded(table_name):
        logger.info(f"Skipping {table_name} (already loaded)")
        return

    # Stream chunks from DB and accumulate — no S3 round-trips during the read
    chunks = []
    try:
        for chunk in pd.read_sql(
            f'SELECT * FROM public."{table_name}"',
            engine,
            chunksize=CHUNK_SIZE,
        ):
            chunks.append(chunk)
    except Exception as e:
        logger.error(f"Failed reading table {table_name}: {e}")
        raise

    if not chunks:
        logger.warning(f"No rows found in {table_name}, skipping upload")
        return

    # Combine all chunks and write a single Parquet — O(1) S3 writes
    df = pd.concat(chunks, ignore_index=True)
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    upload_to_s3(buffer, target_key)
    logger.info(f"Saved {table_name} → s3://{BUCKET}/{target_key} ({len(df):,} rows)")


# ----------------------------
# MAIN PIPELINE
# ----------------------------
def postgres_ingestion_pipeline():
    logger.info("Starting Postgres → S3 pipeline...")

    tables = [
        "sales_2026_03_10",
        "sales_2026_03_11",
        "sales_2026_03_12",
        "sales_2026_03_13",
        "sales_2026_03_14",
        "sales_2026_03_15",
        "sales_2026_03_16",
    ]

    # Single engine, reused across all tables — avoids repeated auth + handshake
    engine = get_engine_with_keepalive()

    try:
        for table in tables:
            try:
                extract_table_to_s3(table, engine)
            except Exception as e:
                logger.error(f"Failed processing {table}: {e}")
    finally:
        engine.dispose()
        logger.info("DB engine disposed.")

    logger.info("Pipeline completed.")


if __name__ == "__main__":
    postgres_ingestion_pipeline()