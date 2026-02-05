import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_daily_sync():
    db_path = "data/outputs/scalable.db"
    
    # Grab from exported env
    minio_key = os.getenv("MINIO_ROOT_USER")
    minio_secret = os.getenv("MINIO_ROOT_PASSWORD")
    aws_region = os.getenv("AWS_REGION", "eu-central-1")
    s3_endpoint = "127.0.0.1:9100" 
    s3_bucket = "warehouse"

    logger.info(f"Connecting to DuckDB at {db_path}...")
    db = duckdb.connect(db_path)

    try:
        db.execute("INSTALL httpfs; LOAD httpfs;")
        db.execute("INSTALL iceberg; LOAD iceberg;")
        
        # Force global S3 settings to bypass SECRET lookup issues
        db.execute(f"SET s3_endpoint='{s3_endpoint}';")
        db.execute(f"SET s3_access_key_id='{minio_key}';")
        db.execute(f"SET s3_secret_access_key='{minio_secret}';")
        db.execute(f"SET s3_region='{aws_region}';")
        db.execute("SET s3_url_style='path';")
        db.execute("SET s3_use_ssl=false;")
        
        # Keep this for the Iceberg metadata crawler
        db.execute("SET unsafe_enable_version_guessing = true;")

        logger.info("Syncing Gold Layer from Iceberg...")
        iceberg_path = f"s3://{s3_bucket}/gold/user_peaks"
        
        # Explicitly scanning the Iceberg table
        db.execute(f"CREATE OR REPLACE TABLE gold_peaks AS SELECT * FROM iceberg_scan('{iceberg_path}')")
        
        count = db.execute("SELECT COUNT(*) FROM gold_peaks").fetchone()[0]
        logger.info(f"SUCCESS: {count} records synced to {db_path}")

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    run_daily_sync()