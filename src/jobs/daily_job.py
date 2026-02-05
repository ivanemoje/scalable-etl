import os
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_daily_sync():
    # 1. Resolve Project Root and Load Env
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    load_dotenv(dotenv_path=BASE_DIR / ".env")

    # Local DB Path for Host (Mac)
    DB_PATH = str(BASE_DIR / "data" / "outputs" / "scalable.db")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    minio_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret = os.getenv("MINIO_ROOT_PASSWORD", "password")
    
    logger.info(f"Connecting to DuckDB: {DB_PATH}")
    db = duckdb.connect(DB_PATH)

    try:
        # 2. Configure DuckDB S3 Connection
        db.execute("INSTALL httpfs; LOAD httpfs;")
        db.execute("SET s3_endpoint='localhost:9100';") # Mapping to MinIO Host Port
        db.execute(f"SET s3_access_key_id='{minio_key}';")
        db.execute(f"SET s3_secret_access_key='{minio_secret}';")
        db.execute("SET s3_url_style='path';")
        db.execute("SET s3_use_ssl=false;")
        db.execute("SET s3_region='us-east-1';")

        # 3. Define S3 Glob Patterns
        # Spark writes demo.silver_listens -> s3://warehouse/silver_listens/
        # Spark writes demo.gold.user_peaks -> s3://warehouse/gold/user_peaks/
        silver_s3 = "s3://warehouse/silver_listens/data/**/*.parquet"
        gold_s3 = "s3://warehouse/gold/user_peaks/data/**/*.parquet"

        # 4. Sync Layers
        logger.info("Linking Silver Layer (View)...")
        db.execute(f"CREATE OR REPLACE VIEW silver_listens AS SELECT * FROM read_parquet('{silver_s3}');")

        logger.info("Syncing Gold Layer (Local Table)...")
        db.execute(f"CREATE OR REPLACE TABLE gold_peaks AS SELECT * FROM read_parquet('{gold_s3}');")

        # 5. Queries
        print("\n" + "=" * 60)
        print("DAILY REPORTING QUERIES")
        print("=" * 60)

        print("\nA1. Top 10 users by songs listened to:")
        db.query("SELECT user_name, COUNT(*) AS listen_count FROM silver_listens GROUP BY 1 ORDER BY 2 DESC LIMIT 10").show()

        print("\nA2. Rolling 7-Day Active Users (DAU):")
        db.query("""
            SELECT 
                listened_date,
                COUNT(DISTINCT user_name) OVER (
                    ORDER BY listened_date 
                    RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
                ) as active_users_7d
            FROM silver_listens
            ORDER BY listened_date DESC
            LIMIT 10
        """).show()

        logger.info("Daily job finished.")

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        # If no files found, let's see what is actually in MinIO
        print("\n[DEBUG] Running Bucket Inspection:")
        os.system("docker exec mc-scalable mc ls -r minio/warehouse")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    run_daily_sync()