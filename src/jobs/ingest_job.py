import duckdb
import os
import time
import logging
import tempfile
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestWatcher")

# Load paths from env
DB_PATH = os.environ.get("DB_PATH")
BRONZE_DIR = os.environ.get("BRONZE_DIR")
INPUT_DIR = os.environ.get("INPUT_DIR")

# S3/MinIO configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio-scalable:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "scalable")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "scalable123")
S3_INPUT_BUCKET = os.environ.get("S3_INPUT_BUCKET", "input-data")
S3_REGION = os.environ.get("AWS_REGION", "eu-central-1")
S3_POLL_INTERVAL = int(os.environ.get("S3_POLL_INTERVAL", "30"))  # seconds

def get_s3_client():
    """Initialize S3/MinIO client"""
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=boto3.session.Config(signature_version='s3v4')
    )

def ensure_s3_bucket_exists(s3_client):
    """Create S3 input bucket if it doesn't exist"""
    try:
        s3_client.head_bucket(Bucket=S3_INPUT_BUCKET)
        logger.info(f"✓ S3 bucket '{S3_INPUT_BUCKET}' exists")
    except ClientError:
        try:
            s3_client.create_bucket(Bucket=S3_INPUT_BUCKET)
            logger.info(f"✓ Created S3 bucket '{S3_INPUT_BUCKET}'")
        except Exception as e:
            logger.warning(f"Could not create S3 bucket: {e}")

def process_file(file_path, source="local"):
    """Process a single file - works for both local and S3 files"""
    if not file_path.lower().endswith(('.txt', '.json')):
        return
    
    filename = os.path.basename(file_path)
    
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename VARCHAR PRIMARY KEY, source VARCHAR, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        
        # Check if already processed
        if con.execute("SELECT 1 FROM processed_files WHERE filename = ?", [filename]).fetchone():
            logger.debug(f"Already processed: {filename}")
            return
        
        try:
            logger.info(f"[{source.upper()}] Ingesting: {filename}")
            
            # DuckDB reads the JSON and writes partitioned Parquet
            # Convert UUID fields to VARCHAR for Spark compatibility
            # Added ignore_errors=true and increased maximum_depth to handle malformed data
            con.execute(f"""
                COPY (SELECT 
                    listened_at,
                    CAST(recording_msid AS VARCHAR) as recording_msid,
                    user_name,
                    track_metadata->>'track_name' as track_name,
                    track_metadata->>'artist_name' as artist_name,
                    track_metadata->>'release_name' as release_name,
                    CAST(track_metadata->'additional_info'->>'recording_msid' AS VARCHAR) as track_recording_msid,
                    CAST(track_metadata->'additional_info'->>'release_msid' AS VARCHAR) as track_release_msid,
                    CAST(track_metadata->'additional_info'->>'artist_msid' AS VARCHAR) as track_artist_msid,
                    to_timestamp(listened_at)::DATE as listened_date
                FROM read_json('{file_path}', format='newline_delimited', ignore_errors=true, maximum_depth=10))
                TO '{BRONZE_DIR}' (FORMAT 'PARQUET', PARTITION_BY (user_name), OVERWRITE_OR_IGNORE 1)
            """)
            
            con.execute("INSERT INTO processed_files (filename, source) VALUES (?, ?)", [filename, source])
            logger.info(f"✓ [{source.upper()}] Successfully ingested: {filename}")
            
        except Exception as e:
            logger.error(f"✗ [{source.upper()}] Failed to process {filename}: {e}")
            raise

def download_and_process_s3_file(s3_client, key):
    """Download file from S3 and process it"""
    filename = os.path.basename(key)
    
    # Download to temporary file
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        try:
            logger.info(f"[S3] Downloading: {key}")
            s3_client.download_fileobj(S3_INPUT_BUCKET, key, tmp_file)
            
            # Process the downloaded file
            process_file(tmp_path, source="s3")
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

def poll_s3_bucket(s3_client, con):
    """Poll S3 bucket for new files"""
    try:
        response = s3_client.list_objects_v2(Bucket=S3_INPUT_BUCKET)
        
        if 'Contents' not in response:
            logger.debug(f"[S3] No files in bucket '{S3_INPUT_BUCKET}'")
            return
        
        for obj in response['Contents']:
            key = obj['Key']
            filename = os.path.basename(key)
            
            # Skip if already processed
            if con.execute("SELECT 1 FROM processed_files WHERE filename = ?", [filename]).fetchone():
                continue
            
            # Download and process
            download_and_process_s3_file(s3_client, key)
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.warning(f"[S3] Bucket '{S3_INPUT_BUCKET}' does not exist")
        else:
            logger.error(f"[S3] Error polling bucket: {e}")
    except Exception as e:
        logger.error(f"[S3] Unexpected error: {e}")

def s3_polling_loop(s3_client):
    """Continuous S3 polling in background"""
    logger.info(f"[S3] Starting polling loop (interval: {S3_POLL_INTERVAL}s)")
    
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename VARCHAR PRIMARY KEY, source VARCHAR, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    while True:
        try:
            with duckdb.connect(DB_PATH) as con:
                poll_s3_bucket(s3_client, con)
        except Exception as e:
            logger.error(f"[S3] Polling error: {e}")
        
        time.sleep(S3_POLL_INTERVAL)

def run_ingest():
    """Main ingestion orchestrator - watches both local and S3"""
    
    # Create necessary directories
    for p in [os.path.dirname(DB_PATH), BRONZE_DIR, INPUT_DIR]:
        os.makedirs(p, exist_ok=True)
    
    # Initialize database
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename VARCHAR PRIMARY KEY, source VARCHAR, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    logger.info("=" * 70)
    logger.info("HYBRID INGESTION WATCHER STARTED")
    logger.info("=" * 70)
    logger.info(f"Local Input Dir: {INPUT_DIR}")
    logger.info(f"S3 Endpoint: {S3_ENDPOINT}")
    logger.info(f"S3 Bucket: {S3_INPUT_BUCKET}")
    logger.info(f"Bronze Output: {BRONZE_DIR}")
    logger.info("=" * 70)
    
    # Initialize S3 client
    try:
        s3_client = get_s3_client()
        ensure_s3_bucket_exists(s3_client)
        s3_enabled = True
        logger.info("✓ S3/MinIO connection established")
    except Exception as e:
        logger.warning(f"⚠ S3/MinIO not available: {e}")
        logger.warning("⚠ Will only watch local filesystem")
        s3_enabled = False
    
    # Process existing local files
    logger.info("[LOCAL] Scanning for existing files...")
    for f in os.listdir(INPUT_DIR):
        process_file(os.path.join(INPUT_DIR, f), source="local")
    
    # Process existing S3 files
    if s3_enabled:
        logger.info("[S3] Scanning for existing files...")
        with duckdb.connect(DB_PATH) as con:
            poll_s3_bucket(s3_client, con)
    
    # Set up local filesystem watcher
    logger.info("[LOCAL] Starting filesystem watcher...")
    observer = Observer()
    handler = FileSystemEventHandler()
    handler.on_created = lambda event: process_file(event.src_path, source="local")
    observer.schedule(handler, INPUT_DIR, recursive=False)
    observer.start()
    
    # Start S3 polling loop
    if s3_enabled:
        logger.info("[S3] Starting polling loop...")
        import threading
        s3_thread = threading.Thread(target=s3_polling_loop, args=(s3_client,), daemon=True)
        s3_thread.start()
    
    logger.info("✓ All watchers active. Waiting for files...")
    logger.info("")
    logger.info("To add files:")
    logger.info(f"  Local:  cp your_file.txt {INPUT_DIR}/")
    if s3_enabled:
        logger.info(f"  S3:     aws s3 cp your_file.txt s3://{S3_INPUT_BUCKET}/ --endpoint-url {S3_ENDPOINT}")
    logger.info("")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    run_ingest()